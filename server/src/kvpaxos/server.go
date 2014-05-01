package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Key string
  Value string
  Type string
  DoHash bool
  ClientID int64
  RequestTime time.Time
}

type RequestValue struct {
  Value string
  ClientID int64
  RequestTime time.Time
  SeqNum int
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  keyvalues map[string]RequestValue
  putRequests map[int64]DuplicatePut
  getRequests map[int64]DuplicateGet
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  duplicate, ok := kv.getRequests[args.ClientID]
  if (ok && duplicate.RequestTime.Before(args.RequestTime)) || !ok {
    getOp := Op{args.Key, "", GET, false, args.ClientID, args.RequestTime}
    var seqNum int

    seqNum = kv.px.Max() + 1
    kv.px.Start(seqNum, getOp)

    // Wait for agreement
    to := time.Millisecond
    for {
      decided, val := kv.px.Status(seqNum)
      if decided {
        if val.(Op) == getOp {
          // Check if agreed on instance is this instance
          break
        } else {
          // No agreement, have client try again
          reply.Err = ErrNoAgreement
          kv.mu.Unlock()
          return nil
        }
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }

    // Update first/do catchup
    curSeq := kv.px.Min()
    for curSeq < seqNum {
      decided, val := kv.px.Status(curSeq)
      if decided {
        if val.(Op).Type == PUT { // Only care about PUT ops
          dbVal, dbOk := kv.keyvalues[val.(Op).Key]

          newval := ""
          previousVal := ""
          // Calculate hash as we go
          if val.(Op).DoHash {
            curVal, ok := kv.keyvalues[val.(Op).Key]
            if !ok {
              curVal = RequestValue{"", 0, time.Now(), 0}
            }
            previousVal = curVal.Value
            newval = strconv.Itoa(int(hash(curVal.Value + val.(Op).Value)))
          } else {
            newval = val.(Op).Value
          }

          if !dbOk {
            kv.keyvalues[val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq}
            kv.putRequests[val.(Op).ClientID] = DuplicatePut{val.(Op).RequestTime, previousVal}
          } else if dbVal.SeqNum < curSeq {
            if dbVal.RequestTime != val.(Op).RequestTime { // At most once guarantee
              kv.keyvalues[val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq}
              kv.putRequests[val.(Op).ClientID] = DuplicatePut{val.(Op).RequestTime, previousVal}
            }
          }
        }
        curSeq++
      } else {
        // Not decided yet or missed out or done, send in a NOP to check
        if curSeq >= kv.px.Min() {
          noOp := Op{"", "", GET, false, 0, time.Now()}
          kv.px.Start(curSeq, noOp)
          to := time.Millisecond
          for {
            decided, _ := kv.px.Status(curSeq)
            if decided {
              break
            }
            time.Sleep(to)
            if to < 10 * time.Second {
              to *= 2
            }
          }
        } else {
          curSeq++
        }
      }
    }
    // End of updates

    val, ok := kv.keyvalues[args.Key]
    if ok {
      // Value exists, return it
      reply.Err = OK
      reply.Value = val.Value
      kv.getRequests[args.ClientID] = DuplicateGet{args.RequestTime, reply.Value}
    } else {
      // Key doesn't exist
      reply.Err = ErrNoKey
    }
    kv.px.Done(seqNum)
  } else {
    reply.Err = OK
    reply.Value = duplicate.Value
  }
  kv.mu.Unlock()
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  duplicate, ok := kv.putRequests[args.ClientID]
  if (ok && duplicate.RequestTime.Before(args.RequestTime)) || !ok {
    var newval string
    putOp := Op{args.Key, args.Value, PUT, args.DoHash, args.ClientID, args.RequestTime}
    seqNum := kv.px.Max() + 1
    kv.px.Start(seqNum, putOp)

    // Wait for agreement
    to := 10 * time.Millisecond
    for {
      decided, val := kv.px.Status(seqNum)
      if decided {
        // Updates first
        curSeq := kv.px.Min()
        for curSeq < seqNum {
          decided, val := kv.px.Status(curSeq)
          if decided {
            if val.(Op).Type == PUT {
              dbVal, dbOk := kv.keyvalues[val.(Op).Key]
              previousVal := ""
              if val.(Op).DoHash {
                curVal, ok := kv.keyvalues[val.(Op).Key]
                if !ok {
                  curVal = RequestValue{"", 0, time.Now(), 0}
                }
                previousVal = curVal.Value
                newval = strconv.Itoa(int(hash(curVal.Value + val.(Op).Value)))
              } else {
                newval = val.(Op).Value
              }

              if !dbOk {
                // Encountered new key
                kv.keyvalues[val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq}
                kv.putRequests[val.(Op).ClientID] = DuplicatePut{val.(Op).RequestTime, previousVal}
              } else if dbVal.SeqNum < curSeq && dbVal.RequestTime != val.(Op).RequestTime {
                // Old key, but by a client request we haven't seen yet
                kv.keyvalues[val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq}
                kv.putRequests[val.(Op).ClientID] = DuplicatePut{val.(Op).RequestTime, previousVal}
              }
            }
            curSeq++
          } else {
            // Not decided yet or missed out or done, send in a NOP to check
            noOp := Op{"", "", GET, false, 0, time.Now()}
            kv.px.Start(curSeq, noOp)
            to := time.Millisecond
            for {
              decided, _ := kv.px.Status(curSeq)
              if decided {
                break
              }
              time.Sleep(to)
              if to < 10 * time.Second {
                to *= 2
              }
            }
          }
        }

        if val.(Op) == putOp {
          // Calculate hash again to be safe
          if args.DoHash {
            val, ok := kv.keyvalues[args.Key]
            if !ok {
              val = RequestValue{"", 0, time.Now(), 0}
            }
            reply.PreviousValue = val.Value
            newval = strconv.Itoa(int(hash(val.Value + args.Value)))
          } else {
            newval = args.Value
          }

          reply.Err = OK
          dbVal, dbOk := kv.keyvalues[args.Key]
          if !dbOk || (dbVal.SeqNum < seqNum && dbVal.RequestTime != args.RequestTime) {
            // Make sure at once
            kv.keyvalues[args.Key] = RequestValue{newval, args.ClientID, args.RequestTime, seqNum}
            kv.putRequests[args.ClientID] = DuplicatePut{args.RequestTime, reply.PreviousValue}
          } else {
            // Otherwise send back the previous previous-value
            reply.PreviousValue = kv.putRequests[args.ClientID].PreviousValue
          }

          kv.px.Done(seqNum)
          kv.mu.Unlock()
          return nil
        } else {
          reply.Err = ErrNoAgreement
          break
        }
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  } else {
    reply.Err = OK
    reply.PreviousValue = duplicate.PreviousValue
  }
  kv.mu.Unlock()
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.keyvalues = make(map[string]RequestValue)
  kv.putRequests = make(map[int64]DuplicatePut)
  kv.getRequests = make(map[int64]DuplicateGet)

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

