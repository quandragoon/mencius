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
import "strconv"
import "time"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

const (
  PutOp = "PUT"
  GetOp = "GET"
  NoOp = "NOOP"
)

// const (
//   GameMoveOp = "MOVE"
//   NoOp = "NOOP"
// )

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Type string
  Key string
  Value string
  IsPutHash bool
  ClientID int64
  RequestID int64
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  kvStore map[string]string
  lastExecutedSeqId int
  clientExecutedRequestIds map[int64]int64
  clientPreviousResponses map[int64]string
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  getOp := Op{Type: GetOp,
             Key: args.Key,
             Value: "",
             IsPutHash: false,
             ClientID: args.ClientID,
             RequestID: args.RequestID}

  kv.commitOp(getOp)

  value := kv.executeGetOp(getOp)

  kv.lastExecutedSeqId += 1
  kv.px.Done(kv.lastExecutedSeqId)

  if value == "" {
    reply.Err = ErrNoKey
  } else {
    reply.Err = OK
  }

  reply.Value = value

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  putOp := Op{Type: PutOp, 
              Key: args.Key, 
              Value: args.Value, 
              IsPutHash: args.DoHash,
              ClientID: args.ClientID,
              RequestID: args.RequestID}

  kv.commitOp(putOp)

  previousValue := kv.executePutOp(putOp)

  kv.lastExecutedSeqId += 1
  kv.px.Done(kv.lastExecutedSeqId)

  reply.Err = OK
  reply.PreviousValue = previousValue

  return nil
}

// tries to commit an op in the log.
// when it fails on an instance, it will execute whatever value is in there,
// and then move on.
// when it finally commits the operation, it doesn't execute it. it returns
// and lets its caller execute it.
func (kv *KVPaxos) commitOp(operation Op) {
  committed := false

  for !committed {
    currSeqId := kv.lastExecutedSeqId + 1
    decided, thisOp := kv.px.Status(currSeqId)

    if decided {
      kv.executeOp(thisOp.(Op))
      kv.px.Done(currSeqId)
      kv.lastExecutedSeqId = currSeqId
    } else {
      kv.px.Start(currSeqId, operation) 
      decidedOp := kv.waitForPaxos(currSeqId)

      if decidedOp.ClientID == operation.ClientID && 
         decidedOp.RequestID == operation.RequestID {
        committed = true
      }
    }
  }
  return
}

func (kv *KVPaxos) executeOp(operation Op) {
  if kv.clientExecutedRequestIds[operation.ClientID] >= operation.RequestID {
    return
  } else {
    switch operation.Type {
    case PutOp:
      kv.executePutOp(operation)
      return
    case GetOp:
      return 
    case NoOp:
      return
    }
    return   
  }
}

func (kv *KVPaxos) executePutOp(putOp Op) string {
  if kv.clientExecutedRequestIds[putOp.ClientID] >= putOp.RequestID {
    return kv.clientPreviousResponses[putOp.ClientID]
  }
  previousValue := kv.kvStore[putOp.Key]
  newValue := putOp.Value

  if putOp.IsPutHash {
    newString := previousValue + newValue
    newValue = strconv.Itoa(int(hash(newString)))
  }

  kv.kvStore[putOp.Key] = newValue
  kv.clientExecutedRequestIds[putOp.ClientID] = putOp.RequestID
  kv.clientPreviousResponses[putOp.ClientID] = previousValue

  return previousValue
}

func (kv *KVPaxos) executeGetOp(getOp Op) string {
  return kv.kvStore[getOp.Key]
}

func (kv *KVPaxos) waitForPaxos(seqId int) Op {
  to := 10 * time.Millisecond
  for {
    decided, operation := kv.px.Status(seqId)
    if decided {
      return operation.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
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
  kv.kvStore = make(map[string]string)
  kv.lastExecutedSeqId = -1
  kv.clientExecutedRequestIds = make(map[int64]int64)
  kv.clientPreviousResponses = make(map[int64]string)

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

