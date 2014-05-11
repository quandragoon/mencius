package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "math"
import "shardmaster"
import "strconv"
import "reflect"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
          log.Printf(format, a...)
  }
  return
}

const (
  PUT = "PUT"
  GET = "GET"
  RECONFIG = "RECONFIG"
  UPDATE = "UPDATE"
  TICK = "TICK"
)

type Type string

type Op struct {
  OpType Type
  Key string
  Value string
  Num int
  
  DoHash bool
  ClientID int64
  RequestTime time.Time
}

type RequestValue struct {
  Value string
  ClientID int64
  RequestTime time.Time
  SeqNum int
  ConfigNum int
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  meString string
  keyvalues map[int]map[string]RequestValue // Map of viewnums to keyvalues
  putRequests map[int64]DuplicatePut
  getRequests map[int64]DuplicateGet
  config shardmaster.Config
  peersDone map[string]int
}

func (kv *ShardKV) Agreement(op Op) int {
  var seqNum int

  seqNum = kv.px.Max() + 1
  kv.px.Start(seqNum, op)

  // Wait for agreement
  for {
    to := time.Millisecond
    for {
      decided, val := kv.px.Status(seqNum)
      if decided {
        if reflect.DeepEqual(val.(Op), op) {
          // Check if agreed on instance is this instance
          DPrintf("%d [%s %s, %d | %d %d] %d: Agreement %s, %s reached at sequence number %d\n", op.Num, op.OpType, op.Key, key2shard(op.Key), kv.gid, kv.me, op.ClientID, op.Key, op.Value, seqNum)
          return seqNum
        } else {
          // No agreement, have client try again
          seqNum = kv.px.Max() + 1
          kv.px.Start(seqNum, op)
          break
        }
      }
      time.Sleep(to)
      if to < 10 * time.Second {
        to *= 2
      }
    }
  }
}

func (kv *ShardKV) FreeSnapshots() {
  // Memory management
  // Update list of servers
  for gid, servers := range(kv.config.Groups) {
    for _, server := range(servers) {
      if _, ok := kv.peersDone[server]; !ok && gid != kv.gid {
        kv.peersDone[server] = 0;
      }
    }
  }
  // Update self
  kv.ConfigDone(kv.config.Num - 1, kv.meString)

  // Find min
  min := math.MaxInt32
  for _, config := range(kv.peersDone) {
    if config < min {
      min = config
    }
  }

  // Release everything below the min
  for i := 0; i < min; i++ {
    if _, ok := kv.keyvalues[i]; ok {
      kv.keyvalues[i] = nil
    }
  }

  DPrintf("[%d %d] Min config among peers is %d. %s", kv.gid, kv.me, min, kv.keyvalues[1])
}

func (kv *ShardKV) ConfigDone(num int, server string) {
  val, ok := kv.peersDone[server]
  if !ok || val < num {
    kv.peersDone[server] = num
  }
}

func (kv *ShardKV) Reconfig(idx int, opType Type, seqNum int) {
  newConfig := kv.sm.Query(idx)
  if newConfig.Num > kv.config.Num {
    DPrintf("[%s %d %d] Starting reconfiguration %d\n", opType, kv.gid, kv.me, newConfig.Num)
    updates := make(map[int]*UpdateArgs)
    if kv.keyvalues[idx] == nil {
      kv.keyvalues[idx] = make(map[string]RequestValue)
    }
    keyvalues := ""
    for k, v := range(kv.keyvalues[idx-1]) {
      kv.keyvalues[idx][k] = v
      keyvalues += k + ": " + v.Value + "; Shard " + strconv.Itoa(key2shard(k)) + " Config " + strconv.Itoa(v.ConfigNum) + "\n"
    }
    DPrintf("[%s %d %d] Keyvalues before reconfiguration for config %d: %s\n", opType, kv.gid, kv.me, idx, keyvalues)
    if Debug > 0 {
      shards := ""
      for i := 0; i < shardmaster.NShards; i++ {
        shards += strconv.FormatInt(kv.config.Shards[i], 10) + " "
      }
      DPrintf("[%s %d %d] Shards %d before: %s\n", opType, kv.gid, kv.me, kv.config.Num, shards)
      shards = ""
      for i := 0; i < shardmaster.NShards; i++ {
        shards += strconv.FormatInt(newConfig.Shards[i], 10) + " "
      }
      DPrintf("[%s %d %d] Shards %d after: %s\n", opType, kv.gid, kv.me, newConfig.Num, shards)
    }

    for shard, gid := range(newConfig.Shards) {
      if gid == kv.gid && kv.config.Shards[shard] != kv.gid {
        updates[shard] = &UpdateArgs{idx - 1, shard, kv.gid, kv.me, kv.meString}
      }
    }

    for shard, update := range(updates) {
      gid := kv.config.Shards[shard]
      pastConfig := kv.config
      updated := false
      //to := time.Millisecond
      requestOk := false
      outer:
      for !updated && len(pastConfig.Groups[gid]) > 0 && !kv.dead {
         DPrintf("%d [TICK %d %d] Asking for shard %d from group %d\n", idx, kv.gid, kv.me, shard, gid)
        for _, server := range(pastConfig.Groups[gid]) {
          var reply UpdateReply
          DPrintf("%d [%s %d %d] Asking for shard %d from group %d server %s with config %d\n", idx, opType, kv.gid, kv.me, shard, gid, server, update.Num)
          requestOk = call(server, "ShardKV.Update", update, &reply)
          if requestOk && reply.Err == OK {
            for k, v := range(reply.KeyValues) {
              kv.keyvalues[idx][k] = RequestValue{v.Value, v.ClientID, v.RequestTime, seqNum, v.ConfigNum}
            }

            for id, request := range(reply.ClientPut) {
              if _, ok := kv.putRequests[id]; !ok || kv.putRequests[id].RequestTime.Before(request.RequestTime) {
                kv.putRequests[id] = request
              }
            }

            for id, request := range(reply.ClientGet) {
              if _, ok := kv.getRequests[id]; !ok || kv.getRequests[id].RequestTime.Before(request.RequestTime) {
                kv.getRequests[id] = request
              }
            }

            updated = true

            // If updated is true, then the contacted server has config number idx - 1
            kv.ConfigDone(update.Num, server)
          } else {
            DPrintf("%d [%s %d %d] Group %d server %s had no keys to give for shard %d config %d: %b, %b\n", idx, opType, kv.gid, kv.me, gid, server, shard, update.Num, requestOk, updated)
            //time.Sleep(time.Millisecond * 100)
          }
        }
        if updated {
          break outer
        }
        time.Sleep(time.Millisecond * 100)
      }
    }
    keyvalues = ""
    for k, v := range(kv.keyvalues[idx]) {
      keyvalues += k + ": " + v.Value + "; Shard " + strconv.Itoa(key2shard(k)) + " Config " + strconv.Itoa(v.ConfigNum) + "\n"
    }
    DPrintf("[%s %d %d] Updated to config %d, keyvalue now %s\n", opType, kv.gid, kv.me, idx, keyvalues)
    kv.config = newConfig // Not sure about this
    kv.FreeSnapshots()
  }
}

func (kv *ShardKV) Catchup(seqNum int, opType Type, reconfig bool) {
  DPrintf("%d [%s %d %d] Catching up to sequence number %d\n", kv.config.Num, opType, kv.gid, kv.me, seqNum)
  curSeq := kv.px.Min()
  for curSeq < seqNum {
    decided, val := kv.px.Status(curSeq)
    if decided {
      if val.(Op).OpType == PUT {
        dbVal, dbOk := kv.keyvalues[kv.config.Num][val.(Op).Key]
        newval := ""
        previousVal := ""
        if val.(Op).DoHash {
          curVal, ok := kv.keyvalues[kv.config.Num][val.(Op).Key]
          if !ok {
            curVal = RequestValue{"", 0, time.Now(), 0, 0}
          }
          previousVal = curVal.Value
          newval = strconv.Itoa(int(hash(curVal.Value + val.(Op).Value)))
        } else {
          newval = val.(Op).Value
        }

        if !dbOk {
          //if (dbVal.ConfigNum < val.(Op).Num && val.(Op).Num <= kv.config.Num) {
            DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered for logged config with previous config %d \n", kv.config.Num, opType, val.(Op).Key, key2shard(val.(Op).Key), kv.gid, kv.me, curSeq, val.(Op).Key, val.(Op).Value, dbVal.ConfigNum)
            if kv.keyvalues[kv.config.Num] == nil {
              kv.keyvalues[kv.config.Num] = make(map[string]RequestValue)
            }
            kv.keyvalues[kv.config.Num][val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq, kv.config.Num}
            kv.putRequests[val.(Op).ClientID] = DuplicatePut{val.(Op).RequestTime, previousVal}
        } else if dbVal.ConfigNum <= kv.config.Num && dbVal.SeqNum < curSeq {
          if dbVal.RequestTime.Before(val.(Op).RequestTime) { // At most once guarantee
            DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered for config with previous value %s for client %d\n", kv.config.Num, opType, val.(Op).Key, key2shard(val.(Op).Key), kv.gid, kv.me, curSeq, val.(Op).Key, val.(Op).Value, previousVal, val.(Op).ClientID)
            kv.keyvalues[kv.config.Num][val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq, val.(Op).Num}
            kv.putRequests[val.(Op).ClientID] = DuplicatePut{val.(Op).RequestTime, previousVal}
          } else {
            DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered but not executed for config with previous value %s for client %d\n", kv.config.Num, opType, val.(Op).Key, key2shard(val.(Op).Key), kv.gid, kv.me, curSeq, val.(Op).Key, val.(Op).Value, previousVal, val.(Op).ClientID)
          }
        } else {
          DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered but not executed for config with previous config %d\nCurrent config is %d, current sequence # is %d, db sequence # is %d", kv.config.Num, opType, val.(Op).Key, key2shard(val.(Op).Key), kv.gid, kv.me, curSeq, val.(Op).Key, val.(Op).Value, dbVal.ConfigNum, kv.config.Num, curSeq, dbVal.SeqNum)
        }
      } else if val.(Op).OpType == RECONFIG {
        if val.(Op).Num > kv.config.Num && reconfig {
          DPrintf("%d [%s %d %d] Catch up - Reconfig encountered. Moving to %d\n", kv.config.Num, opType, kv.gid, kv.me, val.(Op).Num)
          kv.Reconfig(val.(Op).Num, opType, curSeq)
        }
      }
      curSeq++
    } else {
      if curSeq >= kv.px.Min() {
        DPrintf("[%s %d %d] Catchup - Sequence number %d not decided on yet\n", opType, kv.gid, kv.me, curSeq)
        noOp := Op{"", "", "", -1, false, 0, time.Now()}
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
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  duplicate, ok := kv.getRequests[args.ClientID]
  if (ok && duplicate.RequestTime.Before(args.RequestTime)) || !ok {
    //curConfig := kv.sm.Query(-1)
    //kv.Catchup(kv.px.Max() + 1, GET, true)
    //if kv.config.Shards[key2shard(args.Key)] == kv.gid  && kv.config.Num == curConfig.Num {
      getOp := Op{GET, args.Key, "", kv.config.Num, false, args.ClientID, args.RequestTime}
      seqNum := kv.Agreement(getOp)
      kv.Catchup(seqNum, GET, true)
      if kv.config.Shards[key2shard(args.Key)] == kv.gid {
        val, ok := kv.keyvalues[kv.config.Num][args.Key]
        if !ok {
          reply.Err = ErrNoKey
        } else {
          reply.Err = OK
          reply.Value = val.Value
          DPrintf("%d [GET %s, %d | %d %d] %d: Returning %s for key %s\n", kv.config.Num, args.Key, key2shard(args.Key),kv.gid, kv.me, args.ClientID, val.Value, args.Key)
        }
        kv.px.Done(seqNum)
      } else {
       DPrintf("%d [GET %s, %d | %d %d] %d: Wrong group - Key %s is for group %d\n", kv.config.Num, args.Key, key2shard(args.Key),kv.gid, kv.me, args.ClientID, args.Key, kv.config.Shards[key2shard(args.Key)] )
       reply.Err = ErrWrongGroup
      }
  } else {
    reply.Err = OK
    reply.Value = duplicate.Value
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  duplicate, ok := kv.putRequests[args.ClientID]
  if (ok && duplicate.RequestTime.Before(args.RequestTime)) || !ok {
    if kv.config.Num == 0 {
      reply.Err = ErrWrongGroup
    } else {
    //kv.Catchup(kv.px.Max() + 1, PUT, true)
    //if kv.config.Shards[key2shard(args.Key)] == kv.gid && kv.config.Num == curConfig.Num {
      putOp := Op{PUT, args.Key, args.Value, kv.config.Num, args.DoHash, args.ClientID, args.RequestTime}
      seqNum := kv.Agreement(putOp)
      kv.Catchup(seqNum, PUT, true)
      if kv.config.Shards[key2shard(args.Key)] == kv.gid {
        newval := ""
        if args.DoHash {
          curVal, ok := kv.keyvalues[kv.config.Num][args.Key]
          if !ok {
            curVal = RequestValue{"", 0, time.Now(), 0, 0}
          }
          reply.PreviousValue = curVal.Value
          newval = strconv.Itoa(int(hash(curVal.Value + args.Value)))
        } else {
          newval = args.Value
        }

        dbVal, dbOk := kv.keyvalues[kv.config.Num][args.Key]
        if !dbOk || dbVal.ConfigNum != kv.config.Num || (dbVal.SeqNum < seqNum && dbVal.RequestTime != args.RequestTime) {
          // Make sure at once
          kv.keyvalues[kv.config.Num][args.Key] = RequestValue{newval, args.ClientID, args.RequestTime, seqNum, kv.config.Num}
          kv.putRequests[args.ClientID] = DuplicatePut{args.RequestTime, reply.PreviousValue}
        } else {
          // Otherwise send back the previous previous-value
          reply.PreviousValue = kv.putRequests[args.ClientID].PreviousValue
          DPrintf("%d [PUT %s, %d | %d %d] %d: Duplicate put request Previousvalue: %s\n", kv.config.Num, args.Key, key2shard(args.Key), kv.gid, kv.me, args.ClientID, reply.PreviousValue)
        }
        reply.Err = OK
        kv.px.Done(seqNum)
      } else {
       DPrintf("%d [PUT %s, %d | %d %d] %d: Wrong group - Key %s is for group %d\n", kv.config.Num, args.Key, key2shard(args.Key),kv.gid, kv.me, args.ClientID, args.Key, kv.config.Shards[key2shard(args.Key)] )
       reply.Err = ErrWrongGroup
      }
    }
  } else {
    reply.Err = OK
    reply.PreviousValue = duplicate.PreviousValue
  }
  return nil
}

func (kv *ShardKV) Update(args *UpdateArgs, reply *UpdateReply) error {
  for {
    kv.Catchup(kv.px.Max() + 1, UPDATE, true)

    if args.Num <= kv.config.Num {
      keyvalues := make(map[string]RequestValue)
      keysSent := ""
      keys := ""
      for k, v := range(kv.keyvalues[args.Num]) {
        keys += k + ": " + v.Value + "; Shard" + strconv.Itoa(key2shard(k)) + " Config " + strconv.Itoa(v.ConfigNum) + "\n"
      }
      DPrintf("%d [UPDATE %d %d] Key values for server %d from group %d: %s\n", args.Num, kv.gid, kv.me, args.Server, args.Group, keys)
      for k, v := range(kv.keyvalues[args.Num]) { // What if not caught up?
        if key2shard(k) == args.Shard {
          keyvalues[k] = v
          keysSent += k + ": " + v.Value + ", "
        }
      }
      if len(keysSent) == 0 {
        reply.Err = OK

        // Server contacting this one is done with the config number
        val, ok := kv.peersDone[args.ServerString]
        if !ok || val < args.Num {
          kv.peersDone[args.ServerString] = args.Num
        }
        DPrintf("%d [UPDATE %d %d] No keys to send to server %d from group %d!\n", args.Num, kv.gid, kv.me, args.Server, args.Group)
        return nil
      } else {
        DPrintf("%d [UPDATE %d %d] Sent keys %s to server %d from group %d\n", args.Num, kv.gid, kv.me, keysSent, args.Server, args.Group)
        reply.KeyValues = keyvalues
        reply.ClientPut = kv.putRequests
        reply.ClientGet = kv.getRequests
        reply.Err = OK
        // Server contacting this one is done with the config number
        kv.ConfigDone(args.Num, args.ServerString)
        return nil
      }
    }
    time.Sleep(time.Millisecond * 100)
  }
  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  curConfig := kv.sm.Query(-1)
  if curConfig.Num > kv.config.Num {
    DPrintf("[TICK %d %d] New config number found: %d, Old: %d\n", kv.gid, kv.me, curConfig.Num, kv.config.Num)
    kv.mu.Lock()
    //kv.Catchup(kv.px.Max() + 1, RECONFIG, true)
    
    idx := kv.config.Num + 1
    for idx <= curConfig.Num {
      configOp := Op{RECONFIG, "", "", idx, false, 0, time.Now()}
      seqNum := kv.Agreement(configOp)
      kv.Catchup(seqNum, RECONFIG, true)
      kv.Reconfig(idx, TICK, seqNum)
      idx = kv.config.Num + 1 // Reconfig sets new kv.config
      kv.px.Done(seqNum)
    }
    kv.mu.Unlock()
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) Kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(GetReply{})
  gob.Register(PutReply{})
  gob.Register(UpdateArgs{})
  gob.Register(UpdateReply{})
  gob.Register(RequestValue{})
  gob.Register(DuplicateGet{})
  gob.Register(DuplicatePut{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.meString = servers[kv.me]

  // Your initialization code here.
  kv.keyvalues = make(map[int]map[string]RequestValue)
  kv.keyvalues[0] = make(map[string]RequestValue)
  kv.getRequests = make(map[int64]DuplicateGet)
  kv.putRequests = make(map[int64]DuplicatePut)
  kv.peersDone = make(map[string]int)
  kv.peersDone[kv.meString] = 0
  // Don't call Join().

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.Kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}

func (kv *ShardKV) IsDead() bool {
  return kv.dead
}

func (kv *ShardKV) IsUnreliable() bool {
  return kv.unreliable
}
//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func SetupServer(gid int64, shardmasters []string,
                 servers []string, me int) (*ShardKV, *rpc.Server) {
  gob.Register(Op{})
  gob.Register(GetReply{})
  gob.Register(PutReply{})
  gob.Register(UpdateArgs{})
  gob.Register(UpdateReply{})
  gob.Register(RequestValue{})
  gob.Register(DuplicateGet{})
  gob.Register(DuplicatePut{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.meString = servers[kv.me]

  // Your initialization code here.
  kv.keyvalues = make(map[int]map[string]RequestValue)
  kv.keyvalues[0] = make(map[string]RequestValue)
  kv.getRequests = make(map[int64]DuplicateGet)
  kv.putRequests = make(map[int64]DuplicatePut)
  kv.peersDone = make(map[string]int)
  kv.peersDone[kv.meString] = 0
  // Don't call Join().

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)


  os.Remove(servers[me])
  // l, e := net.Listen("unix", servers[me]);
  // if e != nil {
  //   log.Fatal("listen error: ", e);
  // }


  // kv.l = l

  // // please do not change any of the following code,
  // // or do anything to subvert it.

  // go func() {
  //   for kv.dead == false {
  //     conn, err := kv.l.Accept()
  //     if err == nil && kv.dead == false {
  //       if kv.unreliable && (rand.Int63() % 1000) < 100 {
  //         // discard the request.
  //         conn.Close()
  //       } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
  //         // process the request but force discard of reply.
  //         c1 := conn.(*net.UnixConn)
  //         f, _ := c1.File()
  //         err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
  //         if err != nil {
  //           fmt.Printf("shutdown: %v\n", err)
  //         }
  //         go rpcs.ServeConn(conn)
  //       } else {
  //         go rpcs.ServeConn(conn)
  //       }
  //     } else if err == nil {
  //       conn.Close()
  //     }
  //     if err != nil && kv.dead == false {
  //       fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
  //       kv.kill()
  //     }
  //   }
  // }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv, rpcs
}
