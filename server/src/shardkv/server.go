package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "mencius"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "math"
import "shardmaster"
import "strconv"
import "reflect"
import "container/list"

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
  Key string
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
  diskIO DiskIO

  meString string
  keyvalues map[int]map[string]RequestValue // Map of viewnums to keyvalues
  putRequests map[int64]DuplicatePut
  getRequests map[int64]DuplicateGet
  config shardmaster.Config
  peersDone map[string]int
  writeBuffer *list.List
  writeBufLock sync.Mutex
  lru *list.List
  lruCache map[string]*list.Element
  LRU_SIZE int
  lruLock sync.Mutex
  diskLock sync.Mutex
}

func (kv *ShardKV) Agreement(op Op) int {
  var seqNum int

  seqNum = kv.px.Max() + 1
  kv.px.Start(seqNum, op)

  // Wait for agreement
  for {
    to := time.Millisecond
    for {
      decided, theval := kv.px.Status(seqNum)
      if decided {
        val, ok := theval.(Op)
        if ok && reflect.DeepEqual(val, op) {
          // Check if agreed on instance is this instance
          DPrintf("%d [%s %s, %d | %d %d] %d: Agreement %s, %s reached at sequence number %d\n", op.Num, op.OpType, op.Key, key2shard(op.Key), kv.gid, kv.me, op.ClientID, op.Key, op.Value, seqNum)
          return seqNum
        } else {
          // No agreement, have client try again
          seqNum = kv.px.Start(seqNum, op)
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
	kv.writeStateToDisk()
//	kv.lruFlushCache()
    DPrintf("[%s %d %d] Starting reconfiguration %d\n", opType, kv.gid, kv.me, newConfig.Num)
    updates := make(map[int]*UpdateArgs)
    if kv.keyvalues[idx] == nil {
      kv.keyvalues[idx] = make(map[string]RequestValue)
    }
    keyvalues := ""
	keyValuesFromDisk, _ := kv.diskIO.export(idx-1)
    //for k, v := range(kv.keyvalues[idx-1]) {
    for k, v := range(keyValuesFromDisk) {
	  kv.writeToKeyValues(idx, k, v)
     // kv.keyvalues[idx][k] = v
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
              rv := RequestValue{v.Value, v.ClientID, v.RequestTime, seqNum, v.ConfigNum, k}
              kv.writeToKeyValues(idx, k, rv)
//              kv.keyvalues[idx][k] = RequestValue{v.Value, v.ClientID, v.RequestTime, seqNum, v.ConfigNum, k}
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
//	fmt.Println(opType, " Writing reconfig to disk: ",kv.me," conf:",newConfig.Num)
//	kv.diskIO.writeKVMap(idx, kv.keyvalues[idx])
//	kv.writeStateToDisk()
    kv.FreeSnapshots()
  }
}

func (kv *ShardKV) Catchup(seqNum int, opType Type, reconfig bool) {
  DPrintf("%d [%s %d %d] Catching up to sequence number %d\n", kv.config.Num, opType, kv.gid, kv.me, seqNum)
  curSeq := kv.px.Min()
  for curSeq < seqNum {
    decided, theval := kv.px.Status(curSeq)
    if decided {
      val, ok := theval.(Op)
      if ok {
        if val.OpType == PUT {
          dbVal, dbOk := kv.readFromKeyValues(kv.config.Num, val.Key)
          //dbVal, dbOk := kv.keyvalues[kv.config.Num][val.(Op).Key]
          newval := ""
          previousVal := ""
          if val.DoHash {
            curVal, ok := kv.readFromKeyValues(kv.config.Num, val.Key)
            //curVal, ok := kv.keyvalues[kv.config.Num][val.(Op).Key]
            if !ok {
              curVal = RequestValue{"", 0, time.Now(), 0, 0, ""}
            }
            previousVal = curVal.Value
            newval = strconv.Itoa(int(hash(curVal.Value + val.Value)))
          } else {
            newval = val.Value
          }

          if !dbOk {
            //if (dbVal.ConfigNum < val.(Op).Num && val.(Op).Num <= kv.config.Num) {
              DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered for logged config with previous config %d \n", kv.config.Num, opType, val.Key, key2shard(val.Key), kv.gid, kv.me, curSeq, val.Key, val.Value, dbVal.ConfigNum)
              if kv.keyvalues[kv.config.Num] == nil {
                kv.keyvalues[kv.config.Num] = make(map[string]RequestValue)
              }
              rv := RequestValue{newval, val.ClientID, val.RequestTime, curSeq, kv.config.Num, val.Key}
              kv.writeToKeyValues(kv.config.Num, val.Key, rv)
              //kv.keyvalues[kv.config.Num][val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq, kv.config.Num, val.(Op).Key}
              kv.putRequests[val.ClientID] = DuplicatePut{val.RequestTime, previousVal}
          } else if dbVal.ConfigNum <= kv.config.Num && dbVal.SeqNum < curSeq {
            if dbVal.RequestTime.Before(val.RequestTime) { // At most once guarantee
              DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered for config with previous value %s for client %d\n", kv.config.Num, opType, val.Key, key2shard(val.Key), kv.gid, kv.me, curSeq, val.Key, val.Value, previousVal, val.ClientID)
              rv := RequestValue{newval, val.ClientID, val.RequestTime, curSeq, val.Num, val.Key}
              kv.writeToKeyValues(kv.config.Num, val.Key, rv)
              //kv.keyvalues[kv.config.Num][val.(Op).Key] = RequestValue{newval, val.(Op).ClientID, val.(Op).RequestTime, curSeq, val.(Op).Num, val.(Op).Key}
              kv.putRequests[val.ClientID] = DuplicatePut{val.RequestTime, previousVal}
            } else {
              DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered but not executed for config with previous value %s for client %d\n", kv.config.Num, opType, val.Key, key2shard(val.Key), kv.gid, kv.me, curSeq, val.Key, val.Value, previousVal, val.ClientID)
            }
          } else {
            DPrintf("%d [%s %s, %d | %d %d] #%d Catch up - Put %s, %s encountered but not executed for config with previous config %d\nCurrent config is %d, current sequence # is %d, db sequence # is %d", kv.config.Num, opType, val.Key, key2shard(val.Key), kv.gid, kv.me, curSeq, val.Key, val.Value, dbVal.ConfigNum, kv.config.Num, curSeq, dbVal.SeqNum)
          }
        } else if val.OpType == RECONFIG {
          if val.Num > kv.config.Num && reconfig {
            DPrintf("%d [%s %d %d] Catch up - Reconfig encountered. Moving to %d\n", kv.config.Num, opType, kv.gid, kv.me, val.Num)
            kv.Reconfig(val.Num, opType, curSeq)
          }
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
        //testval, _ := kv.keyvalues[kv.config.Num][args.Key]
        val, ok := kv.readFromKeyValues(kv.config.Num, args.Key)
        // testval, _ := kv.diskIO.readValue(kv.config.Num, args.Key)
        if !ok {
          reply.Err = ErrNoKey
        } else {
          reply.Err = OK
          reply.Value = val.Value
          // if (val.Value != testval.Value) {
          // fmt.Println("Check get! ", kv.config.Num, " key: ", args.Key, " memval: ", val.Value, " diskval: ",testval.Value)
        // }
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
          curVal, ok := kv.readFromKeyValues(kv.config.Num, args.Key)
          //curVal, ok := kv.keyvalues[kv.config.Num][args.Key]
          if !ok {
            curVal = RequestValue{"", 0, time.Now(), 0, 0, ""}
          }
          reply.PreviousValue = curVal.Value
          newval = strconv.Itoa(int(hash(curVal.Value + args.Value)))
        } else {
          newval = args.Value
        }

        dbVal, dbOk := kv.readFromKeyValues(kv.config.Num, args.Key)
        //dbVal, dbOk := kv.keyvalues[kv.config.Num][args.Key]
        if !dbOk || dbVal.ConfigNum != kv.config.Num || (dbVal.SeqNum < seqNum && dbVal.RequestTime != args.RequestTime) {
          // Make sure at once
		  rv := RequestValue{newval, args.ClientID, args.RequestTime, seqNum, kv.config.Num, args.Key}
		  kv.writeToKeyValues(kv.config.Num, args.Key, rv)
          //kv.keyvalues[kv.config.Num][args.Key] = RequestValue{newval, args.ClientID, args.RequestTime, seqNum, kv.config.Num, args.Key}
          kv.putRequests[args.ClientID] = DuplicatePut{args.RequestTime, reply.PreviousValue}
//		  fmt.Println("Writing to disk: ",kv.config.Num," key: ",args.Key," val: ", newval)
//		  err := kv.diskIO.writeEncode(kv.config.Num, args.Key, rv)
//		  if err != nil {
//			fmt.Println("Could not write ", kv.config.Num, " key: ",args.Key, "err: ",err.Error())
//		  }
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
//  kv.mu.Lock()
//  defer kv.mu.Unlock()
  kv.writeStateToDisk()
  for {
    kv.Catchup(kv.px.Max() + 1, UPDATE, true)

    if args.Num < kv.config.Num {
      keyvalues := make(map[string]RequestValue)
      keysSent := ""
      keys := ""
      for k, v := range(kv.keyvalues[args.Num]) {
        keys += k + ": " + v.Value + "; Shard" + strconv.Itoa(key2shard(k)) + " Config " + strconv.Itoa(v.ConfigNum) + "\n"
      }
      DPrintf("%d [UPDATE %d %d] Key values for server %d from group %d: %s\n", args.Num, kv.gid, kv.me, args.Server, args.Group, keys)
	  kv.diskLock.Lock()
	  keyvaluesOnDisk,_  := kv.diskIO.export(args.Num)
	  kv.diskLock.Unlock()
      //for k, v := range(kv.keyvalues[args.Num]) { // What if not caught up?
      for k, v := range(keyvaluesOnDisk) { // What if not caught up?
        if key2shard(k) == args.Shard {
          keyvalues[k] = v
//		  if keyvaluesOnDisk[k].Value != kv.keyvalues[args.Num][k].Value {
//			fmt.Println(args.Num,"Update MISMATCH for ",k," mem: ",kv.keyvalues[args.Num][k].Value," disk: ",keyvaluesOnDisk[k].Value)
//		  }
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
  DPrintf("[TICK %d %d]\n", kv.gid, kv.me)
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
  kv.writeStateToDisk()
}

func (kv *ShardKV) writeToKeyValues(configNum int, key string, rv RequestValue) {
  kv.diskLock.Lock()
  defer kv.diskLock.Unlock()

  kv.keyvalues[configNum][key] = rv
  rv.ConfigNum = configNum
  kv.lruPut(key, rv)
//  fmt.Println("pushing to buffer: c: ",configNum,"k: ",key,"v:",rv.Value)
  kv.writeBuffer.PushBack(rv)
}

func (kv *ShardKV) readFromKeyValues(configNum int, key string) (RequestValue, bool) {
    kv.diskLock.Lock()
    defer kv.diskLock.Unlock()

//    val, OK := kv.keyvalues[configNum][key]
    val, OK := kv.lruGet(key)

//	if val.Value != memval.Value {
//		fmt.Println(kv.gid,"lru differs from mem, c: ",configNum," k:",key,"v:",memval.Value,"lruv:",val.Value)
//	}
   // val, OK = kv.lruGet(key)

	if !OK || val.ConfigNum != configNum {
		OK = false
		//If cannot read from memory, try reading from disk
//		fmt.Println(kv.gid,"Try from disk c: ",configNum," k:",key)
		diskval, err := kv.diskIO.readValue(configNum, key)
		if err == nil {
			OK = true
			val = diskval
		}
	}

	return val, OK
}

//
// Load any saved state from disk
//
func (kv *ShardKV) loadStateFromDisk() {
  kv.diskLock.Lock()
  defer kv.diskLock.Unlock()

  //keyvalues
  latestConfigNum, _ := kv.diskIO.latestConfigNum()

  //putRequests map[int64]DuplicatePut
  savedPutState,err := kv.diskIO.readPutState(latestConfigNum)
  if err != nil {
	  return
  }
  savedGetState,err := kv.diskIO.readGetState(latestConfigNum)
  if err != nil {
	  return
  }
  savedConfigState,err := kv.diskIO.readConfigState(latestConfigNum)
  if err != nil {
	  return
  }
  savedPeersState,err := kv.diskIO.readPeersState(latestConfigNum)

  if err != nil {
	kv.putRequests = savedPutState
	kv.getRequests = savedGetState
	kv.config = savedConfigState
	kv.peersDone = savedPeersState
  }

  // paxos

}

//
// Write state to disk
//
func (kv *ShardKV) writeStateToDisk() {
  kv.diskLock.Lock()
  defer kv.diskLock.Unlock()

  // Paxos state

  // Shardkv state
  kv.diskIO.writeEncode(kv.config.Num, "putRequestState", kv.putRequests)
  kv.diskIO.writeEncode(kv.config.Num, "getRequestState", kv.getRequests)
  kv.diskIO.writeEncode(kv.config.Num, "configState", kv.config)
  kv.diskIO.writeEncode(kv.config.Num, "peersDoneState", kv.peersDone)

  // Flush write buffer
  for e := kv.writeBuffer.Front(); e != nil; e = e.Next() {
	  rv := e.Value.(RequestValue)
//  	fmt.Println(kv.gid,"Writing to disk from buffer c: ",rv.ConfigNum,"key:",rv.Key,"val:",rv.Value)
	err := kv.diskIO.writeEncode(rv.ConfigNum, rv.Key, rv)
	  if err != nil {
		fmt.Println("could not write to buffer",err.Error())
	  }
//  	fmt.Println(kv.gid,"Wrote to disk from buffer c: ",rv.ConfigNum,"key:",rv.Key,"val:",rv.Value)
  }

  kv.writeBuffer = list.New()
}

func (kv *ShardKV) lruGet(key string) (RequestValue, bool) {
	kv.lruLock.Lock()
	defer kv.lruLock.Unlock()

	val, ok := kv.lruCache[key]
	if !ok {
		return RequestValue{}, false
	}
	kv.lru.MoveToBack(val)
	return val.Value.(RequestValue), true
}

func (kv *ShardKV) lruPut(key string, rv RequestValue) {
	kv.lruLock.Lock()
	defer kv.lruLock.Unlock()

	prevval, exists := kv.lruCache[key]
	if exists {
		kv.lru.Remove(prevval)
	}

	e := kv.lru.PushBack(rv)
	kv.lruCache[key] = e

	if kv.lru.Len() > kv.LRU_SIZE {
		front := kv.lru.Front()
		removedVal := kv.lru.Remove(front)
		delete(kv.lruCache, removedVal.(RequestValue).Key)
	}
}


func (kv *ShardKV) lruFlushCache() {
	kv.lruLock.Lock()
	defer kv.lruLock.Unlock()

	kv.lru = kv.lru.Init()
	kv.lruCache = make(map[string]*list.Element)
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
//  kv.diskIO.cleanState()
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
  t := time.Now().Local()
  kv.diskIO.BasePath = "/tmp/Data/" + t.Format("20060102150405")
  kv.diskIO.me = strconv.FormatInt(gid,10)

  kv.keyvalues = make(map[int]map[string]RequestValue)
  kv.keyvalues[0] = make(map[string]RequestValue)
  kv.getRequests = make(map[int64]DuplicateGet)
  kv.putRequests = make(map[int64]DuplicatePut)
  kv.peersDone = make(map[string]int)
  kv.peersDone[kv.meString] = 0

  kv.writeBuffer = list.New()
  kv.lru = list.New()
  kv.lruCache = make(map[string]*list.Element)
  kv.LRU_SIZE = 20
  // Don't call Join().

  kv.loadStateFromDisk()

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
        kv.kill()
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
