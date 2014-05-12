package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "mencius"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "math"
import "time"

const DEBUG = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if DEBUG {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  configSeqNum []int // Sequence number of config
}


type Op struct {
  // Your data here.
  Type Type
  GID int64
  //Num int
  Shard int
  Servers []string
}

func ElemInArray(elem int64, arr []int64) bool {
  exists := false
  for _, v := range arr {
    if v == elem {
      exists = true
    }
  }

  return exists
}

func OpsAreEqual(op1 Op, op2 Op) bool {
  same := true
  same = same && (op1.Type == op2.Type) && (op1.GID == op2.GID) && (op1.Shard == op2.Shard)
  same = same && (len(op1.Servers) == len(op2.Servers))
  if same {
    for i, v := range(op1.Servers) {
      same = same && (v == op2.Servers[i])
    }
  }
  return same
}

func (sm *ShardMaster) ExtendAndCopy(opType Type, GID int64, servers []string, seqNum int) {
  configNum := len(sm.configs) + 1

  // Copy
  newConfig := Config{Num: configNum - 1, Groups: make(map[int64][]string)}
  for k, v := range sm.configs[configNum - 2].Groups {
    if ((opType == LEAVE && k != GID) || opType != LEAVE) {
      newConfig.Groups[k] = v
    }
  }

  if opType == JOIN {
    newConfig.Groups[GID] = servers
  }

  // Append
  sm.configs = append(sm.configs, newConfig)
  sm.configSeqNum = append(sm.configSeqNum, seqNum)
}

func (sm *ShardMaster) ProposeOp(op Op, seqNum int, opType Type) int {
  // Retry for failure..?
  curSeqNum := seqNum
  for {
    to := time.Millisecond
    for {
      decided, val := sm.px.Status(curSeqNum)
      // DPrintf("%s %d: Waiting for response for GID %d\n", opType, op.GID, op.GID)
      if decided {
        // fmt.Println(val) 
        // val, ok := val.(Op)
        // fmt.Println(val)
        // fmt.Println(ok)
        // if ok && OpsAreEqual(val, op) {

        val, _ := val.(Op)
        if val.Type == "" {
          val.Shard = -1
        }

        // fmt.Println(val) 
        // fmt.Println(op)
        // fmt.Println(OpsAreEqual(val, op))

        if OpsAreEqual(val, op) {
          // Check if agreed on instance is this instance
          DPrintf("%s %d: Decided on op with sequence number %d\n", opType, op.GID, curSeqNum)
          return curSeqNum
        } else {
          // No agreement, have client try again
          // DPrintf("%s %d: No agreement on sequence number %d, catching up before trying again\n", opType, op.GID, curSeqNum)
          // curSeqNum = sm.px.Max() + 1
          // sm.Catchup(curSeqNum, opType, op.GID)
          op = Op{opType, op.GID, op.Shard, op.Servers}
          curSeqNum = sm.px.Start(curSeqNum, op)
          DPrintf("%s %d: No decision was made! Trying sequence number %d\n", opType, op.GID, curSeqNum)
          time.Sleep(100*time.Millisecond)
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

func (sm *ShardMaster) Catchup(seqNum int, opType Type, GID int64) {
  DPrintf("%s %d: Catching up first\n", opType, GID)
  // Catchup
  curSeq := sm.px.Min()
  for curSeq < seqNum {
    decided, val := sm.px.Status(curSeq)
    // time.Sleep(100*time.Millisecond)
    if decided {
      val, ok := val.(Op)
      if ok && val.Type != QUERY && val.Type != ""{ // Don't care about QUERY ops
        if (len(sm.configSeqNum) > 0 && curSeq > sm.configSeqNum[len(sm.configSeqNum) - 1]) || len(sm.configSeqNum) == 0 { 
          DPrintf("%s %d: Checking sequence number %d, type %s, %s\n", opType, GID, curSeq, val.Type, val)
          sm.ExtendAndCopy(val.Type, val.GID, val.Servers, curSeq)
          switch {
          case val.Type == JOIN:
            sm.BalanceJoin(val.GID)
          case val.Type == LEAVE:
            sm.BalanceLeave(val.GID)
          case val.Type == MOVE:
            sm.BalanceMove(val.GID, val.Shard)
          }
        }
      } 
      curSeq++
    } else {
      if curSeq >= sm.px.Min() {
        // fmt.Println("IN HEREEEE")
        DPrintf("%s %d: Sequence number %d not decided yet, waiting...\n", opType, GID, curSeq)
        // noOp := Op{"", 0, -1, []string{}}
        // sm.px.Start(curSeq, noOp)
        to := time.Millisecond
        for {
          decided, _ := sm.px.Status(curSeq)
          // fmt.Println(sm.px.Status(curSeq))
          if decided {
            break
          }
          curSeq++
          time.Sleep(to)
          if to < 10 * time.Second {
            to *= 2
          }
        }
        time.Sleep(100*time.Millisecond)
      } else {
        curSeq++
      }
    }
  }
  DPrintf("%s %d: Finished catchup\n", opType, GID)
}

func (sm *ShardMaster) BalanceJoin(GID int64) {
  configNum := len(sm.configs)

  // Balance load
  shardPerGroup := int(math.Max(float64(NShards / len(sm.configs[configNum - 1].Groups)), 1.0))
  shardCount := make(map[int64]int)
  remainingShards := 0
  if len(sm.configs[configNum - 1].Groups) < NShards {
    remainingShards = NShards % len(sm.configs[configNum - 1].Groups)
  }

  DPrintf("JOIN %d: Now have %d groups, will move to %d shards per group with %d left over\n", GID, len(sm.configs[configNum - 1].Groups), shardPerGroup, remainingShards)
  for i, group := range sm.configs[configNum - 2].Shards {
    _, ok := shardCount[group]
    if !ok && group != 0 {
      shardCount[group] = 1
      sm.configs[configNum - 1].Shards[i] = group
    } else if group == 0 { // Check if too many?
      shardCount[GID]++
      sm.configs[configNum - 1].Shards[i] = GID
    } else {
      if shardCount[group] + 1 <= shardPerGroup {
        shardCount[group] = shardCount[group] + 1
        sm.configs[configNum - 1].Shards[i] = group
      } else if shardCount[group] == shardPerGroup && remainingShards > 0 {
        shardCount[group] = shardCount[group] + 1
        sm.configs[configNum - 1].Shards[i] = group
        remainingShards--
      } else { // Need to move it
        shardCount[GID]++
        sm.configs[configNum - 1].Shards[i] = GID
      }
    }
  }
}

func (sm *ShardMaster) BalanceLeave(GID int64) {
  configNum := len(sm.configs)

  // Balance load
  shardPerGroup := int(math.Max(float64(NShards / len(sm.configs[configNum - 1].Groups)), 1.0))
  shardCount := make(map[int64]int)
  remainingShards := 0
  if len(sm.configs[configNum - 1].Groups) < NShards {
    remainingShards = NShards % len(sm.configs[configNum - 1].Groups)
  }

  DPrintf("LEAVE %d: Now have %d groups, will move to %d shards per group with %d left over\n", GID, len(sm.configs[configNum - 1].Groups), shardPerGroup, remainingShards)

  var withoutGroup [NShards]int64
  numGroups := len(sm.configs[configNum - 1].Groups)
  var withoutGroupIDs = make([]int64, numGroups)
  backidx := len(withoutGroupIDs) - 1
  for i, group := range sm.configs[configNum - 2].Shards {
    if group != GID {
      withoutGroup[i] = group
      if !ElemInArray(group, withoutGroupIDs) {
        withoutGroupIDs[backidx] = group
        backidx--
      }
    } else {
      withoutGroup[i] = 0
    }
  }

  idx := 0
  for group, _ := range sm.configs[configNum - 2].Groups {
    if group != GID {
      if !ElemInArray(group, withoutGroupIDs) {
        withoutGroupIDs[idx] = group
        idx++
      }
    }
  }

  idx = 0
  for i, group := range withoutGroup {
    _, ok := shardCount[group]
    if !ok && group != 0 {
      shardCount[group] = 1
      sm.configs[configNum - 1].Shards[i] = group
      //DPrintf("JOIN: Shard %d not yet counted, remaining with group %d\n", i, shard)
    } else if group == 0 { // Check if too many?
      for {
        if shardCount[withoutGroupIDs[idx]] < shardPerGroup {
          sm.configs[configNum - 1].Shards[i] = withoutGroupIDs[idx]
          shardCount[withoutGroupIDs[idx]]++
          break
        } else if shardCount[withoutGroupIDs[idx]] == shardPerGroup && remainingShards > 0 {
          sm.configs[configNum - 1].Shards[i] = withoutGroupIDs[idx]
          shardCount[withoutGroupIDs[idx]]++
          remainingShards--
          idx++
          break
        } else {
          // ruh roh
          idx++
        }
      }
      //DPrintf("JOIN: Shard %d not yet assigned, assigning to new group %d\n", i, args.GID)
    } else {
      if shardCount[group] < shardPerGroup {
        shardCount[group] = shardCount[group] + 1
        sm.configs[configNum - 1].Shards[i] = group
        //DPrintf("JOIN: Group %d still doesn't have enough shards, assigning %d to it\n", shard, i)
        if shardCount[group] == shardPerGroup && remainingShards == 0 && group == withoutGroupIDs[idx] {
          idx++
        }
      } else if shardCount[group] == shardPerGroup && remainingShards > 0 {
        shardCount[group] = shardCount[group] + 1
        sm.configs[configNum - 1].Shards[i] = group
        remainingShards--

        if group == withoutGroupIDs[idx] {
          idx++
        }
        // DPrintf("JOIN: Group %d has enough shards, now distributing remainder, assigning %d to it\n", shard, i)
      } else { // Need to move it
        DPrintf("LEAVE %d: Should't get here...Shard #%d. Leftover group is %d\n", GID, i, withoutGroupIDs[idx])
        if shardCount[withoutGroupIDs[idx]] < shardPerGroup {
          sm.configs[configNum - 1].Shards[i] = withoutGroupIDs[idx]
          shardCount[withoutGroupIDs[idx]]++
        } else if shardCount[withoutGroupIDs[idx]] == shardPerGroup && remainingShards > 0 {
          sm.configs[configNum - 1].Shards[i] = withoutGroupIDs[idx]
          shardCount[withoutGroupIDs[idx]]++
          remainingShards--
          idx++
        } else {
          DPrintf("LEAVE %d: Really Should't get here...Shard #%d\n", GID, i)
        } // else ruh roh
        // Shouldn't get here
      }
    }
  }
}

func (sm *ShardMaster) BalanceMove(GID int64, shard int) {
  configNum := len(sm.configs)
  DPrintf("MOVE: Moving %d to shard %d\n", GID, shard)

  for i, group := range sm.configs[configNum - 2].Shards {
    sm.configs[configNum - 1].Shards[i] = group
  }
  sm.configs[configNum - 1].Shards[shard] = GID
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  DPrintf("JOIN %d: Starting Join operation for GID %d with server %d\n", args.GID, args.GID, sm.me)
  joinOp := Op{JOIN, args.GID, -1, args.Servers}

  seqNum := sm.px.Max() + 1
  seqNum = sm.px.Start(seqNum, joinOp)

  seqNum = sm.ProposeOp(joinOp, seqNum, JOIN)
  DPrintf("JOIN %d: shard assignments before catchup\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")
  sm.Catchup(seqNum, JOIN, args.GID)
  
  DPrintf("Join %d: Shard assignments before join\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")

  sm.ExtendAndCopy(JOIN, args.GID, args.Servers, seqNum)
  sm.BalanceJoin(args.GID)

  DPrintf("Shard assignments after\n")
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")
  sm.px.Done(seqNum)

  sm.mu.Unlock()
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  DPrintf("LEAVE %d: Starting Leave operation for GID %d with server %d\n", args.GID, args.GID, sm.me)
  leaveOp := Op{LEAVE, args.GID, -1, []string{}}

  seqNum := sm.px.Max() + 1
  seqNum = sm.px.Start(seqNum, leaveOp)

  seqNum = sm.ProposeOp(leaveOp, seqNum, LEAVE)
  DPrintf("LEAVE %d: shard assignments before catchup\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  sm.Catchup(seqNum, LEAVE, args.GID)

  DPrintf("LEAVE %d: Shard assignments before\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")

  sm.ExtendAndCopy(LEAVE, args.GID, []string{}, seqNum)
  sm.BalanceLeave(args.GID)

  DPrintf("LEAVE %d: Shard assignments after\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")
  sm.px.Done(seqNum)

  sm.mu.Unlock()
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  DPrintf("MOVE %d: Starting Move operation for GID %d, assigning it shard %d with server %d\n", args.GID, args.GID, args.Shard, sm.me)
  leaveOp := Op{MOVE, args.GID, args.Shard, []string{}}

  seqNum := sm.px.Max() + 1
  seqNum = sm.px.Start(seqNum, leaveOp)

  seqNum = sm.ProposeOp(leaveOp, seqNum, MOVE)
  sm.Catchup(seqNum, MOVE, args.GID)

  DPrintf("MOVE %d: Config currently has %d groups\n", args.GID, len(sm.configs[len(sm.configs) - 1].Groups))
  DPrintf("MOVE %d: shard assignments before\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")

  sm.ExtendAndCopy(MOVE, args.GID, []string{}, seqNum)
  sm.BalanceMove(args.GID, args.Shard)

  DPrintf("MOVE %d: Shard assignments after\n", args.GID)
  for i, g := range sm.configs[len(sm.configs) - 1].Shards {
    DPrintf("%d: %d, ", i, g)
  }
  DPrintf("\n")
  sm.px.Done(seqNum)

  sm.mu.Unlock()
  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  queryOp := Op{QUERY, -1, -1, []string{}}

  seqNum := sm.px.Max() + 1
  seqNum = sm.px.Start(seqNum, queryOp)

  seqNum = sm.ProposeOp(queryOp, seqNum, QUERY)
  sm.Catchup(seqNum, QUERY, 0)
  sm.configSeqNum = append(sm.configSeqNum, seqNum)

  if args.Num == -1 || args.Num >= len(sm.configs) {
    reply.Config = sm.configs[len(sm.configs) - 1]
  } else {
    reply.Config = sm.configs[args.Num]
  }
  sm.px.Done(seqNum)
  sm.mu.Unlock()
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  gob.Register(QueryReply{})
  gob.Register(JoinReply{})
  gob.Register(LeaveReply{})
  gob.Register(MoveReply{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
