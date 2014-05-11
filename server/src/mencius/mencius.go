package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
import "encoding/gob"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  instances map[int]*AgreementInstance
  instancesMapMu sync.Mutex

  instanceNumMu sync.Mutex

  doneNums []int // array representing each peer's doneNum
  currentInstanceNum int
  menciusNumWorkers int
  hasIncomingOp bool
}


type Type string

type Op struct {
  // Your data here.
  Type Type
  GID int64
  //Num int
  Shard int
  Servers []string
}


type AgreementInstance struct {
  InstanceNum int
  Decided bool

  // acceptor state
  N_Prepare int64 // highest prepare number seen
  N_Accept int64 // highest accept number seen
  V_Accept interface{} // highest accept value seen

  DecidedVal interface{} 
}

type Prepare struct {
  InstanceNum int
  ProposalNum int64
}

type PrepareOK struct {
  InstanceNum int
  OK string
  ProposalNumAccept int64
  ValAccept interface{}
  DoneNum int // piggybacked
}

type Accept struct {
  InstanceNum int
  ProposalNum int64
  ValAccept interface{}
}

type AcceptOK struct {
  InstanceNum int
  OK string
  Num int64
  DoneNum int // piggybacked
}

type Decided struct {
  InstanceNum int
  Val_Decided interface{}
}

type DecidedOK struct {
  DoneNum int // piggybacked
}

type NoOp struct {
  Name string
}



func mod (x int, y int) int {
  if x == -1 {
    return y - 1
  }
  return x % y
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

func (px *Paxos) makeNewAgreementInstance(instanceNum int) AgreementInstance {
  return AgreementInstance{InstanceNum: instanceNum, 
                           Decided: false,
                           N_Prepare: -1,
                           N_Accept: -1,
                           V_Accept: nil,
                           DecidedVal: nil}
}

func (px *Paxos) PrepareHandler(args *Prepare, reply *PrepareOK) error {
  // fmt.Println("PrepareHandler in ", px.me, args.InstanceNum)
  px.mu.Lock()
  defer px.mu.Unlock()


  px.instancesMapMu.Lock()
  if _, exist := px.instances[args.InstanceNum]; !exist {
    agreementInstance := px.makeNewAgreementInstance(args.InstanceNum)
    px.instances[args.InstanceNum] = &agreementInstance
  }
  agreementInstance := px.instances[args.InstanceNum]
  px.instancesMapMu.Unlock()

  if args.ProposalNum > agreementInstance.N_Prepare &&
     !agreementInstance.Decided {
    agreementInstance.N_Prepare = args.ProposalNum
    reply.OK = "ok"
    reply.ProposalNumAccept = agreementInstance.N_Accept
    reply.ValAccept = agreementInstance.V_Accept
  } else {
    reply.OK = "reject"
  }

  reply.DoneNum = px.doneNums[px.me]

  return nil
}

func (px *Paxos) AcceptHandler(args *Accept, reply *AcceptOK) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  px.instancesMapMu.Lock()
  if _, exist := px.instances[args.InstanceNum]; !exist {
    agreementInstance := px.makeNewAgreementInstance(args.InstanceNum)
    px.instances[args.InstanceNum] = &agreementInstance
  }


  agreementInstance := px.instances[args.InstanceNum]
  px.instancesMapMu.Unlock()
  if args.ProposalNum >= agreementInstance.N_Prepare &&
     !agreementInstance.Decided {
    agreementInstance.N_Prepare = args.ProposalNum
    agreementInstance.N_Accept = args.ProposalNum
    agreementInstance.V_Accept = args.ValAccept
    reply.OK = "ok"
    reply.InstanceNum = args.InstanceNum
  } else {
    reply.OK = "reject"
  }

  reply.DoneNum = px.doneNums[px.me]

  return nil
}

func (px *Paxos) DecidedHandler(args *Decided, reply *DecidedOK) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  px.instancesMapMu.Lock()
  if _, exist := px.instances[args.InstanceNum]; !exist {
    agreementInstance := px.makeNewAgreementInstance(args.InstanceNum)
    px.instances[args.InstanceNum] = &agreementInstance
  }
  agreementInstance := px.instances[args.InstanceNum]
  px.instancesMapMu.Unlock()

  px.instanceNumMu.Lock() 
  if px.currentInstanceNum < args.InstanceNum + 1 {
    px.currentInstanceNum = args.InstanceNum + 1
  }
  px.instanceNumMu.Unlock()

  agreementInstance.Decided = true
  agreementInstance.DecidedVal = args.Val_Decided

  reply.DoneNum = px.doneNums[px.me]



  // fmt.Println("decided")
  // fmt.Println("---------------")
  // fmt.Println(px.me)
  // fmt.Println(px.currentInstanceNum)
  // fmt.Println(agreementInstance.DecidedVal)

  return nil
}

func (px *Paxos) generateProposalNumber() int64 {
  return time.Now().UnixNano() * int64(len(px.peers)) + int64(px.me)
}

func (px *Paxos) updateDoneNum(peerNum int, newDoneNum int) {
  if newDoneNum > px.doneNums[peerNum] {
    px.doneNums[peerNum] = newDoneNum
  }
}

func (px *Paxos) propose(seq int, v interface{}) {
  majority := (len(px.peers) / 2) + 1
  decided := false

  for !decided && !px.dead{
    // time.Sleep(100 * time.Millisecond)
    // generate higher and unique proposalNumber
    proposalNumber := px.generateProposalNumber()

    // Prepare phase
    prepareReplies := make([]PrepareOK,len(px.peers))

    // Send prepares
    for index, peer := range px.peers {
      // fmt.Println("Peer index: ", index, peer)
      prepareMsg := Prepare{seq, proposalNumber}
      var prepareReply PrepareOK

      if index == px.me {
        px.PrepareHandler(&prepareMsg, &prepareReply)
      } else {
        // fmt.Println("SENDING TO: ", peer, px.peers)
        call(peer, "Paxos.PrepareHandler", prepareMsg, &prepareReply)
        // fmt.Println("Call ok: ", ok, peer)
      }

      prepareReplies[index] = prepareReply
      px.updateDoneNum(index, prepareReply.DoneNum)
    }

    prepareOKCount := 0
    var highestValue interface{}
    highestProposalNum := int64(-1)

    for _, prepareReply := range prepareReplies {
      if prepareReply.OK == "ok" {
        prepareOKCount += 1
        if prepareReply.ProposalNumAccept > highestProposalNum {
          highestProposalNum = prepareReply.ProposalNumAccept
          highestValue = prepareReply.ValAccept
        }
      }
    }

    // Accept phase
    if prepareOKCount >= majority {
      acceptValue := v

      if highestValue != nil {
        acceptValue = highestValue
      }

      decided = px.proposeAcceptPhase(seq, proposalNumber, acceptValue)
    }
  }
}

func (px *Paxos) proposeAcceptPhase(seq int, proposalNumber int64, acceptValue interface{}) bool {
  majority := (len(px.peers) / 2) + 1
  num_accepted := 0
  decided := false

  // send accepts
  for index, peer := range px.peers {
    acceptMsg := Accept{seq, proposalNumber, acceptValue}
    var acceptReply AcceptOK

    if index == px.me {
      px.AcceptHandler(&acceptMsg, &acceptReply)
    } else {
      call(peer, "Paxos.AcceptHandler", acceptMsg, &acceptReply)
    }

    if acceptReply.OK == "ok" {
      num_accepted += 1
    }

    px.updateDoneNum(index, acceptReply.DoneNum)

    if num_accepted >= majority {
      break
    }
  }


  // Decided phase
  if num_accepted >= majority {
    decided = true
    px.proposeDecidedPhase(seq, acceptValue)
  }

  return decided

}

func (px *Paxos) proposeDecidedPhase(seq int, decidedValue interface{}) {
  // decided := true
  // decidedValue := acceptValue

  for index, peer := range px.peers {
    decidedMsg := Decided{seq, decidedValue}
    var decidedReply DecidedOK

    if index == px.me {
      px.DecidedHandler(&decidedMsg, &decidedReply)
    } else {
      call(peer, "Paxos.DecidedHandler", decidedMsg, &decidedReply)
    }

    px.updateDoneNum(index, decidedReply.DoneNum)
  }
}




func (px *Paxos) ProposeSeq (seq int, v interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if px.Min() <= seq{
    agreed, _ := px.Status(seq)
    if agreed{
      return
    }
    go px.propose(seq, v)
  }

  return
} 




//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) int{
  px.mu.Lock()
  defer px.mu.Unlock()

  // px.instancesMapMu.Lock()
  // if _, exist := px.instances[seq]; !exist {
  //   agreementInstance := px.makeNewAgreementInstance(seq)
  //   px.instances[seq] = &agreementInstance
  // }
  // px.instancesMapMu.Unlock()


  deadServerTimeout := 10
  sleepTime := 2
  totalWait := 0
  px.hasIncomingOp = true

  currNum := px.currentInstanceNum

  for mod(currNum, px.menciusNumWorkers) != px.me {
    px.instancesMapMu.Lock()
    if _, exist := px.instances[currNum]; !exist {
      agreementInstance := px.makeNewAgreementInstance(currNum)
      px.instances[currNum] = &agreementInstance
    }
    px.instancesMapMu.Unlock()

    if px.instances[currNum].Decided {
      currNum += 1
      totalWait = 0
    } else {
      time.Sleep(time.Duration(sleepTime) * time.Millisecond)
      totalWait += sleepTime

      if totalWait >= deadServerTimeout {
        // do a normal no-op
        // fmt.Println("propose in Start", px.me)
        go px.propose(currNum, Op{"", 0, -1, []string{}}) // change this to parameterize no-op

        currNum += 1
        totalWait = 0
      }
    }
  }

  go px.proposeAcceptPhase(currNum, px.generateProposalNumber(), v)
  px.hasIncomingOp = false

  px.instanceNumMu.Lock()
  if px.currentInstanceNum < currNum{
    px.currentInstanceNum = currNum
  }
  currNum = px.currentInstanceNum
  px.instanceNumMu.Unlock()

  return currNum
}

func (px *Paxos) MenciusBackgroundThread() {
  for !px.dead {
    // fmt.Println("Current Instance Number: ", px.currentInstanceNum)
    px.instanceNumMu.Lock()
    // fmt.Println("Background: ", px.instances[1])
    if mod(px.currentInstanceNum, px.menciusNumWorkers) == px.me &&
       !px.hasIncomingOp{

      // do a skip
      go px.proposeDecidedPhase(px.currentInstanceNum, Op{"", 0, -1, []string{}}) // parameterize no-op
      px.currentInstanceNum += 1
      px.instanceNumMu.Unlock()
    } else {
      px.instanceNumMu.Unlock()
      time.Sleep(100 * time.Millisecond)
    }
  }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.updateDoneNum(px.me, seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  max := -1
  for instanceNumber, _ := range px.instances {
    if max < instanceNumber {
      max = instanceNumber
    }
  }

  return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  min := px.doneNums[0]

  for _, doneNum := range px.doneNums {
    if doneNum < min {
      min = doneNum
    }
  }

  return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.instancesMapMu.Lock()
  defer px.instancesMapMu.Unlock()
  if agreementInstance, ok := px.instances[seq]; ok {
    // fmt.Println("NOT HOLE", px.instances[seq])
    // if (!agreementInstance.Decided) {
    //   go px.propose(seq, Op{"", 0, -1, []string{}})
    // }
    return agreementInstance.Decided, agreementInstance.DecidedVal
  } else {
    return false, nil
  }
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {

  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.doneNums = make([]int, len(px.peers))

  for i := 0; i < len(px.doneNums); i++ {
    px.doneNums[i] = -1
  }

  px.instances = make(map[int]*AgreementInstance)
  px.currentInstanceNum = -1
  px.menciusNumWorkers = len(peers)
  px.hasIncomingOp = false

  gob.Register(NoOp{})
  gob.Register(Op{})

  go px.MenciusBackgroundThread()
  
  go func(){
    for !px.dead{
      px.mu.Lock()
      min := px.Min()
      px.instancesMapMu.Lock()
      for instanceNum, _ := range px.instances {
        if instanceNum < min {
          delete(px.instances, instanceNum)
        }
      }
      px.instancesMapMu.Unlock()
      px.mu.Unlock()

      time.Sleep(10*time.Millisecond)
    }
  }()

  // Your initialization code here.

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
