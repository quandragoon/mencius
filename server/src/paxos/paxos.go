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
import "container/list"
import "time"


type Instance struct{
  highestAccepted int64
  highestResponded int64
  agreed bool
  val interface{}
}


// RPC structs

type PrepareArgs struct {
  Instance int  
  Proposal int64 
  MaxDone int // max done seen
  Me string 
}

type PrepareReply struct {
  MaxProposalAccepted int64
  NextProposalNumber int64
  OK bool
  Value interface{}
}

type AcceptArgs struct {
  Instance int
  Proposal int64
  Value interface{}
}

type AcceptReply struct {
  Proposal int64
  NextProposalNumber int64
  OK bool
}

type DecidedArgs struct {
  Instance int
  Proposal int64
  Value interface{}
}

type DecidedReply struct {
  OK bool
  NextProposalNumber int64
}








type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  instances map[int]Instance
  maxPeerDones map[string]int

  // iLock sync.Mutex
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











func (px *Paxos) ProposalGen() int64{
  return (int64(time.Now().UnixNano()) * int64(len(px.peers))) + int64(px.me)
}








func MakeNewInstance() Instance{
  return Instance{-1, -1, false, nil}
}







func Max(x int, y int) int {
  if x > y{
    return x
  }
  return y
}




func Min(x int, y int) int {
  if x < y {
    return x
  }
  return y
}







func (px *Paxos) GetPaxosInstance(seq int) Instance {

  if instance, found := px.instances[seq]; found {
    return instance
  }
  px.instances[seq] = MakeNewInstance()
  instance := px.instances[seq]
  return instance
}




func (px *Paxos) Propose(instance int, value interface{}) {
//   choose n, unique and higher than any n seen so far
//   send prepare(n) to all servers including self


  majority := (len(px.peers) / 2) + 1
  Done := false
  maxProposalValue := value



  for !Done {
    if px.dead{
      break
    }
    proposal := px.ProposalGen()
    
    P_replies := list.New()

    for _, peer := range px.peers {
      prepareArgs := &PrepareArgs{instance, proposal, px.maxPeerDones[px.peers[px.me]], px.peers[px.me]}
      var reply PrepareReply = PrepareReply{}
      if peer != px.peers[px.me]{
        ok := call(peer, "Paxos.Prepare", prepareArgs, &reply)
        if ok {
          P_replies.PushBack(reply)
        }
      } else { // if self, still send prepare, but no need for RPC
        px.Prepare(prepareArgs, &reply)
        P_replies.PushBack(reply)
      }
    }


    NumPrepareReply := 0
    maxProposal := int64(-1)

    // go through all replies to count majority 
    for p_re := P_replies.Front(); p_re != nil; p_re = p_re.Next(){
      reply := p_re.Value.(PrepareReply)
      if reply.OK {
        NumPrepareReply++
        if reply.MaxProposalAccepted > maxProposal{
          maxProposal = reply.MaxProposalAccepted
          maxProposalValue = reply.Value
        }
      } 
    }
    

    A_replies := list.New()
    
    // if prepare_ok(n_a, v_a) from majority:
    if (NumPrepareReply >= majority) {
      // v' = v_a with highest n_a; choose own v otherwise
      // send accept(n, v') to all
      for _, p := range px.peers {
        acceptArgs := &AcceptArgs{instance, proposal, maxProposalValue}
        var reply AcceptReply = AcceptReply{}
        if p != px.peers[px.me] {
          ok := call(p, "Paxos.Accept", acceptArgs, &reply)
          if ok {
            A_replies.PushBack(reply)
          }
        } else { // if self, still send accept, but not using RPC
          px.Accept(acceptArgs, &reply)
          A_replies.PushBack(reply)
        }
      }


      NumAccepted := 0
      for a_re := A_replies.Front(); a_re != nil; a_re = a_re.Next() {
        reply := a_re.Value.(AcceptReply)
        if reply.OK {
          NumAccepted++
        } 
      }
      // if accept_ok(n) from majority:
      // send decided(v') to all
      if NumAccepted >= majority {
        decidedArgs := &DecidedArgs{instance, proposal, maxProposalValue}
        var reply DecidedReply = DecidedReply{}
        for _, p := range px.peers {
          if p != px.peers[px.me]{
            call(p, "Paxos.Decided", decidedArgs, &reply)
          } else {
            px.Decided(decidedArgs, &reply) // if self
          }
        }
        Done = true
      }
    } 
    // clean up this shit
    px.ForgetMin()
  }
}









// acceptor's prepare(n) handler:
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  
  px.mu.Lock()
  defer px.mu.Unlock()
  
  // update table of what each peer sees
  px.maxPeerDones[args.Me] = Max(px.maxPeerDones[args.Me], args.MaxDone)

  ok := false

  i := px.GetPaxosInstance(args.Instance)
  
  // if n > n_p
  if args.Proposal > i.highestResponded{
    // n_p = n
    i.highestResponded = args.Proposal
    px.instances[args.Instance] = i
    reply.MaxProposalAccepted = i.highestAccepted
    reply.Value = i.val
    // reply prepare_ok(n_a, v_a)
    // else
    //    reply prepare_reject
    ok = true
  } else {
    reply.NextProposalNumber = i.highestResponded
  }
  
  reply.OK = ok

  return nil
}







// acceptor's accept(n, v) handler:
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

  px.mu.Lock()
  defer px.mu.Unlock()


  ok := false
  i := px.GetPaxosInstance(args.Instance)
  // if n >= n_p
  if args.Proposal >= i.highestResponded {
    // n_p = n
    i.highestResponded = args.Proposal
    // n_a = n
    i.highestAccepted = args.Proposal
    // v_a = v
    i.val = args.Value

    px.instances[args.Instance] = i

    reply.Proposal = args.Proposal
    // reply accept_ok(n)
    //   else
    //   reply accept_reject
    ok = true
  } else {
    reply.NextProposalNumber = i.highestResponded
  }

  reply.OK = ok

  return nil
}






func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
 
  px.mu.Lock()
  defer px.mu.Unlock()

  i := px.GetPaxosInstance(args.Instance)
  i.val = args.Value
  i.agreed = true
  px.instances[args.Instance] = i

  reply.OK = true

  return nil
}





//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  
  px.mu.Lock()
  defer px.mu.Unlock()
  
  if px.Min() <= seq{
    agreed, _ := px.Status(seq)
    if agreed{
      return
    }

    go px.Propose(seq, v)
  }
  return
}









//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.

  px.mu.Lock()
  defer px.mu.Unlock()

  me := px.peers[px.me]
  px.maxPeerDones[me] = int(Max(seq, px.maxPeerDones[me]))
  px.ForgetMin()

  return
}






//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.

  // px.iLock.Lock()
  // defer px.iLock.Unlock()

  max := -1
  for i := range(px.instances){
    max = Max(max, i)
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
  // You code here.

  min := px.maxPeerDones[px.peers[px.me]] // can set this to MAXINT
  for _, m := range(px.maxPeerDones){
    min = Min(min, m)
  }
  return min + 1
}






func (px *Paxos) ForgetMin() {

  min := px.Min()

  for i := range(px.instances) {
    if i < min {
      delete(px.instances, i)
    }
  }

  return
}







//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.

  // don't grab lock because this is called by other 
  // functions who already have the lock


  i, ok := px.instances[seq]
  if ok {
    return i.agreed, i.val
  }

  return false, nil
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


  // Your initialization code here.
  px.instances = map[int]Instance{}
  px.maxPeerDones = map[string]int{}
  for _, p := range (px.peers){
    px.maxPeerDones[p] = -1
  }



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
