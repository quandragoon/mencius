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
import "math"

const (
  OK = "OK"
  Reject = "Reject"
  OKNOVALUE = "OKNOVALUE"
)

type Reply string

type AcceptArgs struct {
  Seq int
  N int64
  Value interface{}
  PeerID int
  DoneSeq int
}

type AcceptReply struct {
  Reply Reply
  Seq int
}

type DecidedArgs struct {
  Seq int
  Value interface{}
  PeerID int
  DoneSeq int
}

type DecidedReply struct {
  Reply Reply
  PeerID int
  DoneSeq int
}

type PrepareArgs struct {
  Seq int
  N int64
  Value interface{}
  PeerID int
  DoneSeq int
}

type PrepareReply struct {
  Reply Reply
  Seq int
  N_a int64
  Value_a interface{}
}

type Agreement struct {
  seq int
  n_p int64
  n_a int64
  v_a interface{} //??
  v_final interface{}
  decided bool
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
  agreements map[int]*Agreement
  peersDone []int
  curSeq int
  highestDone int
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

// Loops through the peers are sends Prepare statements, waiting for reply
func (px *Paxos) ProposePrepare(seq int, value interface{}) (int, int64, interface{}) {
  t := time.Now().UnixNano()
  n := t * int64(len(px.peers)) + int64(px.me)
  replied := 0
  v := value
  n_highest := int64(0)
  for i := 0; i < len(px.peers); i++ {
    prepareArgs := &PrepareArgs{seq, n, value, px.me, px.highestDone}
    var prepareReply PrepareReply
    if i != px.me {
      call(px.peers[i], "Paxos.Prepare", prepareArgs, &prepareReply)
    } else {
      px.Prepare(prepareArgs, &prepareReply)
    }
    if prepareReply.Reply == OK {
      replied++
      if prepareReply.N_a > n_highest {
        n_highest = prepareReply.N_a
        v = prepareReply.Value_a
      }
    } else if prepareReply.Reply == OKNOVALUE {
      replied++
    }
  }
  return replied, n, v
}

func (px *Paxos) ProposeAccept(seq int, n int64, value interface{}) int {
  replied := 0
  for i := 0; i < len(px.peers); i++ {
    acceptArgs := &AcceptArgs{seq, n, value, px.me, px.highestDone}
    var acceptReply AcceptReply
    if i != px.me {
      call(px.peers[i], "Paxos.Accept", acceptArgs, &acceptReply)
    } else {
      px.Accept(acceptArgs, &acceptReply)
    }
    if acceptReply.Reply == OK {
      replied++
    }
  }
  return replied
}

func (px *Paxos) Proposer(seq int, value interface{}) error {
  px.mu.Lock()
  decided := px.agreements[seq] == nil || px.agreements[seq].decided
  px.mu.Unlock()
  for !decided && px.dead == false {
    replied, n, v := px.ProposePrepare(seq, value)
    if replied > len(px.peers) / 2 {
      replied = px.ProposeAccept(seq, n, v)

      if replied > len(px.peers) / 2 {
        decideArgs := &DecidedArgs{seq, v, px.me, px.highestDone}
        for i := 0; i < len(px.peers); i++ {
          var decideReply DecidedReply
          if i != px.me {
            call(px.peers[i], "Paxos.Decide", decideArgs, &decideReply)
          } else {
            px.Decide(decideArgs, &decideReply)
          }
          if px.peersDone[decideReply.PeerID] < decideReply.DoneSeq {
            px.peersDone[decideReply.PeerID] = decideReply.DoneSeq
          }
        }
      }
    } else {
      time.Sleep(500 * time.Millisecond)
    }
    px.mu.Lock()
    decided = px.agreements[seq] == nil || px.agreements[seq].decided
    px.mu.Unlock()
  }

  px.Done(px.highestDone)
  return nil
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()

  if _, ok := px.agreements[args.Seq]; !ok || px.agreements[args.Seq] == nil {
    px.agreements[args.Seq] = &Agreement{seq: args.Seq}
  }

  if args.N > px.agreements[args.Seq].n_p {
    px.agreements[args.Seq].n_p = args.N
    reply.Reply = OK

    if px.agreements[args.Seq].n_a == 0 {
      reply.Value_a = nil
      reply.N_a = 0
      reply.Reply = OKNOVALUE
    } else {
      reply.Value_a = px.agreements[args.Seq].v_a
      reply.N_a = px.agreements[args.Seq].n_a
    }
    
    
  } else {
    reply.Reply = Reject
  }
  px.mu.Unlock()
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()

  if px.agreements[args.Seq] != nil {
    if args.N >= px.agreements[args.Seq].n_p {
      px.agreements[args.Seq].n_p = args.N
      px.agreements[args.Seq].n_a = args.N
      px.agreements[args.Seq].v_a = args.Value
      reply.Reply = OK
    } else {
      reply.Reply = Reject
    }
  }
  px.mu.Unlock()
  return nil
}

func (px *Paxos) Decide(args *DecidedArgs, reply *DecidedReply) error {
  px.mu.Lock()
  if px.agreements[args.Seq] != nil && !px.agreements[args.Seq].decided{
    px.agreements[args.Seq].v_final = args.Value
    px.agreements[args.Seq].decided = true
    reply.Reply = OK
    if px.peersDone[args.PeerID] < args.DoneSeq {
      px.peersDone[args.PeerID] = args.DoneSeq
    }
    reply.PeerID = px.me
    reply.DoneSeq = px.highestDone
    if args.Seq > px.curSeq {
      px.curSeq = args.Seq
    }
  }
  px.mu.Unlock()
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
  if seq >= px.Min() {
    _, ok := px.agreements[seq]
    if !ok || px.agreements[seq] == nil{
      if seq > px.curSeq {
        px.curSeq = seq
      }

      px.mu.Lock()
      px.agreements[seq] = &Agreement{seq: seq}
      px.mu.Unlock()
      if px.dead == false {
        go px.Proposer(seq, v)
      }
    } else {
      if !px.agreements[seq].decided && px.dead == false{
        go px.Proposer(seq, v)
      }
    }
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
  if seq > px.highestDone {
    px.highestDone = seq
  }

  px.mu.Lock()
  min := px.Min()
  for seq, _ := range px.agreements {
    if seq < min {
      px.agreements[seq] = nil
    }
  }
  px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  return px.curSeq
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
  min := math.MaxInt32
  for i := 0; i < len(px.peersDone); i++ {
    if px.peersDone[i] < min {
      min = px.peersDone[i]
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
  if seq >= px.Min() {
    if agreement, ok := px.agreements[seq]; ok && agreement != nil{
      if px.agreements[seq].decided {
        return true, px.agreements[seq].v_final
      }
    }
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
  px.agreements = make(map[int]*Agreement)
  px.curSeq = -1
  px.highestDone = -1
  px.peersDone = make([]int, len(px.peers))
  for i := 0; i < len(px.peersDone); i++ {
    px.peersDone[i] = -1
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
      log.Fatal("paxos listen error: ", e);
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
