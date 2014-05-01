package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.

  view viewservice.View
  keyvalues map[string]string
  putRequests map[int64]DuplicatePut
  getRequests map[int64]DuplicateGet
  mu sync.Mutex
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  //DPrintf("Attempting put for [%s, %s]\n", args.Key, args.Value)
  pb.mu.Lock()
  if pb.me == pb.view.Primary {
    // DPrintf("Client %d for [%s, %s]\n", args.ClientID, args.Key, args.Value)
    // DPrintf("Map before put for primary %s:\n", pb.me)
    // for k, v := range pb.keyvalues {
    //   DPrintf("%s -> %s\n", k, v)
    // }
    //DPrintf("Primary %s Grabbed lock for [%s, %s]\n", pb.me, args.Key, args.Value)
    duplicate, ok := pb.putRequests[args.ClientID]
    if (ok && duplicate.RequestTime.Before(args.RequestTime)) || !ok {
      var newval string
      if args.DoHash {
        val, ok := pb.keyvalues[args.Key]
        if !ok {
          val = ""
        }
        reply.PreviousValue = val
        newval = strconv.Itoa(int(hash(val + args.Value)))
        //DPrintf("New hash value %s\n", newval)
        //DPrintf("Previous hash value %s\n", reply.PreviousValue)
      } else {
        newval = args.Value
      }

      duplicate = DuplicatePut{args.RequestTime, reply.PreviousValue}

      if pb.view.Backup != "" {
        updateArgs := &UpdateArgs{args.Key, newval, pb.me, args.ClientID, duplicate}
        var updateReply UpdateReply
        call(pb.view.Backup, "PBServer.Update", updateArgs, &updateReply)

        if updateReply.Err == OK {
          pb.keyvalues[args.Key] = newval
          pb.putRequests[args.ClientID] = duplicate
          reply.Err = OK
        } else {
          reply.Err = ErrWrongServer
        }
      } else {
        pb.keyvalues[args.Key] = newval
        pb.putRequests[args.ClientID] = duplicate
        reply.Err = OK
      }

      //DPrintf("Committed hash value\n")

      // DPrintf("Map after put:\n")
      // for k, v := range pb.keyvalues {
      //   DPrintf("%s -> %s\n", k, v)
      // }
      
    } else {
      //DPrintf("You already tried that!\n")
      reply.Err = OK
      reply.PreviousValue = duplicate.PreviousValue
      //DPrintf("Previous value: %s\n", reply.PreviousValue)
    }
  } else {
    reply.Err = ErrWrongServer
    if pb.me == pb.view.Backup {
      // DPrintf("Map before put for backup %s:\n", pb.me)
      // for k, v := range pb.keyvalues {
      //   DPrintf("%s -> %s\n", k, v)
      // }
    }
  }
  pb.mu.Unlock()
  //DPrintf("Released lock\n")
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  if pb.me == pb.view.Primary || pb.me == pb.view.Backup {
    duplicate, ok := pb.getRequests[args.ClientID]
    if (ok && duplicate.RequestTime.Before(args.RequestTime)) || !ok {
      if val, keyok := pb.keyvalues[args.Key]; keyok {
        reply.Value = val
        reply.Err = OK
      } else {
        reply.Value = ""
        reply.Err = ErrNoKey
      }
      pb.getRequests[args.ClientID] = DuplicateGet{args.RequestTime, reply.Value}
      // Forward to backup as well
    } else {
      reply.Err = OK
      reply.Value = duplicate.Value
    }
  } else {
    reply.Err = ErrWrongServer
  } 
  pb.mu.Unlock()
  return nil
}

func (pb *PBServer) Update(args *UpdateArgs, reply *UpdateReply) error {
  if args.Primary == pb.view.Primary && pb.me == pb.view.Backup {
    pb.keyvalues[args.Key] = args.Value
    pb.putRequests[args.ClientID] = args.Request
    reply.Err = OK
    // DPrintf("Map after put for backup %s:\n", pb.me)
    // for k, v := range pb.keyvalues {
    //   DPrintf("%s -> %s\n", k, v)
    // }
  } else {
    reply.Err = ErrWrongServer
  }
  return nil
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
  // Check if actually was primary...? Or doesn't matter
  // DPrintf("Map before forwarding:\n")
  // for k, v := range pb.keyvalues {
  //   DPrintf("%s -> %s\n", k, v)
  // }
  pb.mu.Lock()
  pb.keyvalues = args.KeyValues
  pb.putRequests = args.Requests
  // DPrintf("Map after forwarding:\n")
  // for k, v := range pb.keyvalues {
  //   DPrintf("%s -> %s\n", k, v)
  // }
  reply.Err = OK
  pb.mu.Unlock()
  return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  newview, _ := pb.vs.Ping(pb.view.Viewnum)
  if pb.me == newview.Primary && pb.view.Backup != newview.Backup {
    // This server is the primary and it just detected a backup
    args := &ForwardArgs{pb.keyvalues, pb.putRequests}
    var reply ForwardReply
    call(newview.Backup, "PBServer.Forward", args, &reply)
    // Check if ok...?
  }
  pb.view = newview
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.

  pb.view = viewservice.View{0, "", ""}
  pb.keyvalues = make(map[string]string)
  pb.putRequests = make(map[int64]DuplicatePut)
  pb.getRequests = make(map[int64]DuplicateGet)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
