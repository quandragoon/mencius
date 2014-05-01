
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.

  servers map[string]time.Time
  currentView View
  serverView View
  primaryLastView int
  primaryAcked bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.servers[args.Me] = time.Now()

  // Debug printing
  // log.Printf("Pinged by server %s and view number %d\n", args.Me, args.Viewnum)
  // log.Printf("Current view:\nViewnum: %d\nPrimary: %s\nBackup: %s\n", vs.currentView.Viewnum, vs.currentView.Primary, vs.currentView.Backup)
  // log.Printf("View for servers:\nViewnum: %d\nPrimary: %s\nBackup: %s\n", vs.serverView.Viewnum, vs.serverView.Primary, vs.serverView.Backup)

  if vs.currentView.Primary == "" {
    vs.currentView.Primary = args.Me
    vs.currentView.Viewnum++
    vs.serverView = vs.currentView
  } else {
    if vs.serverView.Primary != args.Me && vs.serverView.Backup == "" {
      vs.currentView.Backup = args.Me
      vs.currentView.Viewnum++
      if vs.primaryAcked {
        vs.serverView = vs.currentView
        vs.primaryAcked = false
        //log.Printf("New backup, primary hasn't acked yet\n")
      }
    } else if vs.serverView.Primary == args.Me {
      if args.Viewnum == vs.serverView.Viewnum {
        vs.primaryAcked = true
        vs.serverView = vs.currentView // Now you can update to next view
        //log.Printf("Primary acknowledged view number %d\n", vs.currentView.Viewnum)        
      } else if args.Viewnum == 0 { // Crashed
        //log.Printf("Primary server crashed\n")
        if vs.currentView.Backup != "" {
          newPrimary := vs.currentView.Backup
          vs.currentView.Primary = newPrimary
          vs.currentView.Backup = ""
          for k, v := range vs.servers {
            if k != newPrimary && k != args.Me && time.Now().Sub(v) < DeadPings * PingInterval {
              vs.currentView.Backup = k
            }
          }
          vs.currentView.Viewnum++
          if vs.primaryAcked {
            vs.serverView = vs.currentView
            vs.primaryAcked = false
          }
        }
      }
    } else if vs.serverView.Backup == args.Me && args.Viewnum == 0 {
      // Backup crashed
      vs.currentView.Viewnum++
      oldBackup := vs.currentView.Backup
      for k, v := range vs.servers {
        if k != vs.currentView.Primary && k != oldBackup && time.Now().Sub(v) < DeadPings * PingInterval {
          vs.currentView.Backup = k
          break
        }
      }
      if vs.primaryAcked {
        vs.serverView = vs.currentView
        vs.primaryAcked = false
      }
    }
  }
  reply.View = vs.serverView
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = vs.serverView
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
// promote the backup if the viewservice has missed DeadPings pings from the primary
func (vs *ViewServer) tick() {
  if time.Now().Sub(vs.servers[vs.currentView.Primary]) >= DeadPings * PingInterval {
    // log.Printf("Primary died, moving backup to primary\n")
    // Primary is dead
    if vs.currentView.Backup != "" {
      vs.currentView.Viewnum++
      oldPrimary := vs.currentView.Primary
      newPrimary := vs.currentView.Backup
      vs.currentView.Primary = vs.currentView.Backup
      vs.currentView.Backup = ""

      // Scan for new backup, if any
      for k, v := range vs.servers {
        if k != oldPrimary && k != newPrimary && time.Now().Sub(v) < DeadPings * PingInterval {
          vs.currentView.Backup = k
          break
        }
      }
      if vs.primaryAcked {
        vs.serverView = vs.currentView
        vs.primaryAcked = false
      }
    }
    // log.Printf("Primary is now %s\n", vs.currentView.Primary)
  } else if vs.currentView.Backup != "" && time.Now().Sub(vs.servers[vs.currentView.Backup]) >= DeadPings * PingInterval {
    vs.currentView.Viewnum++
    oldBackup := vs.currentView.Backup
    for k, v := range vs.servers {
      if k != vs.currentView.Primary && k != oldBackup && time.Now().Sub(v) < DeadPings * PingInterval {
        vs.currentView.Backup = k
        break
      }
    }
    if vs.primaryAcked {
      vs.serverView = vs.currentView
      vs.primaryAcked = false
    }
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me

  // Your vs.* initializations here.
  vs.servers = make(map[string]time.Time)
  vs.currentView = View{0, "", ""}
  vs.serverView = View{0, "", ""}

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
