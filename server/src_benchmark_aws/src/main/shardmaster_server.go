package main

import "shardmaster"
import "net"
import "fmt"
import "log"
import "syscall"
import "math/rand"
import "flag"
import "strconv"


func main() {
  var port int
  flag.IntVar(&port, "portindex", 0, "port for server")
  flag.Parse()
  var smh[]string = make([]string, 3)
  const IP = "0.0.0.0:"
  const nmasters = 3
  for i := 0; i < nmasters; i++ {
    smh[i] = IP + "808" + strconv.Itoa(i)
  }
  sm, rpcs := shardmaster.SetupServer(smh, port)
  listener, e := net.Listen("tcp", smh[port]);
  if e != nil {
    log.Fatal("shardmaster listen error: ", e);
  }
  fmt.Printf("START SERVER: port %d\n", port)

  // please do not change any of the following code,
  // or do anything to subvert it.

  for sm.IsDead() == false {
    conn, err := listener.Accept()
    if err == nil && sm.IsDead() == false {
      if sm.IsUnreliable() && (rand.Int63() % 1000) < 100 {
        // discard the request.
        conn.Close()
      } else if sm.IsUnreliable() && (rand.Int63() % 1000) < 200 {
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
    if err != nil && sm.IsDead() == false {
      fmt.Printf("ShardMaster(%v) accept: %v\n", 0, err.Error())
      sm.Kill()
    }
  }}