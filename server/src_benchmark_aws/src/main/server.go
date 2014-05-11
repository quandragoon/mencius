package main

import "shardmaster"
import "net"
import "fmt"
import "log"
import "syscall"
import "math/rand"
import "flag"


func main() {
  var port string
  flag.StringVar(&port, "port", "8080", "port for server")
  flag.Parse()
  var smh[]string = make([]string, 1)
  smh[0] = ":" + port
  sm, rpcs := shardmaster.SetupServer(smh, 0)
  listener, e := net.Listen("tcp", smh[0]);
  if e != nil {
    log.Fatal("shardmaster listen error: ", e);
  }
  fmt.Println("START SERVER: port " + port)

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