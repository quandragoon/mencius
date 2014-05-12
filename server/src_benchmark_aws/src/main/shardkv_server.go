package main

import "shardkv"
import "net"
import "fmt"
import "log"
import "syscall"
import "math/rand"
import "flag"
import "strconv"


func main() {
  var port int
  var gid int
  flag.IntVar(&port, "portindex", 0, "port for server")
  flag.IntVar(&gid, "gidindex",0, "gid for server")
  flag.Parse()

  const ngroups = 3
  const nreplicas = 3
  const IP = "54.187.210.114:"

  IPS := make([]string, ngroups)
  IPS[0] = "54.187.166.1"
  IPS[1] = "54.187.139.234"
  IPS[2] = "54.187.203.101"

  ha := make([][]string, ngroups)
  gids := make([]int64, ngroups)
  for i := 0; i < ngroups; i++ {
    gids[i] = int64(i + 100)
    ha[i] = make([]string, nreplicas)
    for j := 0; j < nreplicas; j++ {
      if i == gid {
        ha[i][j] = "0.0.0.0:808" + strconv.Itoa(j)
      } else {
        ha[i][j] = IPS[i] + ":808" + strconv.Itoa(j)
      }
    }
  }
  var smh[]string = make([]string, 3)
  for i := 0; i < ngroups; i++ {
    smh[i] = IP + "808" + strconv.Itoa(i)
  }
  kv, rpcs := shardkv.SetupServer(gids[gid], smh, ha[gid], port)
  listener, e := net.Listen("tcp", ha[gid][port]);
  if e != nil {
    log.Fatal("shardkv listen error: ", e);
  }
  fmt.Printf("START SERVER: port %d\n", port)

  for kv.IsDead() == false {
    conn, err := listener.Accept()
    if err == nil && kv.IsDead() == false {
      if kv.IsUnreliable() && (rand.Int63() % 1000) < 100 {
        // discard the request.
        conn.Close()
      } else if kv.IsUnreliable() && (rand.Int63() % 1000) < 200 {
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
    if err != nil && kv.IsDead() == false {
      fmt.Printf("ShardKV(%v) accept: %v\n", port, err.Error())
      kv.Kill()
    }
  }
}