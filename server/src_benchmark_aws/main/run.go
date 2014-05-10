package main

import "net/rpc"
import "shardmaster"
import "fmt"

func main() {
  var smh[]string = make([]string, 1)
  smh[0] = "127.0.0.1:8080"
  var ha[]string = make([]string, 1)
  ha[0] = "a"
  mck := shardmaster.MakeClerk(smh)
  fmt.Printf("Made clerk, %s %s\n", smh, mck)
  args := &shardmaster.JoinArgs{}
  var reply shardmaster.JoinReply
  c, errx := rpc.Dial("tcp", smh[0])
  if errx != nil {
    fmt.Println(errx)
  } else {
  defer c.Close()
    
  err := c.Call("ShardMaster.Join", args, reply)
  fmt.Println(err)
  }

  
}