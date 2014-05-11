package main

import "shardmaster"

func main() {
  var smh[]string = make([]string, 1)
  smh[0] = "127.0.0.1:8080"
  var ha[]string = make([]string, 1)
  ha[0] = "a"
  mck := shardmaster.MakeClerk(smh)
  mck.Join(101, ha)
}