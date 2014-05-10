package main


import "shardmaster"


func main() {
  var smh[]string = make([]string, 1)
  smh[0] = ":8080"
  shardmaster.StartServer(smh, 0)
}