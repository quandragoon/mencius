package shardkv
import "hash/fnv"
import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  ClientID int64
  RequestTime time.Time

}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  ClientID int64
  RequestTime time.Time
}

type GetReply struct {
  Err Err
  Value string
}

type UpdateArgs struct {
  Num int
  Shard int
  Group int64
  Server int
}

type UpdateReply struct {
  KeyValues map[string]RequestValue
  ClientPut map[int64]DuplicatePut
  ClientGet map[int64]DuplicateGet
  Err Err
}

type DuplicatePut struct {
  RequestTime time.Time
  PreviousValue string
}

type DuplicateGet struct {
  RequestTime time.Time
  Value string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

