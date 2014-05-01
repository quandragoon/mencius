package kvpaxos

import "hash/fnv"
import "time"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrNoAgreement = "ErrNoAgreement"
  PUT = "PUT"
  GET = "GET"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
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
