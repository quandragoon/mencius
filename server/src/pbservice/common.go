package pbservice

import "hash/fnv"
import "time"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
  ErrDuplicate = "ErrDuplicate"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.

  ClientID int64
  RequestTime time.Time

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
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

// Your RPC definitions here.
type DuplicatePut struct {
  RequestTime time.Time
  PreviousValue string
}

type DuplicateGet struct {
  RequestTime time.Time
  Value string
}

type UpdateArgs struct {
  Key string
  Value string
  Primary string
  ClientID int64
  Request DuplicatePut
}

type UpdateReply struct {
  Err Err
}

type ForwardArgs struct {
  KeyValues map[string]string
  Requests map[int64]DuplicatePut
}

type ForwardReply struct {
  Err Err
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

