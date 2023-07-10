package kvraft

import (
	"hash/fnv"
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const SHOW_BIT = 100000

var debugStart time.Time

type Err string

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func KVHash(Key string, Value string) uint32 {
	return hash(Key + Value)
}

func VHash(Value string) uint32 {
	return hash(Value)
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID  int64
	FID int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ID  int64
}

type GetReply struct {
	Err   Err
	Value string
}
