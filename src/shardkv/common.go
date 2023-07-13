package shardkv

import (
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type CommandType string

const (
	GetCommand          CommandType = "Get"
	PutCommand          CommandType = "Put"
	AppendCommand       CommandType = "Append"
	UpdateConfigCommand CommandType = "Updtcmd"
)

const Debug = true
const SHOW_BIT = 1000

var debugStart time.Time

func DPrintf(format string, a ...interface{}) {

	if Debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d ", time)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}

func DeepCopyMap(db map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range db {
		res[k] = v
	}
	return res
}

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID int64 // for duplicate detection
}

type PutAppendReply struct {
	Err Err
	ID  int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID int64
}

type GetReply struct {
	Err   Err
	Value string
	ID    int64
}

type PullShardArgs struct {
	Gid   int // sender's gid
	Shard int // request shard number
}

type PullShardReply struct {
	Err      Err
	Database map[string]string
}
