package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	me         int
	mu         sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {

	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

	if debugStart.IsZero() {
		debugStart = time.Now()
	}
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0 // start from 0
	ck.me = int(nrand() % 100)
	time.Sleep(500 * time.Millisecond)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// use synchornous number
// TODO, what's the consensus?
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	Id := nrand()
	DPrintf("[C%d]Get Id=%v key=%s", ck.me, Id%SHOW_BIT, key)
	st := ck.lastLeader
	for {
		resV := ""
		resCnt := 0
		for i := 0; i < len(ck.servers); i++ {
			server := (i + st) % len(ck.servers) // got target server
			DPrintf("[C%d]i=%d Get S=%d Id=%v", ck.me, i, server, Id%SHOW_BIT)
			args := GetArgs{
				Key: key,
				ID:  Id,
			}
			reply := GetReply{}
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == ErrWrongLeader { // continue
					if reply.Value == "fail" {
						DPrintf("[C%d] S=%d Not leader, not synced, cnt=%d", ck.me, server, resCnt)
						continue
					}
					DPrintf("[C%d] S=%d Not leader, but synced, cnt=%d", ck.me, server, resCnt+1)
					resCnt++
					if resCnt*2 >= len(ck.servers) {
						DPrintf("	[C%d]success to Get Id=%v key=%s vh=%v", ck.me, Id%SHOW_BIT, key, resV)
						return resV
					}
				}
				if reply.Err == OK || reply.Err == ErrNoKey {
					DPrintf("[C%d] S=%d believe leader, v=%s, cnt=%d", ck.me, server, reply.Value, resCnt+1)
					ck.lastLeader = server
					resCnt++
					resV = reply.Value
					if (resCnt * 2) >= len(ck.servers) {
						DPrintf("	[C%d]success to Get Id=%v key=%s vh=%v", ck.me, Id%SHOW_BIT, key, reply.Value)
						return reply.Value
					}
				}
			} else {
				DPrintf("	[C%d]READ Id=%v S=%d Error, next", ck.me, Id%SHOW_BIT, server)
				continue
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	Id := nrand()
	DPrintf("[C%d]PA Id=%v key=%s vh=%v op=%s", ck.me, Id%SHOW_BIT, key, value, op)
	for { // handler leader change
		st := ck.lastLeader
		cntV := 0
		for i := 0; i < len(ck.servers); i++ { // handle wrong leader
			server := (i + st) % len(ck.servers) // got target server
			//DPrintf("[C%d]PA S=%d", ck.me, server)
			args := PutAppendArgs{
				Key:   key,
				Value: value,
				Op:    op,
				ID:    Id,
			}
			reply := PutAppendReply{}
			ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
			if ok {
				if reply.Err == ErrNoKey { // Special for no agree
					DPrintf("	[C%d]PA Id=%v S%d not leader, not syned", ck.me, Id%SHOW_BIT, server) // retry
					continue
				}
				if reply.Err == ErrWrongLeader { // switch leader
					DPrintf("	[C%d]PA Id=%v S%d not leader, but syned", ck.me, Id%SHOW_BIT, server)
					cntV++
					if cntV*2 >= len(ck.servers) {
						DPrintf("	[C%d]PA Id=%v Success", ck.me, Id%SHOW_BIT)
						return
					}
					continue
				}
				if reply.Err == OK {
					ck.lastLeader = server
					DPrintf("	[C%d]PA Id=%v S%d is leader, syned", ck.me, Id%SHOW_BIT, server)
					cntV++
					if cntV*2 >= len(ck.servers) {
						DPrintf("	[C%d]PA Id=%v Success", ck.me, Id%SHOW_BIT)
						return
					}
				}
			} else {
				DPrintf("	[C%d]PA Id=%v S=%d Error, next", ck.me, Id%SHOW_BIT, server)
				continue
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
