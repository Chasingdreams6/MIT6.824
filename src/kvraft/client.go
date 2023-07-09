package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	me         int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0 // start from 0
	ck.me = int(nrand() % 100)
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
	Id := nrand()
	DPrintf("[C%d]Get Id=%v key=%s", ck.me, Id%SHOW_BIT, key)
	st := ck.lastLeader
	for {
		//res := make(map[string]int)
		for i := 0; i < len(ck.servers); i++ {
			server := (i + st) % len(ck.servers) // got target server
			DPrintf("[C%d]Get S=%d", ck.me, server)
			args := GetArgs{
				Key: key,
				ID:  Id,
			}
			reply := GetReply{}
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == ErrWrongLeader { // continue
					continue
				}
				if reply.Err == OK || reply.Err == ErrNoKey {
					ck.lastLeader = server
					DPrintf("	[C%d]Get Id=%v key=%s vh=%v", ck.me, Id%SHOW_BIT, key, VHash(reply.Value))
					return reply.Value
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
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
	Id := nrand()

	DPrintf("[C%d]PA Id=%v key=%s vh=%v op=%s", ck.me, Id%SHOW_BIT, key, VHash(value), op)
	for { // handler leader change
		st := ck.lastLeader
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
				if reply.Err == ErrNoKey { // TODO may not happen?
					DPrintf("	[C%d]PA Id=%v UnexpectedResponse", ck.me, Id%SHOW_BIT) // retry
				}
				if reply.Err == ErrWrongLeader { // switch leader
					//DPrintf("	[C%d]PA Id=%v S%d not leader!", ck.me, Id%SHOW_BIT, server)
					continue
				}
				if reply.Err == OK {
					DPrintf("	[C%d]PA Id=%v Success", ck.me, Id%SHOW_BIT)
					ck.lastLeader = server
					return
				}
			} else {
				//DPrintf("	[C%d]PA Id=%v S=%d Error, retry", ck.me, Id%SHOW_BIT, server)
				continue
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
