package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"sync"
	"time"
)
import Rn "math/rand"
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
	Id := nrand()
	DPrintf("[C%d]Get Id=%v key=%s", ck.me, Id%SHOW_BIT, key)
	resV := NOP_String
	var resCnt []int
	var callMap []int
	for i := 0; i < len(ck.servers); i++ { // init
		resCnt = append(resCnt, 0)
		callMap = append(callMap, 0)
	}
	var smu sync.Mutex
	for {
		smu.Lock()
		st := ck.lastLeader
		smu.Unlock()
		for i := 0; i < len(ck.servers); i++ {
			server := (i + st) % len(ck.servers) // got target server
			//DPrintf("[C%d]i=%d Get S=%d Id=%v", ck.me, i, server, Id%SHOW_BIT)
			args := GetArgs{
				Key: key,
				ID:  Id,
			}
			reply := GetReply{}
			if callMap[server] == 1 { // last call not finished
				continue
			}
			go func() { // start a new call
				callMap[server] = 1
				//DPrintf("[C%d] Get Sent Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
				ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
				callMap[server] = 0
				if ok {
					if reply.Err == ErrFailAgree { // leader or follower

					}
					if reply.Err == OK { // only from leader
						smu.Lock()
						ck.lastLeader = server
						resCnt[server] = 1
						resV = reply.Value
						smu.Unlock()
					}
					if reply.Err == ErrNoKey { // only from leader
						smu.Lock()
						ck.lastLeader = server
						resCnt[server] = 1
						resV = ""
						smu.Unlock()
					}
					if reply.Err == ErrWrongLeader { // only from follower
						smu.Lock()
						resCnt[server] = 1
						smu.Unlock()
					}
				}
			}()
		}
		smu.Lock()
		cur := 0
		for i := 0; i < len(ck.servers); i++ {
			if resCnt[i] == 1 {
				cur++
			}
		}
		if resV != NOP_String && (cur*2) >= len(ck.servers) {
			smu.Unlock()
			DPrintf("[C%d] Get Success Id=%v K=%s v=%s", ck.me, Id%SHOW_BIT, key, resV)
			return resV
		}
		smu.Unlock()
		ms := 10 + Rn.Int()%10
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	DPrintf("[C%d]PA Id=%v key=%s vh=%v op=%s", ck.me, Id%SHOW_BIT, key, value, op)
	var resCnt []int
	var callMap []int
	for i := 0; i < len(ck.servers); i++ { // init
		resCnt = append(resCnt, 0)
		callMap = append(callMap, 0)
	}
	var smu sync.Mutex
	for {
		smu.Lock()
		st := ck.lastLeader
		smu.Unlock()
		for i := 0; i < len(ck.servers); i++ {
			server := (i + st) % len(ck.servers) // got target server
			args := PutAppendArgs{
				Key:   key,
				Value: value,
				Op:    op,
				ID:    Id,
			}
			reply := PutAppendReply{}
			if callMap[server] == 1 { // last call not finished
				continue
			}
			go func() { // start a new call
				callMap[server] = 1
				//DPrintf("[C%d] Put/Append Sent Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
				ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
				callMap[server] = 0
				if ok {
					if reply.Err == OK { // ok append from leader
						smu.Lock()
						//DPrintf("[C%d] Put/Append OK Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
						ck.lastLeader = server
						resCnt[server] = 1
						smu.Unlock()
					}
					if reply.Err == ErrWrongLeader { // ok append from follower
						smu.Lock()
						//DPrintf("[C%d] Put/Append WLD Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
						resCnt[server] = 1
						smu.Unlock()
					}
					if reply.Err == ErrFailAgree { // fail append from leader/follower
						//DPrintf("[C%d] Put/Append fail agree Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
					}
				}
			}()
		}
		smu.Lock()
		cur := 0
		for i := 0; i < len(ck.servers); i++ {
			if resCnt[i] == 1 {
				cur++
			}
		}
		smu.Unlock()
		if (cur * 2) >= len(ck.servers) {
			DPrintf("[C%d] Put/Append Success Id=%v op=%s K=%s v=%s", ck.me, Id%SHOW_BIT, op, key, value)
			return
		}
		ms := 10 + Rn.Int()%10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
