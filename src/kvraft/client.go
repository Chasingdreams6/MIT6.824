package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"fmt"
	"sort"
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
	labgob.Register(GcArgs{})
	labgob.Register(GcReply{})

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
	var resCnt []string
	var callMap []int
	for i := 0; i < len(ck.servers); i++ { // init
		resCnt = append(resCnt, NOP_String)
		callMap = append(callMap, 0)
	}

	var smu sync.Mutex
	var cmu sync.Mutex
	smu.Lock()
	st := ck.lastLeader
	smu.Unlock()
	for j := 0; ; j++ {

		for i := 0; i < len(ck.servers); i++ {
			server := (i + st) % len(ck.servers) // got target server
			//DPrintf("[C%d]i=%d Get S=%d Id=%v", ck.me, i, server, Id%SHOW_BIT)
			cmu.Lock()
			if callMap[server] == 1 { // last call not finished
				cmu.Unlock()
				continue
			}
			cmu.Unlock()
			go func(server int) { // start a new call
				cmu.Lock()
				callMap[server] = 1
				cmu.Unlock()
				args := GetArgs{
					Key: key,
					ID:  Id,
				}
				reply := GetReply{}
				//DPrintf("[C%d] Get Sent Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
				ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
				cmu.Lock()
				callMap[server] = 0
				cmu.Unlock()
				if ok {
					if reply.Err == ErrFailAgree { // leader or follower

					}
					if reply.Err == OK { // only from "leader"
						smu.Lock()
						//ck.lastLeader = server
						resCnt[server] = reply.Value
						smu.Unlock()
					}
					if reply.Err == ErrNoKey { // only from leader
						smu.Lock()
						//ck.lastLeader = server
						resCnt[server] = reply.Value
						smu.Unlock()
					}
					if reply.Err == ErrWrongLeader { // only from follower
						smu.Lock()
						resCnt[server] = reply.Value
						smu.Unlock()
					}
				}
			}(server)
		}

		smu.Lock()
		// debug
		str := ""
		for i := 0; i < len(ck.servers); i++ {
			str = str + fmt.Sprintf("S=%d V=%s ", i, resCnt[i])
			//DPrintf("S=%d V=%s", i, resCnt[i])
		}
		DPrintf(str)
		//
		cur := 0
		maxV := 0
		sort.Strings(resCnt)
		for i := 0; i < len(ck.servers); i++ {
			if i == 0 {
				cur = 1
				resV = resCnt[i]
				maxV = 1
				continue
			}
			if resCnt[i] == resCnt[i-1] {
				cur++
				if cur > maxV {
					maxV = cur
					resV = resCnt[i-1]
				}
			} else {
				if cur > maxV {
					maxV = cur
					resV = resCnt[i-1]
				}
				cur = 1
			}
		}
		if resV != NOP_String && (maxV*2) > len(ck.servers) {
			DPrintf("[C%d] Get Success Id=%v K=%s v=%s", ck.me, Id%SHOW_BIT, key, resV)
			//ck.GC(Id)
			// TODO, gc will affect correct?
			rt := resV
			smu.Unlock()
			return rt
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
	RId := nrand()
	var curv string
	if op == "Append" {
		v := ck.Get(key)
		curv = v + value
	}
	DPrintf("[C%d]PA Id=%v key=%s vh=%v op=%s", ck.me, Id%SHOW_BIT, key, curv, op)
	var resCnt []int
	var callMap []int
	for i := 0; i < len(ck.servers); i++ { // init
		resCnt = append(resCnt, 0)
		callMap = append(callMap, 0)
	}
	var smu sync.Mutex
	var cmu sync.Mutex
	smu.Lock()
	st := ck.lastLeader
	smu.Unlock()
	for j := 0; ; j++ {

		if op == "Append" && j%11 == 10 { // clear and retry
			v := ck.Get(key)
			curv = v + value
			for i := 0; i < len(ck.servers); i++ { // init
				smu.Lock()
				cmu.Lock()
				resCnt[i] = 0
				callMap[i] = 0
				smu.Unlock()
				cmu.Unlock()
			}
			DPrintf("[C%d]Append Retry Id=%v key=%s vh=%v op=%s", ck.me, Id%SHOW_BIT, key, curv, op)
		}

		for i := 0; i < len(ck.servers); i++ {
			server := (i + st) % len(ck.servers) // got target server
			cmu.Lock()
			if callMap[server] == 1 { // last call not finished
				cmu.Unlock()
				continue
			}
			cmu.Unlock()
			flag := true
			smu.Lock()
			if resCnt[server] == 1 {
				flag = false
			}
			smu.Unlock()
			if !flag { // skip
				continue
			}
			go func(server int) { // start a new call
				cmu.Lock()
				callMap[server] = 1
				cmu.Unlock()
				args := PutAppendArgs{
					Key:   key,
					Value: curv,
					Op:    op,
					ID:    Id,
					RID:   RId,
				}
				reply := PutAppendReply{}
				//DPrintf("[C%d] Put/Append Sent Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
				ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
				cmu.Lock()
				callMap[server] = 0
				cmu.Unlock()
				if ok {
					if reply.Err == OK { // ok append from leader
						smu.Lock()
						//DPrintf("[C%d] Put/Append OK Id=%v S=%d", ck.me, Id%SHOW_BIT, server)
						//ck.lastLeader = server
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
			}(server)
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
			DPrintf("[C%d] Put/Append Success Id=%v op=%s K=%s v=%s", ck.me, Id%SHOW_BIT, op, key, curv)
			//ck.GC(Id)
			//ck.GC(RId)
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

// return immeditately
func (ck *Clerk) GC(Id int64) {

	for i := 0; i < len(ck.servers); i++ {
		go func(it int) {
			args := GcArgs{
				Id: Id,
			}
			reply := GcReply{}
			_ = ck.servers[it].Call("KVServer.GCID", &args, &reply)
		}(i)
	}
}

// return immeditately
func (ck *Clerk) GCS(Id int64) {
	for i := 0; i < len(ck.servers); i++ {
		args := GcArgs{
			Id: Id,
		}
		reply := GcReply{}
		_ = ck.servers[i].Call("KVServer.GCID", &args, &reply)
	}
}
