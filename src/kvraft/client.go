package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
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
func (ck *Clerk) Get(key string) string {
	Id := nrand()
	DPrintf("Get Id=%v key=%s", Id%SHOW_BIT, key)
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (i + ck.lastLeader) % len(ck.servers) // got target server
			for {
				args := GetArgs{
					Key: key,
					ID:  Id,
				}
				reply := GetReply{}
				ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
				if ok {
					if reply.Err == ErrNoKey { // response ""
						DPrintf("	Get Id=%v key=%s NotExist!", Id%SHOW_BIT, key)
						ck.lastLeader = server
						return ""
					}
					if reply.Err == ErrWrongLeader { // switch
						DPrintf("	Get Id=%v key=%s S%d not leader!", Id%SHOW_BIT, key, server)
						break
					}
					if reply.Err == OK { // response correctly
						DPrintf("	Get Id=%v key=%s vh=%v", Id%SHOW_BIT, key, VHash(reply.Value))
						ck.lastLeader = server
						return reply.Value
					}
				} else { // retry
					DPrintf("	Get Id=%v S=%d Key=%s Error, retry", Id%SHOW_BIT,
						server, key)
				}
			}
		}
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

	DPrintf("PA Id=%v key=%s vh=%v op=%s", Id, key, KVHash(key, value), op)
	for { // handler leader change
		for i := 0; i < len(ck.servers); i++ { // handle wrong leader
			server := (i + ck.lastLeader) % len(ck.servers) // got target server
			for {                                           // handle network error
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
						DPrintf("	PA Id=%v UnexpectedResponse", Id) // retry
					}
					if reply.Err == ErrWrongLeader { // switch leader
						DPrintf("	PA Id=%v S%d not leader!", Id, server)
						break
					}
					if reply.Err == OK {
						DPrintf("	PA Id=%v Success", Id)
						ck.lastLeader = server
						return
					}
				} else {
					DPrintf("	PA Id=%v S=%d Error, retry", Id, server)
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
