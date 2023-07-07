package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string // key
	Value string // value
	ID    int64  // ID is only mark
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Database      map[string]string // key->value
	DuplicatedMap map[int64]bool    // int64->bool

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if isLeader == false { // false, ret
		reply.Err = ErrWrongLeader
		return
	}
	v, ok := kv.Database[args.Key]
	if ok {
		reply.Err = OK
		reply.Value = v
		return
	} else {
		reply.Err = ErrNoKey
		reply.Value = ""
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if isLeader == false { // false, ret
		reply.Err = ErrWrongLeader
		return
	}
	v := kv.DuplicatedMap[args.ID]
	if v == true { // ok before, ignore
		reply.Err = OK
		return
	}
	cmd := Op{}
	if args.Op == "Put" { // put
		cmd.Key = args.Key
		cmd.ID = args.ID
		cmd.Value = args.Value
	}

	if args.Op == "Append" { // append
		v, ok := kv.Database[args.Key]
		if !ok {
			v = ""
		}
		v = v + args.Value
		cmd.Key = args.Key
		cmd.ID = args.ID
		cmd.Value = v
	}

	index, _, ok := kv.rf.Start(cmd)
	if ok { // wait for agreement
		startTime := time.Now()
		for time.Now().Sub(startTime) < 2*time.Second { // waiting for agreement
			msg, ok := <-kv.applyCh
			if ok {
				if msg.CommandValid {
					AppliedCmd := msg.Command.(Op)
					if kv.DuplicatedMap[AppliedCmd.ID] == false { // done
						kv.DuplicatedMap[AppliedCmd.ID] = true
						kv.Database[AppliedCmd.Key] = AppliedCmd.Value
						reply.Err = OK
						DPrintf("S%d Success to PA ID=%v key=%s v=%v", kv.me,
							cmd.ID, cmd.Key, KVHash(cmd.Key, cmd.Value))
						return
					} else { // done before
						reply.Err = OK
						DPrintf("S%d Duplicated PA ID=%v key=%s v=%v", kv.me,
							cmd.ID, cmd.Key, KVHash(cmd.Key, cmd.Value))
						return
					}
				}
			} else {
				ms := 100 + rand.Int()%100
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}
		// not agreement..
		DPrintf("[FATAL]: ID:%v key=%s v=%s at Index:%d Not Agree",
			args.ID, args.Key, args.Value, index)
		reply.Err = ErrWrongLeader // TODO what to do here?
		return
	} else {
		reply.Err = ErrWrongLeader
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Database = make(map[string]string)
	kv.DuplicatedMap = make(map[int64]bool)
	return kv
}
