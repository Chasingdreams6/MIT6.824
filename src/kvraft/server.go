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
	Kind  string // Read/Write
}

const READ = "read"
const WRITE = "write"

type KVServer struct {
	mu      sync.Mutex
	pamu    sync.Mutex
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
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	cmd := Op{}
	cmd.ID = args.ID
	cmd.Key = args.Key
	cmd.Kind = READ

	index, _, ok := kv.rf.Start(cmd)
	if ok {
		startTime := time.Now()
		for time.Now().Sub(startTime) < 2*time.Second { // waiting for agreement
			if kv.DuplicatedMap[cmd.ID] { // success to read
				reply.Err = OK
				reply.Value, ok = kv.Database[cmd.Key]
				if ok {
					DPrintf("[S%d] success to read id=%d k=%s vh=%v", kv.me, cmd.ID%SHOW_BIT, cmd.Key, reply.Value)
					reply.Err = OK
				} else {
					reply.Value = ""
					reply.Err = ErrNoKey
					DPrintf("[S%d] read no key id=%d k=%s vh=%v", kv.me, cmd.ID%SHOW_BIT, cmd.Key, reply.Value)
				}
				return
			}
			kv.mu.Unlock()
			ms := 30 + rand.Int()%40
			time.Sleep(time.Duration(ms) * time.Millisecond)
			kv.mu.Lock()
		}
		// not agreement..
		DPrintf("[FATAL]: ID:%v key=%s at Index:%d Not Agree",
			args.ID%SHOW_BIT, args.Key, index)
		reply.Err = ErrWrongLeader // TODO what to do here?
		return
	} else {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.pamu.Lock()
	defer kv.pamu.Unlock()
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
		cmd.Kind = WRITE
	}

	if args.Op == "Append" { // append = get + put

		cmd1 := Op{}
		cmd1.ID = nrand()
		cmd1.Key = args.Key
		cmd1.Kind = READ

		var v string
		index, _, ok := kv.rf.Start(cmd1)
		if ok {
			DPrintf("[S%d] Append, first read Id=%v key=%s", kv.me, cmd1.ID%SHOW_BIT, cmd1.Key)
			startTime := time.Now()
			for time.Now().Sub(startTime) < 2*time.Second { // waiting for agreement
				if kv.DuplicatedMap[cmd1.ID] { // success to read
					v = kv.Database[args.Key]
					DPrintf("[S%d] Append, got v=%v skip to append", kv.me, v)
					goto nextLabel
				}
				kv.mu.Unlock()
				ms := 30 + rand.Int()%40
				time.Sleep(time.Duration(ms) * time.Millisecond)
				kv.mu.Lock()
			}
			// not agreement..
			DPrintf("[FATAL]: ID:%v key=%s at Index:%d Not Agree",
				args.ID%SHOW_BIT, args.Key, index)
			reply.Err = ErrWrongLeader // TODO what to do here?
			return
		} else {
			reply.Err = ErrWrongLeader
			return
		}
	nextLabel:
		v = v + args.Value
		cmd.Key = args.Key
		cmd.ID = args.ID
		cmd.Value = v
		cmd.Kind = WRITE
	}

	index, _, ok := kv.rf.Start(cmd)
	if ok { // wait for agreement
		startTime := time.Now()
		for time.Now().Sub(startTime) < 2*time.Second { // waiting for agreement
			if kv.DuplicatedMap[cmd.ID] { // success to achieve agreement
				reply.Err = OK
				DPrintf("[S%d] success to put/append id=%d k=%s vh=%v", kv.me, cmd.ID%SHOW_BIT, cmd.Key, cmd.Value)
				return
			}
			kv.mu.Unlock()
			ms := 30 + rand.Int()%40
			time.Sleep(time.Duration(ms) * time.Millisecond)
			kv.mu.Lock()
		}
		// not agreement..
		DPrintf("[FATAL]: ID:%v key=%s v=%v at Index:%d Not Agree",
			args.ID%SHOW_BIT, args.Key, VHash(args.Value), index)
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

// background go-routine for handle applyMSg
func (kv *KVServer) applier(applyChan chan raft.ApplyMsg) {
	for msg := range applyChan {
		if msg.CommandValid {
			kv.mu.Lock()
			AppliedCmd := msg.Command.(Op)
			if AppliedCmd.Kind == WRITE {
				if kv.DuplicatedMap[AppliedCmd.ID] == false { // done
					kv.DuplicatedMap[AppliedCmd.ID] = true
					kv.Database[AppliedCmd.Key] = AppliedCmd.Value
					DPrintf("S[%d] Applier Success to PA ID=%v key=%s v=%v", kv.me,
						AppliedCmd.ID%SHOW_BIT, AppliedCmd.Key, AppliedCmd.Value)
				} else { // done before
					DPrintf("S[%d] Applier Duplicated PA ID=%v key=%s v=%v", kv.me,
						AppliedCmd.ID%SHOW_BIT, AppliedCmd.Key, AppliedCmd.Value)
				}
			} else {
				if kv.DuplicatedMap[AppliedCmd.ID] == false {
					kv.DuplicatedMap[AppliedCmd.ID] = true
					DPrintf("S[%d] Applier Read Id=%v key=%s", kv.me, AppliedCmd.ID%SHOW_BIT, AppliedCmd.Key)
				} else {
					DPrintf("S[%d] Applier Duplicated Read Id=%v key=%s", kv.me, AppliedCmd.ID%SHOW_BIT, AppliedCmd.Key)
				}
			}
			kv.mu.Unlock()
		}
	}
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
	go kv.applier(kv.applyCh)
	time.Sleep(10 * time.Millisecond)
	return kv
}
