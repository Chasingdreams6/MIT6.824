package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

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
	mu           sync.Mutex
	pamu         sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Database      map[string]string // key->value
	DuplicatedMap map[int64]bool    // int64->bool
}

func (kv *KVServer) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	Dblen := len(kv.Database)
	e.Encode(Dblen)
	for K, V := range kv.Database {
		e.Encode(K)
		e.Encode(V)
	}
	Dulen := len(kv.DuplicatedMap)
	e.Encode(Dulen)
	for K, _ := range kv.DuplicatedMap {
		str := strconv.FormatInt(K, 10)
		e.Encode(str)
	}
	return w.Bytes()
}

func (kv *KVServer) ReadSnapshot(snapshot []byte) {

	//kv.Database = make(map[string]string)
	//kv.DuplicatedMap = make(map[int64]bool)
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var Dblen, Dulen int
	if d.Decode(&Dblen) != nil {
		DPrintf("[FATAL] DECODE error")
		return
	}
	var K, V string
	for i := 0; i < Dblen; i++ {
		if d.Decode(&K) != nil || d.Decode(&V) != nil {
			DPrintf("[FATAL] DECODE error")
			return
		}
		kv.Database[K] = V
		DPrintf("S[%d] ReadSnapshot k=%s v=%s", kv.me, K, V)
	}
	if d.Decode(&Dulen) != nil {
		DPrintf("[FATAL] DECODE error")
		return
	}
	var str string
	for i := 0; i < Dulen; i++ {
		if d.Decode(&str) != nil {
			DPrintf("[FATAL] DECODE error")
			return
		}
		K1, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			DPrintf("[FATAL] DECODE error")
			return
		}
		kv.DuplicatedMap[K1] = true
	}

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	// for follower, wait applyMsg
	if isLeader == false {
		reply.Err = ErrWrongLeader
		if kv.DuplicatedMap[args.ID] {
			return
		}
		reply.Err = ErrFailAgree
		return
	}
	cmd := Op{}
	cmd.ID = args.ID
	cmd.Key = args.Key
	cmd.Kind = READ
	DPrintf("[S%d] believe leader, start agree id=%v", kv.me, args.ID%SHOW_BIT)
	index, _, ok := kv.rf.Start(cmd) // for leader, try to issue an agreement
	if ok {
		startTime := time.Now()
		for time.Now().Sub(startTime) < 1*time.Second { // waiting for agreement
			if kv.DuplicatedMap[cmd.ID] { // success to read, set return value
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
			ms := 10 + rand.Int()%10
			time.Sleep(time.Duration(ms) * time.Millisecond)
			kv.mu.Lock()
		}
		// not agreement..
		DPrintf("[FATAL%d]: GET ID:%v key=%s at Index:%d Not Agree",
			kv.me, args.ID%SHOW_BIT, args.Key, index)
		reply.Err = ErrFailAgree // TODO what to do here?
		reply.Value = kv.Database[args.Key]
		return
	} else { // for follower, wait applyMsg
		reply.Err = ErrWrongLeader
		if kv.DuplicatedMap[args.ID] {
			return
		}
		reply.Err = ErrFailAgree
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
	if isLeader == false { // false, wait for agree
		if kv.DuplicatedMap[args.ID] { // success to sync
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = ErrFailAgree
		return
	}
	v := kv.DuplicatedMap[args.ID]
	if v == true { // ok before, ignore
		reply.Err = OK
		DPrintf("[S%d] Ignore same PA request ID=%v", kv.me, args.ID)
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

		RID := nrand()

		cmd1 := Op{}
		cmd1.ID = RID
		cmd1.Key = args.Key
		cmd1.Kind = READ

		var v string
		DPrintf("[S%d] believe leader, start agree phase1 id=%v RID:%v", kv.me, args.ID%SHOW_BIT, RID%SHOW_BIT)

		_, _, ok := kv.rf.Start(cmd1)
		if ok {
			DPrintf("[S%d] (%v)Append, first read Id=%v key=%s", kv.me, args.ID%SHOW_BIT, cmd1.ID%SHOW_BIT, cmd1.Key)
			startTime := time.Now()
			for time.Now().Sub(startTime) < 1*time.Second { // waiting for agreement
				if kv.DuplicatedMap[cmd1.ID] { // success to read
					v, ok = kv.Database[args.Key] // ok may be false because of first empty slot
					DPrintf("[S%d] leader, agree rid=%v", kv.me, cmd1.ID%SHOW_BIT)
					goto nextLabel
				}
				kv.mu.Unlock()
				ms := 10 + rand.Int()%10
				time.Sleep(time.Duration(ms) * time.Millisecond)
				kv.mu.Lock()
			}
			DPrintf("[S%d] leader, no agree rid=%v", kv.me, cmd1.ID%SHOW_BIT)
			reply.Err = ErrFailAgree
			return
		} else {
			if kv.DuplicatedMap[args.ID] { // success to sync
				reply.Err = ErrWrongLeader
				return
			}
			reply.Err = ErrFailAgree
			return
		}

	nextLabel:
		v = v + args.Value
		cmd.Key = args.Key
		cmd.ID = args.ID
		cmd.Value = v
		cmd.Kind = WRITE
	}

	// retry
	DPrintf("[S%d] believe leader, start agree phase2 id=%v", kv.me, args.ID%SHOW_BIT)
	index, _, ok := kv.rf.Start(cmd)
	if ok { // wait for agreement
		startTime := time.Now()
		for time.Now().Sub(startTime) < 1*time.Second { // waiting for agreement
			if kv.DuplicatedMap[cmd.ID] { // success to achieve agreement
				reply.Err = OK
				DPrintf("[S%d] success to put/append id=%d k=%s vh=%v", kv.me, cmd.ID%SHOW_BIT, cmd.Key, cmd.Value)
				return
			}
			kv.mu.Unlock()
			ms := 10 + rand.Int()%10
			time.Sleep(time.Duration(ms) * time.Millisecond)
			kv.mu.Lock()
		}
		// not agreement..
		DPrintf("[FATAL%d]: APPEND_2 ID:%v key=%s v=%v at Index:%d Not Agree",
			kv.me, args.ID%SHOW_BIT, args.Key, args.Value, index)
		reply.Err = ErrFailAgree
		return
	} else {
		if kv.DuplicatedMap[args.ID] { // success to sync
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = ErrFailAgree
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
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate { // make snapshot
				DPrintf("S[%d] Applier Make Snapshot", kv.me)
				kv.rf.Snapshot(msg.CommandIndex, kv.MakeSnapshot())
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid { // read snapshot
			kv.mu.Lock()
			DPrintf("S[%d] Applier read snapshot", kv.me)
			kv.ReadSnapshot(msg.Snapshot)
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
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.Database = make(map[string]string)
	kv.DuplicatedMap = make(map[int64]bool)
	go kv.applier(kv.applyCh)
	time.Sleep(10 * time.Millisecond)
	return kv
}
