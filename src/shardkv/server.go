package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"math/rand"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

// Op read/put/append command consensus for raft
// Field names must start with capital letters,
type Op struct {
	ID      int64              // unique id for duplicate detection
	Key     string             // useful for read/put/append
	Value   string             // useful for put/append
	ConfigX shardctrler.Config // only useful for updateConfig CMD
	Kind    CommandType
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // the kvserver's belonged gid
	ctrlers      []*labrpc.ClientEnd
	peers        []*labrpc.ClientEnd
	sm           *shardctrler.Clerk // shardCtrler client
	maxraftstate int                // snapshot if log grows this big

	// config
	config shardctrler.Config
	// Your definitions here.
	Database     map[string]string
	DuplicateMap map[int64]bool
}

// MakeSnapshot
func (kv *ShardKV) MakeSnapshot() []byte {
	var res []byte
	// TODO
	return res
}

// ReadSnapshot
func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	DPrintf("[s:%d, gid:%d] ReadSnapshot", kv.me, kv.gid)
	// TODO
}

// IfRightGroup TODO during the re-configuration?
func (kv *ShardKV) IfRightGroup(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) WaitForApply(op Op) Err {
	startTime := time.Now()
	for time.Now().Sub(startTime) < 2*time.Second {
		if kv.DuplicateMap[op.ID] == true { // applied
			return OK
		}
		kv.mu.Unlock()
		ms := 90 + rand.Int()%20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		kv.mu.Lock()
	}
	// not agreement
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.ID = args.ID

	// return immediately when not the right group
	if kv.IfRightGroup(args.Key) == false {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.DuplicateMap[args.ID] {
		reply.Err = OK
		reply.Value = kv.Database[args.Key]
		return
	}

	cmd := Op{
		ID:   args.ID,
		Kind: GetCommand,
		Key:  args.Key,
	}

	_, _, ok := kv.rf.Start(cmd)
	if ok { // wait..

	} else { // not the leader

	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// UpdateConfig Background go-routine for update config every 100 ms
// TODO, the consensus of config in the group?
// Even if we use raft to prove consensus, when the config is not applied
// There may be occasion when each server has different configs
// We should use raft, because it can prove the order between put and re-configuration
// the raft's logs are linearized.
// INsight: a read must occur after the completion of configuration and pullShards ?
func (kv *ShardKV) UpdateConfig() {
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader { // optimize of useless query
			cmd := Op{
				ID:      nrand(),
				Kind:    UpdateConfigCommand,
				ConfigX: kv.sm.Query(-1),
			}
			kv.rf.Start(cmd) // start issue an agreement
		}
		ms := 190 + rand.Int()%20
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Applier for raft command
func (kv *ShardKV) Applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			AppliedCmd := msg.Command.(Op)
			kv.mu.Lock()
			if kv.DuplicateMap[AppliedCmd.ID] == false {
				DPrintf("[s:%d gid:%d] Apply command kind=%s id=%v", kv.me, kv.gid, AppliedCmd.Kind, AppliedCmd.ID%SHOW_BIT)
				kv.DuplicateMap[AppliedCmd.ID] = true
				if AppliedCmd.Kind == UpdateConfigCommand { // update config
					kv.config = shardctrler.ConfigDeepCopy(AppliedCmd.ConfigX, false)
				}
				// TODO other command
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// TODO snapshot
		}
	}
}

// PullShard
// if configuration changed, pull shards from other group using RPC
// TODO how to prove pull the latest version of the target group's servers
// TODO when it triggered? what's the order with put/append ?
func (kv *ShardKV) PullShard() {

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	if debugStart.IsZero() { // first server to init debugStart
		debugStart = time.Now()
	}

	kv.sm = shardctrler.MakeClerk(kv.ctrlers) // sm should only query config from ctrlers
	kv.peers = servers                        // peer servers in one group, we have same k-v pairs
	kv.persister = persister
	kv.Database = make(map[string]string)
	kv.DuplicateMap = make(map[int64]bool)

	kv.ReadSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start some background threads
	go kv.UpdateConfig()
	go kv.Applier()

	return kv
}
