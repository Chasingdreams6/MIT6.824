package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"math/rand"
	"reflect"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

// Op read/put/append command consensus for raft
// Field names must start with capital letters,
type Op struct {
	ID           int64              // unique id for duplicate detection
	Key          string             // useful for read/put/append
	Value        string             // useful for put/append
	ConfigNumber int                // use for redo
	ConfigX      shardctrler.Config // only useful for updateConfig CMD
	Kind         CommandType
}

type ShardKV struct {
	mu           sync.Mutex
	wmu          sync.Mutex
	onChanging   bool
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
		ID:           args.ID,
		Kind:         GetCommand,
		Key:          args.Key,
		ConfigNumber: kv.config.Num,
	}

	_, _, ok := kv.rf.Start(cmd)
	if ok { // wait..
		reply.Err = kv.WaitForApply(cmd)
		reply.Value = kv.Database[args.Key]
	} else { // not the leader
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.wmu.Lock()
	defer kv.wmu.Unlock()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.onChanging = true
	defer func() {
		kv.onChanging = false
	}()
	reply.ID = args.ID
	// return immediately when not the right group
	if kv.IfRightGroup(args.Key) == false {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.DuplicateMap[args.ID] {
		reply.Err = OK
		return
	}

	configNumber := kv.config.Num
	v := ""
	if args.Op == "Append" { // read first
		cmd1 := Op{
			ID:           nrand(),
			Kind:         GetCommand,
			Key:          args.Key,
			ConfigNumber: configNumber,
		}
		_, _, ok := kv.rf.Start(cmd1)
		if ok {
			reply.Err = kv.WaitForApply(cmd1)
			if reply.Err != OK {
				return
			}
			v = kv.Database[args.Key]
		} else {
			reply.Err = ErrWrongLeader
		}
	}
	DPrintf("[s:%d gid:%d] Append(put) First read v=%s id=%v StartConfigNumber:%d Now:%d", kv.me, kv.gid, v, args.ID%SHOW_BIT, configNumber, kv.config.Num)
	if kv.config.Num != configNumber { // config changed, fail
		reply.Err = ErrWrongGroup
		return
	}
	v = v + args.Value
	cmd2 := Op{
		ID:           args.ID,
		Kind:         PutCommand,
		Key:          args.Key,
		Value:        v,
		ConfigNumber: configNumber,
	}
	_, _, ok := kv.rf.Start(cmd2)
	if ok {
		reply.Err = kv.WaitForApply(cmd2)
	} else {
		reply.Err = ErrWrongLeader
	}
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
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		if isLeader { // optimize of useless query
			cmd := Op{
				ID:           nrand(),
				Kind:         UpdateConfigCommand,
				ConfigX:      kv.sm.Query(-1),
				ConfigNumber: kv.config.Num,
			}
			kv.rf.Start(cmd) // start issue an agreement
		}
		kv.mu.Unlock()
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
				if kv.config.Num != AppliedCmd.ConfigNumber {
					DPrintf("[s:%d gid:%d] ConfigNumber changed lst:%d now:%d, give up", kv.me, kv.gid,
						AppliedCmd.ConfigNumber, kv.config.Num)
				} else {
					DPrintf("[s:%d gid:%d] Apply command kind=%s id=%v", kv.me, kv.gid, AppliedCmd.Kind, AppliedCmd.ID%SHOW_BIT)
					kv.DuplicateMap[AppliedCmd.ID] = true
					if AppliedCmd.Kind == UpdateConfigCommand { // update config
						newConfig := shardctrler.ConfigDeepCopy(AppliedCmd.ConfigX, false)
						if !reflect.DeepEqual(newConfig, kv.config) {
							DPrintf("[s:%d gid:%d] update config old:%s", kv.me, kv.gid, shardctrler.ConfigToString(kv.config))
							DPrintf("[s:%d gid:%d] update config new:%s", kv.me, kv.gid, shardctrler.ConfigToString(newConfig))
							kv.PullShard(newConfig, kv.config)
							kv.config = newConfig
						}
					}
					if AppliedCmd.Kind == GetCommand { //

					}
					if AppliedCmd.Kind == PutCommand {
						kv.Database[AppliedCmd.Key] = AppliedCmd.Value
					}
				}
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// TODO snapshot
		}
	}
}

func (kv *ShardKV) MergeDB(db map[string]string) {
	for k, v := range db {
		kv.Database[k] = v
	}
}

// PullShard
// if configuration changed, pull shards from other group using RPC
// TODO how to prove pull the latest version of the target group's servers.
// only pull from leader?
// TODO when it triggered? what's the order with put/append ?
// after config changed, then pull. The order is proved by the order of config change/
func (kv *ShardKV) PullShard(newConfig shardctrler.Config, oldConfig shardctrler.Config) {
	oldOwns := make(map[int]bool)

	for i := 0; i < shardctrler.NShards; i++ {
		if oldConfig.Shards[i] == kv.gid { // owned
			oldOwns[i] = true
		}
	}
	for i := 0; i < shardctrler.NShards; i++ {
		if oldConfig.Shards[i] == 0 { // skip default shards change
			continue
		}
		if newConfig.Shards[i] == kv.gid && oldOwns[i] == false { // should pull
			// find the oldOwns of
			oldGid := oldConfig.Shards[i]
			for {
				okay := false
				DPrintf("[s:%d, gid:%d] try to pull shard from gid:%d", kv.me, kv.gid, oldGid)
				for _, server := range oldConfig.Groups[oldGid] {
					args := PullShardArgs{
						Gid:   kv.gid,
						Shard: i,
					}
					reply := PullShardReply{}
					ok := kv.make_end(server).Call("ShardKV.GrantShard", &args, &reply)
					if ok && reply.Err == OK { // only from leader...
						okay = true
						kv.MergeDB(reply.Database)
						DPrintf("[s:%d, gid:%d] pull shard from gid:%d server:%s okay", kv.me, kv.gid, oldGid, server)
					} else {
						DPrintf("[s:%d, gid:%d] pull shard from gid:%d server:%s error:%s", kv.me, kv.gid, oldGid, server, reply.Err)
					}
				}
				if okay {
					break
				}
				ms := 90 + rand.Int()%20
				time.Sleep(time.Duration(ms) * time.Millisecond)
			}
		}
	}
}

func (kv *ShardKV) GrantShard(args *PullShardArgs, reply *PullShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.onChanging { // reject when a Put/Append request is processing...
		reply.Err = ErrWrongLeader
		return
	}
	// remove this check, because the old owner's authority may has been canceled

	//if kv.config.Shards[args.Shard] != kv.gid {
	//	DPrintf("[s:%d, gid:%d] Got pull request, but config's gid(%d) != my(%d)!", kv.me, kv.gid, kv.config.Shards[args.Shard], kv.gid)
	//	reply.Err = ErrWrongGroup
	//	return
	//}
	DPrintf("[s:%d, gid:%d] Grant Shard %d to gid:%d", kv.me, kv.gid, args.Shard, args.Gid)
	reply.Database = make(map[string]string)
	for k, v := range kv.Database {
		if key2shard(k) == args.Shard {
			reply.Database[k] = v
		}
	}
	reply.Err = OK
	return
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
