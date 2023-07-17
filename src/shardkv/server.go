package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
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
	Database     map[string]string
	Shards       []int
	Kind         CommandType
}

type ShardKV struct {
	mu           sync.Mutex
	wmu          sync.Mutex
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
	config       shardctrler.Config
	toPullMap    map[int][]int
	pushing      bool
	updatingFlag bool

	// Your definitions here.
	Database     map[string]string
	DuplicateMap map[int64]bool
}

// MakeSnapshot should use when locked
func (kv *ShardKV) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(len(kv.Database))
	for k, v := range kv.Database {
		e.Encode(k)
		e.Encode(v)
	}
	e.Encode(len(kv.DuplicateMap))
	for k, _ := range kv.DuplicateMap {
		e.Encode(k)
	}
	return w.Bytes()
}

// ReadSnapshot
func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	DPrintf("[s:%d, gid:%d] ReadSnapshot", kv.me, kv.gid)
	//kv.Database = make(map[string]string)
	//kv.DuplicateMap = make(map[int64]bool)
	var dblen, dmlen int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&dblen) != nil {
		DPrintf("[FATAL] DECODE error")
		return
	}
	for i := 0; i < dblen; i++ {
		var k, v string
		if d.Decode(&k) != nil || d.Decode(&v) != nil {
			DPrintf("[FATAL] DECODE error")
			return
		}
		kv.Database[k] = v
	}
	if d.Decode(&dmlen) != nil {
		DPrintf("[FATAL] DECODE error")
		return
	}
	for i := 0; i < dmlen; i++ {
		var k int64
		if d.Decode(&k) != nil {
			DPrintf("[FATAL] DECODE error")
			return
		}
		kv.DuplicateMap[k] = true
	}
}

// IfRightGroup TODO during the re-configuration?
func (kv *ShardKV) IfRightGroup(key string) bool {
	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) WaitForApply(op Op, t int) Err {
	startTime := time.Now()
	du := time.Duration(t)
	for time.Now().Sub(startTime) < du*time.Second {
		if kv.DuplicateMap[op.ID] == true { // applied
			return OK
		}
		//DPrintf("[s:%d gid:%d] unlock", kv.me, kv.gid)
		kv.mu.Unlock()
		ms := 40 + rand.Int()%20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		kv.mu.Lock()
		//DPrintf("[s:%d gid:%d] lock", kv.me, kv.gid)
	}
	// not agreement
	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//DPrintf("[s:%d gid:%d] lock", kv.me, kv.gid)
	defer kv.mu.Unlock()
	defer func() {
		//DPrintf("[s:%d gid:%d] unlock", kv.me, kv.gid)
	}()

	reply.ID = args.ID

	if kv.pushing {
		reply.Err = ErrWrongGroup
		return
	}

	// return immediately when not the right group
	if kv.IfRightGroup(args.Key) == false {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.CheckIfAllGet(kv.config.Num) == false {
		reply.Err = ErrWrongGroup
		DPrintf("[s:%d gid:%d] Wait version %d before Get", kv.me, kv.gid, kv.config.Num)
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
	configNumber := kv.config.Num

	_, _, ok := kv.rf.Start(cmd)
	if ok { // wait..
		reply.Err = kv.WaitForApply(cmd, 2)
		reply.Value = kv.Database[args.Key]
		// check the configNum not changed
		if kv.config.Num != configNumber || kv.pushing ||
			kv.CheckIfAllGet(kv.config.Num) == false || kv.IfRightGroup(args.Key) == false {
			reply.Err = ErrWrongGroup
		}
	} else { // not the leader
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.wmu.Lock()
	defer kv.wmu.Unlock()
	kv.mu.Lock()
	//DPrintf("[s:%d gid:%d] lock", kv.me, kv.gid)
	defer kv.mu.Unlock()
	defer func() {
		//DPrintf("[s:%d gid:%d] unlock", kv.me, kv.gid)
	}()
	reply.ID = args.ID

	if kv.pushing {
		reply.Err = ErrWrongGroup
		return
	}

	// return immediately when not the right group
	if kv.IfRightGroup(args.Key) == false {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.CheckIfAllGet(kv.config.Num) == false {
		reply.Err = ErrWrongGroup
		DPrintf("[s:%d gid:%d] Wait version %d before PA", kv.me, kv.gid, kv.config.Num)
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
			reply.Err = kv.WaitForApply(cmd1, 2)
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
		reply.Err = kv.WaitForApply(cmd2, 2)
		if kv.config.Num != configNumber || kv.pushing || kv.CheckIfAllGet(kv.config.Num) == false ||
			kv.IfRightGroup(args.Key) == false {
			reply.Err = ErrWrongGroup
		}
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
		//DPrintf("[s:%d gid:%d] lock", kv.me, kv.gid)
		_, isLeader := kv.rf.GetState()
		if isLeader && !kv.pushing { // optimize of useless query, and delay updateConfig when PA
			cmd := Op{
				ID:           nrand(),
				Kind:         UpdateConfigCommand,
				ConfigX:      kv.sm.Query(kv.config.Num + 1),
				ConfigNumber: kv.config.Num,
			}
			kv.rf.Start(cmd) // start issue an agreement
		}
		//DPrintf("[s:%d gid:%d] unlock", kv.me, kv.gid)
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
			//DPrintf("[s:%d gid:%d] lock", kv.me, kv.gid)
			if kv.DuplicateMap[AppliedCmd.ID] == false {
				if AppliedCmd.Kind == MergeDBCommand { // special command
					kv.DuplicateMap[AppliedCmd.ID] = true
					DPrintf("[s:%d gid:%d] Apply command kind=%s shards:%s id=%v", kv.me, kv.gid, AppliedCmd.Kind, ListTostring(AppliedCmd.Shards), AppliedCmd.ID%SHOW_BIT)
					kv.MergeDB(AppliedCmd.Database, AppliedCmd.Shards)
					kv.DeleteToPullMap(AppliedCmd.ConfigX.Num, AppliedCmd.Shards)
				} else {
					if kv.config.Num != AppliedCmd.ConfigNumber && AppliedCmd.Kind != UpdateConfigCommand {
						DPrintf("[s:%d gid:%d] ConfigNumber changed lst:%d now:%d, give up cmd id=%v", kv.me, kv.gid,
							AppliedCmd.ConfigNumber, kv.config.Num, AppliedCmd.ID%SHOW_BIT)
					} else {
						DPrintf("[s:%d gid:%d] Apply command kind=%s id=%v", kv.me, kv.gid, AppliedCmd.Kind, AppliedCmd.ID%SHOW_BIT)
						kv.DuplicateMap[AppliedCmd.ID] = true
						if AppliedCmd.Kind == UpdateConfigCommand { // update config
							newConfig := shardctrler.ConfigDeepCopy(AppliedCmd.ConfigX, false)
							if !reflect.DeepEqual(newConfig, kv.config) { // optimize for unnecessary pull
								DPrintf("[s:%d gid:%d] update config old:%s", kv.me, kv.gid, shardctrler.ConfigToString(kv.config))
								DPrintf("[s:%d gid:%d] update config new:%s", kv.me, kv.gid, shardctrler.ConfigToString(newConfig))
								kv.SetToPullMap(newConfig, kv.config) // newConfig's pull map
								_, isLeader := kv.rf.GetState()
								if isLeader {
									//db := DeepCopyMap(kv.Database)
									cfg := shardctrler.ConfigDeepCopy(kv.config, false)
									go kv.PushShard(newConfig, cfg)
								}
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
			} else {
				DPrintf("[s:%d gid:%d] Duplicated cmd kind=%s id=%v", kv.me, kv.gid, AppliedCmd.Kind, AppliedCmd.ID%SHOW_BIT)
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate { // make snapshot
				DPrintf("[s:%d gid:%d] Applier Make Snapshot", kv.me, kv.gid)
				kv.rf.Snapshot(msg.CommandIndex, kv.MakeSnapshot())
			}
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			DPrintf("[s:%d gid:%d] Applier read snapshot", kv.me, kv.gid)
			kv.ReadSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) SetToPullMap(newConfig shardctrler.Config, oldConfig shardctrler.Config) {
	for i := 0; i < shardctrler.NShards; i++ {
		if oldConfig.Shards[i] == 0 {
			continue
		}
		if newConfig.Shards[i] == kv.gid && oldConfig.Shards[i] != kv.gid {
			kv.toPullMap[newConfig.Num] = append(kv.toPullMap[newConfig.Num], i)
		}
	}
	DPrintf("[s:%d gid:%d] After set map cfn:%d map:%s", kv.me, kv.gid, newConfig.Num, ListTostring(kv.toPullMap[newConfig.Num]))
}

func (kv *ShardKV) DeleteToPullMap(cfn int, shards []int) {
	var remain []int
	for _, v := range kv.toPullMap[cfn] {
		flag := false
		for _, shard := range shards {
			if v == shard {
				flag = true
				break
			}
		}
		if flag == false {
			remain = append(remain, v)
		}
	}
	kv.toPullMap[cfn] = remain
	DPrintf("[s:%d gid:%d] After delete map cfn:%d map:%s", kv.me, kv.gid, cfn, ListTostring(kv.toPullMap[cfn]))
}

// WaitUntilAllGet, wait until this kv-store
func (kv *ShardKV) WaitUntilAllGet(cfn int) {
	for len(kv.toPullMap[cfn]) > 0 {
		_, isLeader := kv.rf.GetState()
		DPrintf("[s:%d gid:%d] Waiting until get shards:%s isleader:%v", kv.me, kv.gid, ListTostring(kv.toPullMap[cfn]), isLeader)
		kv.mu.Unlock()
		ms := 490 + rand.Int()%20
		time.Sleep(time.Duration(ms) * time.Millisecond)
		kv.mu.Lock()
	}
	DPrintf("[s:%d gid:%d] Waiting until get shards success at cfn:%d", kv.me, kv.gid, cfn)
}

func (kv *ShardKV) CheckIfAllGet(cfn int) bool {
	return len(kv.toPullMap[cfn]) == 0
}

// PushShard use when locked..
func (kv *ShardKV) PushShard(newConfig shardctrler.Config, oldConfig shardctrler.Config) {
	kv.pushing = true
	defer func() {
		kv.pushing = false
	}()
	var lostsShard []int
	var targetGid = make(map[int][]int) // gid -> list of shards
	var targetId = make(map[int]int64)
	var unDoneGid = make(map[int]bool)
	for i := 0; i < shardctrler.NShards; i++ {
		if oldConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
			lostsShard = append(lostsShard, i)
			targetGid[newConfig.Shards[i]] = append(targetGid[newConfig.Shards[i]], i)
		}
	}
	// allocate Id for each targetGid
	for gid, _ := range targetGid {
		targetId[gid] = nrand()
		unDoneGid[gid] = true
	}
	for len(unDoneGid) > 0 {
		var copiedDB map[string]string
		if kv.CheckIfAllGet(oldConfig.Num) == false {
			DPrintf("[s:%d gid:%d] wait cfn:%d before push cfn:%d", kv.me, kv.gid, oldConfig.Num, newConfig.Num)
			goto sleepLabel
		}
		// got db here
		kv.mu.Lock()
		copiedDB = DeepCopyMap(kv.Database)
		kv.mu.Unlock()
		for gid, list := range targetGid {
			if unDoneGid[gid] == false {
				continue
			}
			copiedShards := DeepCopyList(list)
			id := targetId[gid]
			DPrintf("[s:%d gid:%d] Try to push shards:%s to gid:%d id:%v db:%s", kv.me, kv.gid, ListTostring(list), gid, id%SHOW_BIT, kv.DBTS(copiedDB))
			for _, server := range newConfig.Groups[gid] {
				args := PushShardArgs{
					Gid:      kv.gid,
					Shards:   copiedShards,
					Database: copiedDB,
					Id:       id,
					ConfigX:  newConfig,
				}
				reply := PushShardReply{}
				//DPrintf("[s:%d gid:%d] send push shards to server %s id:%v", kv.me, kv.gid, server, id%SHOW_BIT)
				ok := kv.make_end(server).Call("ShardKV.ReceiveShard", &args, &reply)
				//DPrintf("[s:%d gid:%d] send push shards to server %s shards:%s result %s", kv.me, kv.gid, server, ListTostring(args.Shards), reply.Err)
				if ok && reply.Err == OK {
					DPrintf("[s:%d gid:%d] Success to push shards:%s to gid:%d id:%v cfn:%d", kv.me, kv.gid, ListTostring(list), gid, id%SHOW_BIT, newConfig.Num)
					delete(unDoneGid, gid)
					break
				}
			}
		}
	sleepLabel:
		ms := 200 + rand.Int()%100
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// use when locked
func (kv *ShardKV) ReceiveShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.DuplicateMap[args.Id] { // duplicate
		reply.Err = OK
		return
	}
	id := args.Id
	cmd := Op{
		ID:       id,
		Kind:     MergeDBCommand,
		Shards:   DeepCopyList(args.Shards),
		Database: DeepCopyMap(args.Database),
		ConfigX:  shardctrler.ConfigDeepCopy(args.ConfigX, false),
	}
	// skip if not set newConfig here
	if len(kv.toPullMap[args.ConfigX.Num]) == 0 {
		reply.Err = ErrWrongGroup
		DPrintf("[s:%d gid:%d] Waiting to update config before receive shards %s", kv.me, kv.gid, ListTostring(cmd.Shards))
		return
	}

	DPrintf("[s:%d gid:%d] Receiver start to issue cmd on shards:%s", kv.me, kv.gid, ListTostring(cmd.Shards))
	_, _, ok := kv.rf.Start(cmd)
	if ok { // leader
		reply.Err = kv.WaitForApply(cmd, 2)
	} else {
		reply.Err = ErrWrongLeader
	}
	return
}

func (kv *ShardKV) Mig() {
	for {
		for i := 0; i < shardctrler.NShards; i++ {

		}
		ms := 300 + rand.Int()%200
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (kv *ShardKV) DBTS(db map[string]string) string {
	res := ""
	for k, v := range db {
		res += fmt.Sprintf(" key=%s, v=%s ", k, v)
	}
	return res
}

func (kv *ShardKV) MergeDB(db map[string]string, shards []int) {
	for k, v := range db {
		flag := false
		for _, s := range shards {
			if key2shard(k) == s {
				flag = true
				break
			}
		}
		//DPrintf("[s:%d gid:%d] db k=%s v=%s", kv.me, kv.gid, k, v)
		if flag {
			DPrintf("[s:%d gid:%d] change k=%s v=%s", kv.me, kv.gid, k, v)
			kv.Database[k] = v
		}
	}
	DPrintf("[s:%d gid:%d] After merge db: %s", kv.me, kv.gid, kv.DBTS(kv.Database))
}

func ShardsToString(sh map[int]bool) string {
	res := ""
	for s, _ := range sh {
		res = res + strconv.Itoa(s) + " "
	}
	return res
}

//// WaitUntilAllGive use when locked
//func (kv *ShardKV) WaitUntilAllGive(newConfig shardctrler.Config, oldConfig shardctrler.Config) {
//	_, isLeader := kv.rf.GetState()
//	if isLeader == false { // only focus on leader
//		return
//	}
//	var targetShards = make(map[int]bool)
//	for i := 0; i < shardctrler.NShards; i++ {
//		if oldConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
//			targetShards[i] = true
//		}
//	}
//	for len(targetShards) > 0 { // wait until give
//		okayS := -1
//		DPrintf("[s:%d gid:%d] Older owner wait for grant shards:%s", kv.me, kv.gid, ShardsToString(targetShards))
//		for k, _ := range targetShards {
//			cc := make(map[int]bool)
//			for _, se := range kv.grantMap[k] {
//				cc[se] = true
//			}
//			newGid := newConfig.Shards[k]
//			if len(cc) >= len(newConfig.Groups[newGid]) { // okay to give all
//				okayS = k
//				break
//			}
//		}
//		if okayS != -1 {
//			delete(targetShards, okayS)
//			DPrintf("[s:%d gid:%d] Older owner grant shard:%d success", kv.me, kv.gid, okayS)
//		}
//		kv.mu.Unlock()
//		ms := 40 + rand.Int()%20
//		time.Sleep(time.Duration(ms) * time.Millisecond)
//		kv.mu.Lock()
//	}
//	DPrintf("[s:%d gid:%d] Older owner success to grant all genid=%d", kv.me, kv.gid, oldConfig.Num)
//	// clear grant map
//	for i := 0; i < shardctrler.NShards; i++ {
//		kv.grantMap[i] = kv.grantMap[i][:0]
//	}
//}

//// PullShard
//// if configuration changed, pull shards from other group using RPC
//// TODO how to prove pull the latest version of the target group's servers.
//// only pull from leader?
//// TODO when it triggered? what's the order with put/append ?
//// after config changed, then pull. The order is proved by the order of config change/
//// TODO problem1: got empty when read
//// this may be the priority pull shard not finished,
//// TODO problem2: the concurrent join/leave may cause ctl error?
//func (kv *ShardKV) PullShard(newConfig shardctrler.Config, oldConfig shardctrler.Config) {
//	oldOwns := make(map[int]bool)
//
//	for i := 0; i < shardctrler.NShards; i++ {
//		if oldConfig.Shards[i] == kv.gid { // owned
//			oldOwns[i] = true
//		}
//	}
//	for i := 0; i < shardctrler.NShards; i++ {
//		if oldConfig.Shards[i] == 0 { // skip default shards change
//			continue
//		}
//		if newConfig.Shards[i] == kv.gid && oldOwns[i] == false { // should pull
//			// find the oldOwns of
//			oldGid := oldConfig.Shards[i]
//			for {
//				okay := false
//				DPrintf("[s:%d, gid:%d] try to pull shard %d from gid:%d", kv.me, kv.gid, i, oldGid)
//				for _, server := range oldConfig.Groups[oldGid] {
//					args := PullShardArgs{
//						Gid:    kv.gid,
//						Shard:  i,
//						Server: kv.me,
//						MyGen:  oldConfig.Num,
//					}
//					reply := PullShardReply{}
//					ok := kv.make_end(server).Call("ShardKV.GrantShard", &args, &reply)
//					if ok && reply.Err == OK { // only from leader...
//						okay = true
//						kv.MergeDB(reply.Database)
//						DPrintf("[s:%d, gid:%d] pull shard from gid:%d server:%s okay", kv.me, kv.gid, oldGid, server)
//					} else {
//						DPrintf("[s:%d, gid:%d] pull shard from gid:%d server:%s error:%s", kv.me, kv.gid, oldGid, server, reply.Err)
//					}
//				}
//				if okay {
//					break
//				}
//				ms := 90 + rand.Int()%20
//				time.Sleep(time.Duration(ms) * time.Millisecond)
//			}
//		}
//	}
//	//// done..
//	//for _, shard := range targetShards {
//	//	oldGid := oldConfig.Shards[shard]
//	//	for {
//	//		okay := false
//	//		for _, server := range oldConfig.Groups[oldGid] {
//	//			args := PullDoneArgs{
//	//				Gid:     kv.gid,
//	//				PullGen: oldConfig.Num,
//	//			}
//	//			reply := PullDoneReply{}
//	//			ok := kv.make_end(server).Call("ShardKV.")
//	//		}
//	//	}
//	//
//	//}
//}
//
//
//func (kv *ShardKV) GrantShard(args *PullShardArgs, reply *PullShardReply) {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	_, isLeader := kv.rf.GetState()
//	if isLeader == false {
//		reply.Err = ErrWrongLeader
//		return
//	}
//	if args.MyGen > kv.config.Num { // reject pull from newer peers.
//		reply.Err = ErrWrongGroup
//		return
//	}
//	//if kv.onChanging { // reject when a Put/Append request is processing...
//	//	reply.Err = ErrWrongLeader
//	//	DPrintf("[s:%d, gid:%d] Grant Shard %d to gid:%d fail because onChanging", kv.me, kv.gid, args.Shard, args.Gid)
//	//	return
//	//}
//	//if args.TargetGen != kv.config.Num {
//	//	reply.Err = ErrWrongGroup
//	//	DPrintf("[s:%d, gid:%d] Grant Shard %d to gid:%d fail because tg:%d != myg:%d", kv.me, kv.gid, args.Shard, args.Gid, args.TargetGen, kv.config.Num)
//	//	return
//	//}
//	// remove this check, because the old owner's authority may has been canceled
//
//	//if kv.config.Shards[args.Shard] != kv.gid {
//	//	DPrintf("[s:%d, gid:%d] Got pull request, but config's gid(%d) != my(%d)!", kv.me, kv.gid, kv.config.Shards[args.Shard], kv.gid)
//	//	reply.Err = ErrWrongGroup
//	//	return
//	//}
//	DPrintf("[s:%d, gid:%d] Grant Shard %d to gid:%d", kv.me, kv.gid, args.Shard, args.Gid)
//	kv.grantMap[args.Shard] = append(kv.grantMap[args.Shard], args.Server)
//	reply.Database = make(map[string]string)
//	for k, v := range kv.Database {
//		if key2shard(k) == args.Shard {
//			reply.Database[k] = v
//		}
//	}
//	reply.Err = OK
//	return
//}

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
	kv.toPullMap = make(map[int][]int)
	kv.ReadSnapshot(persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start some background threads
	go kv.UpdateConfig()
	go kv.Applier()

	return kv
}
