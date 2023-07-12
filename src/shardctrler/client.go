package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ID = nrand()
	args.Num = num
	DPrintf("[client]: Query configNum=%d Id=%v", num, args.ID%SHOW_BIT)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[client]: Success Query configNum=%d Id=%v cfg:%s",
					num, args.ID%SHOW_BIT, ConfigToString(reply.Config))
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ID = nrand()
	DPrintf("[client]: Join Id=%v gids:%s", args.ID%SHOW_BIT, ServerToString(servers))
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[client]: Success Join Id=%v gids:%s", args.ID%SHOW_BIT, ServerToString(servers))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ID = nrand()
	DPrintf("[client]: Leave Id=%v gids:%s", args.ID%SHOW_BIT, ArrayToString(gids))
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[client]: Success Leave Id=%v gids:%s", args.ID%SHOW_BIT, ArrayToString(gids))
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ID = nrand()
	DPrintf("[client]: Move Id=%v shard->gid(%d->%d)", args.ID%SHOW_BIT, shard, gid)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				DPrintf("[client]: Success Move Id=%v shard->gid(%d->%d)", args.ID%SHOW_BIT, shard, gid)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
