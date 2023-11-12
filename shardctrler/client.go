package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.5840/debugutils"
	"6.5840/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int
	seqId    int
	// debugutils
	logger debugutils.Logger
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("{clientId=%d seqId=%d}", ck.clientId, ck.seqId)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var ID int = 0
var idMu sync.Mutex

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	//
	idMu.Lock()
	ck.clientId = ID
	ID++
	idMu.Unlock()
	ck.seqId = 0
	ck.logger = *debugutils.NewLogger(fmt.Sprintf("Clerk %d", ck.clientId), ClientDefaultLogLevel)

	ck.logger.Info("success make ck=%+v", ck)

	return ck
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		BaseArgs: BaseArgs{
			ck.clientId,
			ck.seqId,
		},
		Servers: servers,
	}
	ck.seqId++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.BaseReply.WrongLeader {
				return
			}
		}
		time.Sleep(NoLeaderSleepTime)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		BaseArgs: BaseArgs{
			ck.clientId,
			ck.seqId,
		},
		GIDs: gids,
	}
	ck.seqId++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.BaseReply.WrongLeader {
				return
			}
		}
		time.Sleep(NoLeaderSleepTime)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		BaseArgs: BaseArgs{
			ck.clientId,
			ck.seqId,
		},
		Shard: shard,
		GID:   gid,
	}
	ck.seqId++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.BaseReply.WrongLeader {
				return
			}
		}
		time.Sleep(NoLeaderSleepTime)
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		BaseArgs: BaseArgs{
			ck.clientId,
			ck.seqId,
		},
		Num: num,
	}
	ck.seqId++
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.BaseReply.WrongLeader {
				return reply.Config
			}
		}
		time.Sleep(NoLeaderSleepTime)
	}
}
