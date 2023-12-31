package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"fmt"
	"time"

	"6.5840/debugutils"
	"6.5840/labrpc"
	"6.5840/shardctrler"
)

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	//
	clientId int64
	seqId    int
	// debugutils
	logger debugutils.Logger
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("{clientId=%d seqId=%d sm=%+v}", ck.clientId, ck.seqId, ck.sm)
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.config = ck.sm.Query(-1)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.seqId = 0
	ck.logger = *debugutils.NewLogger(fmt.Sprintf("Clerk %d", ck.clientId), ClientDefaultLogLevel)
	ck.logger.Debug("success make ck=%+v", ck)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		Key:      key,
	}
	ck.seqId++
	for {
		shardId := key2shard(key)
		gid := ck.config.Shards[shardId]
		ck.logger.Debug("[%d][Get][begin], key=[%s]", args.SeqId, args.Key)
		ck.logger.Debug("ck.config=%+v", ck.config)
		ck.logger.Debug("shardId=[%d], groupId=[%d]", shardId, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				ck.logger.Debug("->[%s]", servers[si])
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
					if reply.Err == OK {
						ck.logger.Debug("[%d][Get][end], reply.Value=[%s]", args.SeqId, reply.Value)
						return reply.Value
					}
					if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(WrongGroupWaitTime)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		Key:      key,
		Value:    value,
		Type:     op,
	}
	ck.seqId++
	for {
		shardId := key2shard(key)
		gid := ck.config.Shards[shardId]
		ck.logger.Debug("[%d][%s][begin], key=[%s], value=[%s]", args.SeqId, args.Type, args.Key, args.Value)
		ck.logger.Debug("ck.config=%+v", ck.config)
		ck.logger.Debug("shardId=[%d], groupId=[%d]", shardId, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				ck.logger.Debug("->[%s]", servers[si])
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
					if reply.Err == OK {
						ck.logger.Debug("[%d][%s][OK]", args.SeqId, args.Type)
						return
					}
					if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(WrongGroupWaitTime)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
