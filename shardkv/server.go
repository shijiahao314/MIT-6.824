package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/debugutils"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	ClientId int64
	SeqId    int
	Type     OpType
	Key      string
	Value    string
}

type Shard struct {
	configNum int
	kvMap     map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int // groupId
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	//
	dead int32
	//
	mck *shardctrler.Clerk
	//
	seqMap  map[int64]int             // key: ck.clientId, value: ck.seqId
	chanMap map[int]chan CommandReply // key: index, value: CommandReply
	// debugutils
	logger debugutils.Logger
	//
	config shardctrler.Config
	shards []Shard // shardId -> Shard
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("{me=%d gid=%d}", kv.me, kv.gid)
}

func (kv *ShardKV) startCommand(cmd Op) Err {
	// 判断是否killed
	if kv.killed() {
		return ErrAlreadyKilled
	}
	// 发送指令并判断kv的rf是否是leader
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader
	}
	kv.logger.Debug("%s start [%d]:[%d], k:v=[%s]:[%s]",
		cmd.Type, cmd.ClientId, cmd.SeqId, cmd.Key, cmd.Value)
	// 结果回调channel
	ch := kv.getWaitCh(index)
	defer kv.closeAndDelete(index) // 超时或完成后关闭通道，防止内存泄漏
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听返回结果
	select {
	case opReply := <-ch:
		if cmd.ClientId != opReply.ClientId || cmd.SeqId != opReply.SeqId {
			return ErrInconsistentData // clientId or seqId not match
		}
		return opReply.Err
	case <-timer.C:
		return ErrRequestTimeOut
	}
}

// ShardKV receive Get RPC from Clerk
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.logger.Debug("Get start [%d]:[%d], key=[%s]",
		args.ClientId, args.SeqId, args.Key)
	// 判断Shard
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		kv.mu.Unlock()
		// 错误的Group
		reply.Err = ErrWrongGroup
		return
	}
	if kv.shards[shardId].kvMap == nil {
		kv.mu.Unlock()
		// Shard还未拉取
		reply.Err = ShardNotArrive
		return
	}
	kv.mu.Unlock()
	// 发送指令
	err := kv.startCommand(Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     GetOp,
		Key:      args.Key,
	})
	defer kv.logger.Debug("GetOp end [%d]:[%d], reply=%+v",
		args.ClientId, args.SeqId, reply)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	// 再次判断Shard
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].kvMap == nil {
		reply.Err = ShardNotArrive
	} else {
		reply.Err = OK
		reply.Value = kv.shards[key2shard(args.Key)].kvMap[args.Key]
	}
	kv.mu.Unlock()
}

// ShardKV receive Put or Append RPC from Clerk
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.logger.Debug("%s start [%d]:[%d], k:v=[%s]:[%s]",
		args.Op, args.ClientId, args.SeqId, args.Key, args.Value)
	defer kv.logger.Debug("reply.Err=%s", reply.Err)
	// 判断Shard
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	kv.logger.Debug("1")
	if kv.config.Shards[shardId] != kv.gid {
		kv.mu.Unlock()
		// 错误的Group
		// *****bug
		reply.Err = ErrWrongGroup
		return
	}
	kv.logger.Debug("2")
	if kv.shards[shardId].kvMap == nil {
		kv.mu.Unlock()
		// Shard还未拉取
		reply.Err = ShardNotArrive
		return
	}
	kv.mu.Unlock()
	// 发送指令
	reply.Err = kv.startCommand(Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	})
	kv.logger.Debug("%s end [%d]:[%d], reply=%+v",
		args.Op, args.ClientId, args.SeqId, reply)
}

// 判断是否是重复的
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeqId, exists := kv.seqMap[clientId]
	if !exists {
		return false
	}
	return seqId <= lastSeqId
}

// close chan at index and delete it
func (kv *ShardKV) closeAndDelete(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.chanMap[index]) // chan应该仅由发送方关闭（*）
	delete(kv.chanMap, index)
}

func (kv *ShardKV) getWaitCh(index int) chan CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.chanMap[index]
	if !ok {
		// （*）这里设置一个有1缓冲的，否则死锁
		kv.chanMap[index] = make(chan CommandReply, 1) // 内存泄漏？不会，因为会关闭通道
		ch = kv.chanMap[index]
	}
	return ch
}

func (kv *ShardKV) applyMsgHandler() {
	for !kv.killed() {
		// 从applyCh接收apply消息
		// 可以保证从applyCh接收的消息一定是多数认可的消息，且是有序的
		msg := <-kv.applyCh
		kv.logger.Debug("applier: receive apply message %+v", msg)
		switch {
		case msg.CommandValid:
			// CommandValid
			index := msg.CommandIndex
			cmd, ok := msg.Command.(Op)
			shardId := key2shard(cmd.Key)
			if !ok {
				panic("ERROR: cannot tranvert msg.Command to Op")
			}
			kv.logger.Debug("cmd.Type=%+v", cmd.Type)
			if cmd.Type == GetOp || cmd.Type == PutOp || cmd.Type == AppendOp {
				reply := CommandReply{
					ClientId: cmd.ClientId,
					SeqId:    cmd.SeqId,
				}
				// 先判断
				if kv.config.Shards[shardId] != kv.gid {
					// 不是对应Group
					reply.Err = ErrWrongGroup
				} else if kv.shards[shardId].kvMap == nil {
					// 分片应该存在但还未到达
					reply.Err = ShardNotArrive
				} else {
					// 分片存在
					if !kv.ifDuplicate(cmd.ClientId, cmd.SeqId) {
						kv.logger.Debug("not duplicate cmd=%+v", cmd)
						// 不是重复的指令
						kv.mu.Lock()
						kv.seqMap[cmd.ClientId] = cmd.SeqId
						switch cmd.Type {
						case GetOp:
							// do nothing
							kv.logger.Debug("after get, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						case PutOp:
							kv.shards[shardId].kvMap[cmd.Key] = cmd.Value
							kv.logger.Debug("after put, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						case AppendOp:
							kv.shards[shardId].kvMap[cmd.Key] = cmd.Value
							kv.logger.Debug("after append, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						default:
							fmt.Printf("cmd=%+v\n", cmd)
							panic("Unknown cmd.Type")
						}
						// 判断是否需要快照
						// if kv.maxraftstate != -1 &&
						// 	float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*RaftstateLoadFactor {
						// 	kv.makeSnapshot(index)
						// }
						kv.mu.Unlock()
					} else {
						kv.logger.Debug("is duplicate cmd=%+v", cmd)
					}
				}
				// 发送消息（考虑中途变更leader情况）
				if _, isLeader := kv.rf.GetState(); isLeader {
					kv.logger.Debug("is leader, send to chan[%d], reply=%+v", index, reply)
					kv.getWaitCh(index) <- reply
				}
			} else {
				// 更新
				kv.logger.Debug("cmd.Type=%s, cmd=%+v", cmd.Type, cmd)
			}
		case msg.SnapshotValid:
			// kv.readPersist(msg.Snapshot)
		default:
			fmt.Printf("msg=%+v\n", msg)
			panic("Unknown msg type")
		}
	}
}

// 配置探测器
func (kv *ShardKV) configDetector() {
	for !kv.killed() {
		// 定时拉取？
		// 判断是否是Leader
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()

		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// map
	kv.shards = make([]Shard, shardctrler.NShards)
	kv.seqMap = make(map[int64]int)
	kv.chanMap = make(map[int]chan CommandReply)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.logger = *debugutils.NewLogger(fmt.Sprintf("ShardKV %d-%d", kv.gid, kv.me), ShardKVDefaultLogLevel)
	kv.logger.Debug("success make ShardKV=%+v", kv)

	//
	go kv.applyMsgHandler()
	go kv.configDetector()

	return kv
}
