package shardkv

import (
	"bytes"
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	persister    *raft.Persister
	applyCh      chan raft.ApplyMsg
	rf           *raft.Raft
	maxraftstate int                 // snapshot if log grows this big
	gid          int                 // groupId
	ctrlers      []*labrpc.ClientEnd // ShardCtrlers
	make_end     func(string) *labrpc.ClientEnd
	mck          *shardctrler.Clerk
	//
	seqMap  map[int64]int             // clientId -> seqId
	chanMap map[int]chan CommandReply // cmd.Index -> CommandReply
	//
	lastConfig shardctrler.Config // last config
	config     shardctrler.Config // now config
	shards     []Shard            // shardId -> Shard
	//
	dead int32
	// debugutils
	logger debugutils.Logger
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("{me=%d gid=%d maxraftsize=%d}", kv.me, kv.gid, kv.maxraftstate)
}

// call rf.Start to apply a command
func (kv *ShardKV) startCommand(cmd Op, timeoutPeroid time.Duration) Err {
	// 判断是否killed
	if kv.killed() {
		return ErrAlreadyKilled
	}
	// 发送指令并判断kv的rf是否是leader
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader
	}
	kv.logger.Debug("[%s][startCommand]<-[%d]:[%d], cmd=%+v",
		cmd.Type, cmd.ClientId, cmd.SeqId, cmd)
	// 结果回调channel
	ch := kv.getWaitCh(index)
	defer kv.closeAndDelete(index) // 超时或完成后关闭通道，防止内存泄漏
	// 设置超时Timer
	timer := time.NewTicker(timeoutPeroid)
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
	kv.logger.Debug("[Get][begin]<-[%d]:[%d], key=[%s]",
		args.ClientId, args.SeqId, args.Key)
	defer func() {
		// defer语句在声明时就会记录相关变量的值，而不是在实际执行时获取
		// 使用func包一层即可
		kv.logger.Debug("[Get][end]->[%d]:[%d], reply=%+v",
			args.ClientId, args.SeqId, reply)
	}()
	// 判断Shard
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		// 错误的Group
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	if kv.shards[shardId].KvMap == nil {
		// Shard还未收到
		// 注意，如果map == nil，那表明其未初始化
		// 如果len(map) == 0，那表明其为空，但是已经初始化，此时使用map == nil返回false
		// 但是（*）：作为stirng输出时，他们输出都是map[]
		kv.mu.Unlock()
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
	}, GetTimeout)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	// 再次判断Shard
	if kv.config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shards[shardId].KvMap == nil {
		reply.Err = ShardNotArrive
	} else {
		reply.Err = OK
		reply.Value = kv.shards[key2shard(args.Key)].KvMap[args.Key]
	}
	kv.mu.Unlock()
}

// ShardKV receive Put or Append RPC from Clerk
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.logger.Debug("[%s][begin]<-[%d]:[%d], k:v=[%s]:[%s]",
		args.Type, args.ClientId, args.SeqId, args.Key, args.Value)
	defer func() {
		kv.logger.Debug("[%s][end]->[%d]:[%d], reply=%+v",
			args.Type, args.ClientId, args.SeqId, reply)
	}()
	// 判断Shard
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		// 错误的Group
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	if kv.shards[shardId].KvMap == nil {
		// Shard还未收到
		kv.mu.Unlock()
		reply.Err = ShardNotArrive
		return
	}
	kv.mu.Unlock()
	// 发送指令
	reply.Err = kv.startCommand(Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     args.Type,
		Key:      args.Key,
		Value:    args.Value,
	}, PutAppendTimeout)
}

// ShardKV receive AddShard RPC from other ShardKV
func (kv *ShardKV) AddShard(args *AddShardArgs, reply *AddShardReply) {
	reply.Err = kv.startCommand(Op{
		ClientId: int64(args.GroupId),
		SeqId:    args.ConfigNum,
		Type:     AddShard,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastApplied,
	}, AddShardTimeout)
}

func (kv *ShardKV) applyMsgHandler() {
	for !kv.killed() {
		// 从applyCh接收apply消息
		// 可以保证从applyCh接收的消息一定是经过Raft层多数认可且有序的
		msg := <-kv.applyCh
		kv.logger.Debug("applier: receive msg=%+v", msg)
		switch {
		case msg.CommandValid:
			// CommandValid
			index := msg.CommandIndex
			cmd, ok := msg.Command.(Op)
			if !ok {
				panic("ERROR: cannot tranvert msg.Command to Op")
			}
			kv.logger.Debug("cmd.Type=%+v, cmd=%+v", cmd.Type, cmd)
			shardId := key2shard(cmd.Key)
			reply := CommandReply{
				ClientId: cmd.ClientId,
				SeqId:    cmd.SeqId,
				Err:      OK, // set default reply.Err OK
			}
			kv.mu.Lock()
			switch cmd.Type {
			case GetOp, PutOp, AppendOp:
				// 先判断
				if kv.config.Shards[shardId] != kv.gid {
					// 不是对应Group
					reply.Err = ErrWrongGroup
				} else if kv.shards[shardId].KvMap == nil {
					// 分片应该存在但还未到达
					reply.Err = ShardNotArrive
				} else {
					// 分片存在
					if !kv.ifDuplicate(cmd.ClientId, cmd.SeqId) {
						// 不是重复的指令
						kv.seqMap[cmd.ClientId] = cmd.SeqId
						switch cmd.Type {
						case GetOp:
							kv.logger.Debug("get, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						case PutOp:
							kv.logger.Debug("before put, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
							kv.shards[shardId].KvMap[cmd.Key] = cmd.Value
							kv.logger.Debug("after put, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						case AppendOp:
							kv.logger.Debug("before append, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
							kv.shards[shardId].KvMap[cmd.Key] += cmd.Value
							kv.logger.Debug("after append, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						default:
							fmt.Printf("cmd=%+v", cmd)
							panic("Unknown cmd.Type")
						}
					}
				}
			case AddShard:
				if kv.config.Num < cmd.SeqId {
					reply.Err = ConfigNotArrive
					break
				}
				kv.addShardHandler(cmd)
			case RemoveShard:
				kv.removeShardHandler(cmd)
			case UpdataShard:
				kv.updateShardHandler(cmd)
			default:
				fmt.Printf("cmd=%+v", cmd)
				panic("Unknown cmd.Type")
			}
			if kv.maxraftstate != -1 &&
				float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*RaftstateLoadFactor {
				// need snapshot
				kv.logger.Debug("kv.persister.RaftStateSize()=%d, ", kv.persister.RaftStateSize())
				kv.makeSnapshot(index)
			}
			kv.mu.Unlock()
			// 发送消息（考虑中途变更leader情况）
			if _, isLeader := kv.rf.GetState(); isLeader {
				// kv.logger.Debug("is leader, send to chan[%d], reply=%+v", index, reply)
				kv.getWaitCh(index) <- reply
			}
		case msg.SnapshotValid:
			kv.readPersist(msg.Snapshot)
		default:
			fmt.Printf("msg=%+v\n", msg)
			panic("Unknown msg type")
		}
	}
}

// make Snapshot
func (kv *ShardKV) makeSnapshot(index int) {
	// 本方法不加锁，建议调用该方法时持有锁
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.seqMap)
	e.Encode(kv.maxraftstate)
	e.Encode(kv.config)
	e.Encode(kv.lastConfig)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// addShardHandler
func (kv *ShardKV) addShardHandler(cmd Op) {
	// 本方法不加锁，建议调用该方法时持有锁
	if kv.shards[cmd.ShardId].KvMap != nil ||
		cmd.Shard.ConfigNum < kv.config.Num {
		return
	}
	kv.shards[cmd.ShardId] = kv.cloneShard(cmd.Shard.ConfigNum, cmd.Shard.KvMap)
	for clientId, seqId := range cmd.SeqMap {
		if r, ok := kv.seqMap[clientId]; !ok || r < seqId {
			kv.seqMap[clientId] = seqId
		}
	}
	kv.logger.Debug("after AddShard, kv.shards[%d]=%+v",
		cmd.ShardId, kv.shards[cmd.ShardId])
}

// removeShardHandler
func (kv *ShardKV) removeShardHandler(cmd Op) {
	// 本方法不加锁，建议调用该方法时持有锁
	if cmd.SeqId < kv.config.Num {
		return
	}
	kv.logger.Debug("before RemoveShard, kv.shards[%d]=%+v",
		cmd.ShardId, kv.shards[cmd.ShardId])
	kv.shards[cmd.ShardId].KvMap = nil
	kv.shards[cmd.ShardId].ConfigNum = cmd.SeqId
	kv.logger.Debug("after RemoveShard, kv.shards[%d]=%+v",
		cmd.ShardId, kv.shards[cmd.ShardId])
}

// updateShardHandler: update kv.config and kv.lastConfig
func (kv *ShardKV) updateShardHandler(cmd Op) {
	// 本方法不加锁，建议调用该方法时持有锁
	curConfig := kv.config
	newConfig := cmd.UpConfig
	if curConfig.Num >= newConfig.Num {
		return
	}
	for shardId, gid := range newConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shardId] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			// 未分配的shards[shardId]才会被清空
			kv.shards[shardId].KvMap = make(map[string]string)
			kv.shards[shardId].ConfigNum = newConfig.Num
		}
	}
	kv.lastConfig = curConfig
	kv.config = newConfig
	kv.logger.Debug("kv.lastConfig=%+v", kv.lastConfig)
	kv.logger.Debug("kv.config=%+v", kv.config)
	kv.logger.Debug("after UpdateShard, kv.shards[%d]=%+v",
		cmd.ShardId, kv.shards[cmd.ShardId])
}

// 判断kv不属于自己的Shard是否全部发送完毕
func (kv *ShardKV) allSent() bool {
	// 本方法不加锁，建议调用该方法时持有锁
	// 遍历上个配置的分片
	for shardId, gid := range kv.lastConfig.Shards {
		// 是 -> 不是
		if gid == kv.gid &&
			kv.config.Shards[shardId] != kv.gid &&
			kv.shards[shardId].ConfigNum < kv.config.Num {
			// 旧配置中该分片是本组分片
			// 新配置中该分片不是本组分片
			// 已记录的该分片配置号过时
			kv.logger.Debug("not allSent")
			kv.logger.Debug("lastConfig=%+v", kv.lastConfig)
			kv.logger.Debug("newConfig=%+v", kv.config)
			kv.logger.Debug("lastConfig->newConfig, shardId[%d] gid:[%d]->[%d] configNum:[%d]->[%d]",
				shardId, kv.config.Shards[shardId], kv.gid, kv.shards[shardId].ConfigNum, kv.config.Num)
			return false
		}
	}
	return true
}

// 判断kv属于自己的Shard是否全部接收完毕
func (kv *ShardKV) allReceived() bool {
	for shardId, gid := range kv.lastConfig.Shards {
		// 不是 -> 是
		if gid != kv.gid &&
			kv.config.Shards[shardId] == kv.gid &&
			kv.shards[shardId].ConfigNum < kv.config.Num {
			// 旧配置中该分片不是本组分片
			// 新配置中该分片是本组分片
			// 已记录的该分片配置号过时
			return false
		}
	}
	return true
}

// 配置探测器（定时拉取）
func (kv *ShardKV) configDetector() {
	kv.mu.Lock()
	rf := kv.rf
	kv.mu.Unlock()
	for !kv.killed() {
		// 判断是否是Leader
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Lock()
		switch {
		case !kv.allSent():
			// 没有全部发送完毕
			// deep copy of map
			seqMap := make(map[int64]int)
			for k, v := range kv.seqMap {
				seqMap[k] = v
			}
			// 遍历lastConfig中的所有的shardId -> gid
			for shardId, gid := range kv.lastConfig.Shards {
				// 判断该分片是否该发送而未发送
				if gid == kv.gid &&
					kv.config.Shards[shardId] != kv.gid &&
					kv.shards[shardId].ConfigNum < kv.config.Num {
					// 将不属于自己的分片发送给对应的ShardKV
					shard := kv.cloneShard(kv.config.Num, kv.shards[shardId].KvMap)
					args := AddShardArgs{
						ConfigNum:   kv.config.Num,
						GroupId:     gid,
						ShardId:     shardId,
						Shard:       shard,
						LastApplied: seqMap,
					}
					// shardId -> gid -> serverList -> servers
					// 获取最新配置中shardId对应gid组下所有的ShardKV列表
					serverList := kv.config.Groups[kv.config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serverList))
					for i, name := range serverList {
						servers[i] = kv.make_end(name)
					}
					kv.logger.Debug("should send AddShard args=%+v", args)
					// 对lastConfig中所有组发送这个自己不需要的分片
					go func(servers []*labrpc.ClientEnd, args *AddShardArgs) {
						start := time.Now()
						for i := 0; ; i++ {
							reply := AddShardReply{}
							ok := servers[i%len(servers)].Call("ShardKV.AddShard", args, &reply)
							if ok {
								if reply.Err == OK || time.Since(start) >= 2*time.Second {
									kv.logger.Debug("success send [%v]", time.Since(start))
									// 发送成功则删除自己的分片
									// 考虑 乱序 导致的问题
									kv.startCommand(Op{
										// ClientId: int64(kv.gid),
										// SeqId:    kv.config.Num,
										ClientId: int64(args.GroupId),
										SeqId:    args.ConfigNum,
										Type:     RemoveShard,
										ShardId:  args.ShardId,
									}, RemoveShardTimeout) // 为什么不怕这步失败？
									break // 退出
								}
							}
							if i != 0 && i%len(servers) == 0 {
								time.Sleep(UpConfigLoopInterval)
							}
						}
					}(servers, &args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		case !kv.allReceived():
			// 没有全部接收完毕
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		default:
			// 全部收发完毕
			curConfig := kv.config
			mck := kv.mck
			kv.mu.Unlock()
			newConfig := mck.Query(curConfig.Num + 1)
			if newConfig.Num != curConfig.Num+1 {
				// 不是想要的配置
				time.Sleep(UpConfigLoopInterval)
				continue
			}
			kv.logger.Debug("allSent and allReceived, receive new config")
			kv.logger.Debug("lastConfig=%+v", kv.lastConfig)
			kv.logger.Debug("nowConfig=%+v", kv.config)
			kv.logger.Debug("newConfig=%+v", newConfig)
			for shardId, shard := range kv.shards {
				if shard.KvMap == nil {
					continue
				}
				kv.logger.Debug("kv.shards[%d]=%+v", shardId, shard)
			}
			kv.logger.Debug("--------------------------------------------------")
			// 调用Raft同步，更新Config
			kv.startCommand(Op{
				ClientId: int64(kv.gid), // groupId
				SeqId:    newConfig.Num, // configNum
				Type:     UpdataShard,   // UpdataShard
				UpConfig: newConfig,     // Config
			}, UpdateConfigTimeout)
		}
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

// read persist
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		kv.shards = make([]Shard, shardctrler.NShards)
		kv.seqMap = make(map[int64]int)
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards []Shard
	var seqMap map[int64]int
	var maxraftstate int
	var config shardctrler.Config
	var lastConfig shardctrler.Config
	if d.Decode(&shards) != nil ||
		d.Decode(&seqMap) != nil ||
		d.Decode(&maxraftstate) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&lastConfig) != nil {
		kv.logger.Error("cannot readPersist")
	} else {
		kv.mu.Lock()
		kv.shards = shards
		kv.seqMap = seqMap
		kv.maxraftstate = maxraftstate
		kv.lastConfig = lastConfig
		kv.config = config
		kv.mu.Unlock()
	}
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
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxraftstate = maxraftstate
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.make_end = make_end
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.chanMap = make(map[int]chan CommandReply)
	kv.logger = *debugutils.NewLogger(fmt.Sprintf("ShardKV %d-%d", kv.gid, kv.me), ShardKVDefaultLogLevel)

	kv.readPersist(persister.ReadSnapshot())
	// kv.shards = make([]Shard, shardctrler.NShards)
	// kv.seqMap = make(map[int64]int)

	kv.logger.Debug("success make ShardKV=%+v", kv)

	go kv.applyMsgHandler()
	go kv.configDetector()

	return kv
}
