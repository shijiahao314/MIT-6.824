package shardkv

import (
	"bytes"
	"fmt"
	"strings"
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
	Key      string             // for: GetOp, PutOp
	Value    string             // for: PutOp
	ShardId  int                // for: AddShard, RemoveShard, UpdateConfig
	Shard    Shard              // for: AddShard
	SeqMap   map[int64]int      // for: AddShard
	UpConfig shardctrler.Config // for: UpdateConfig
}

func (op Op) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(fmt.Sprintf("ClientId:%d ", op.ClientId))
	sb.WriteString(fmt.Sprintf("SeqId:%d ", op.SeqId))
	sb.WriteString(fmt.Sprintf("Type:%s ", op.Type))
	switch op.Type {
	case GetOp:
		sb.WriteString(fmt.Sprintf("Key:%s", op.Key))
	case PutOp:
		sb.WriteString(fmt.Sprintf("Key:%s ", op.Key))
		sb.WriteString(fmt.Sprintf("Value:%s", op.Value))
	case AddShard:
		sb.WriteString(fmt.Sprintf("ShardId:%d ", op.ShardId))
		sb.WriteString(fmt.Sprintf("Shard:%+v ", op.Shard))
		sb.WriteString(fmt.Sprintf("SeqMap:%+v", op.SeqMap))
	case RemoveShard:
		sb.WriteString(fmt.Sprintf("ShardId:%d", op.ShardId))
	case UpdateConfig:
		sb.WriteString(fmt.Sprintf("ShardId:%d ", op.ShardId))
		sb.WriteString(fmt.Sprintf("UpConfig:%+v", op.UpConfig))
	}
	sb.WriteString("}")
	return sb.String()
}

type Shard struct {
	ConfigNum int
	KvMap     map[string]string
}

func (sd Shard) String() string {
	var KvMap_str strings.Builder
	for k, v := range sd.KvMap {
		if len(v) > 6 {
			v = v[0:2] + ".." + v[len(v)-2:]
		}
		KvMap_str.WriteString(k + ":" + v + " ")
	}
	return fmt.Sprintf("{ConfigNum:%d KvMap:[%s]}", sd.ConfigNum, KvMap_str.String())
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft // Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int                 // groupId
	ctrlers      []*labrpc.ClientEnd // ShardCtrlers
	maxraftstate int                 // snapshot if log grows this big
	// debugutils
	logger debugutils.Logger
	//
	dead int32
	//
	mck       *shardctrler.Clerk
	persister *raft.Persister
	//
	seqMap  map[int64]int             // mck.clientId -> mck.seqId
	chanMap map[int]chan CommandReply // index -> chan CommandReply
	//
	config     shardctrler.Config // now config
	lastConfig shardctrler.Config // last config
	shards     []Shard            // shardId -> Shard
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
	// 判断Shard
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	if kv.config.Shards[shardId] != kv.gid {
		kv.mu.Unlock()
		// 错误的Group
		reply.Err = ErrWrongGroup
		return
	}
	if kv.shards[shardId].KvMap == nil {
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
	}, GetTimeout)
	defer kv.logger.Debug("[Get][end]->[%d]:[%d], reply=%+v",
		args.ClientId, args.SeqId, reply)
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
		// defer语句在声明时就会记录相关变量的值，而不是在实际执行时获取
		// 使用func包一层即可
		kv.logger.Debug("[%s][end]->[%d]:[%d], reply=%+v",
			args.Type, args.ClientId, args.SeqId, reply)
	}()
	// 判断Shard
	// 根据Key分片
	shardId := key2shard(args.Key)
	kv.mu.Lock()
	// *****bug:
	// ShardNotArrive -> kv.shards[shardId].KvMap == nil
	// 什么时候更新KvMap？
	kv.logger.Debug("kv.config=%+v", kv.config)
	kv.logger.Debug("kv.shards[%d]=%+v", shardId, kv.shards[shardId])
	if kv.config.Shards[shardId] != kv.gid {
		kv.mu.Unlock()
		// 错误的Group
		reply.Err = ErrWrongGroup
		return
	}
	if kv.shards[shardId].KvMap == nil {
		// 注意，如果map == nil，那表明其未初始化
		// 如果len(map) == 0，那表明其为空，但是已经初始化，此时使用map == nil返回false
		// 但是（*）：作为stirng输出时，他们输出都是map[]
		kv.mu.Unlock()
		reply.Err = ShardNotArrive
		// kv.shards[shardId] = 何时更新？
		// 明明100-0.shards[0].ConfigNum=2, why .KvMap == nil?
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

// 判断是否是重复的
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	// 本方法不加锁，建议调用该方法时持有锁
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

// get wait chan at index
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
						kv.logger.Debug("not duplicate cmd=%+v", cmd)
						// 不是重复的指令
						kv.seqMap[cmd.ClientId] = cmd.SeqId
						switch cmd.Type {
						case GetOp:
							kv.logger.Debug("after get, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						case PutOp:
							kv.shards[shardId].KvMap[cmd.Key] = cmd.Value
							kv.logger.Debug("after put, kv.shards[%d]=%+v", shardId, kv.shards[shardId])
						case AppendOp:
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
			case UpdateConfig:
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
				kv.logger.Debug("is leader, send to chan[%d], reply=%+v", index, reply)
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

// read persist
func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
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
		kv.config = config
		kv.lastConfig = lastConfig
		kv.mu.Unlock()
	}
}

// addShardHandler
func (kv *ShardKV) addShardHandler(cmd Op) {
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
	curConfig := kv.config
	upConfig := cmd.UpConfig
	if curConfig.Num >= upConfig.Num {
		return
	}
	for shard, gid := range upConfig.Shards {
		if gid == kv.gid && curConfig.Shards[shard] == 0 {
			// 如果更新的配置的gid与当前的配置的gid一样且分片为0(未分配）
			kv.shards[shard].KvMap = make(map[string]string)
			kv.shards[shard].ConfigNum = upConfig.Num
		}
	}
	kv.lastConfig = curConfig
	kv.config = upConfig
	kv.logger.Debug("kv.lastConfig=%+v", kv.lastConfig)
	kv.logger.Debug("kv.config=%+v", kv.config)
	kv.logger.Debug("after UpdateShard, kv.shards[%d]=%+v",
		cmd.ShardId, kv.shards[cmd.ShardId])
}

// 判断kv不属于自己的Shard是否全部发送完毕
func (kv *ShardKV) allSent() bool {
	// 遍历上个配置的分片
	for shardId, gid := range kv.lastConfig.Shards {
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置号更小，说明还未发送
		if gid == kv.gid &&
			kv.config.Shards[shardId] != kv.gid &&
			kv.shards[shardId].ConfigNum < kv.config.Num {
			// 是本组的分片
			// 最新配置的分片组号不匹配
			// 记录的分片已经过时
			kv.logger.Debug("allSent=false, gid=%d, kv.gid=%d", gid, kv.gid)
			kv.logger.Debug("kv.config.Shards[%d]=%d", shardId, kv.config.Shards[shardId])
			kv.logger.Debug("kv.shards[%d].ConfigNum=[%d] < kv.config.Num=[%d]",
				kv.shards[shardId].ConfigNum, kv.config.Num)
			return false
		}
	}
	return true
}

// 判断kv属于自己的Shard是否全部接收完毕
func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.lastConfig.Shards {
		if gid != kv.gid &&
			kv.config.Shards[shard] == kv.gid &&
			kv.shards[shard].ConfigNum < kv.config.Num {
			return false
		}
	}
	return true
}

// 配置探测器（定时拉取）
func (kv *ShardKV) configDetector() {
	for !kv.killed() {
		time.Sleep(UpConfigLoopInterval)
		// 判断是否是Leader
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		switch {
		case !kv.allSent():
			kv.logger.Debug("not allSent!")
			// 没有全部发送完毕？
			// deep copy of map
			seqMap := make(map[int64]int)
			for k, v := range kv.seqMap {
				seqMap[k] = v
			}
			// 该ShardKV向组gid的所有ShardKV发送AddShard RPC
			for shardId, gid := range kv.lastConfig.Shards {
				// 判断该分片是否属于自己
				if gid == kv.gid &&
					kv.config.Shards[shardId] != kv.gid &&
					kv.shards[shardId].ConfigNum < kv.config.Num {
					// 该分片不属于自己，那么就发送给该属于的ShardKV
					shard := kv.cloneShard(kv.config.Num, kv.shards[shardId].KvMap)
					args := AddShardArgs{
						GroupId:     gid,
						ShardId:     shardId,
						Shard:       shard,
						LastApplied: seqMap,
						ConfigNum:   kv.config.Num,
					}
					// shardId -> gid -> server names: gid组下所有的ShardKV列表
					serverList := kv.config.Groups[kv.config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serverList))
					for i, name := range serverList {
						// server name -> server
						servers[i] = kv.make_end(name)
					}
					// kv.logger.Debug("kv.shards[shardId].KvMap=%+v", kv.shards[shardId].KvMap)
					kv.logger.Debug("->[%d], trySendAddShard RPC args=%+v", gid, args)
					// 对gid组的所有ShardKV发送这个自己不需要的分片
					go func(servers []*labrpc.ClientEnd, args *AddShardArgs) {
						for i := 0; ; i++ {
							reply := AddShardReply{}
							ok := servers[i%len(servers)].Call("ShardKV.AddShard", args, &reply)
							if ok {
								switch reply.Err {
								case OK:
									// 发送成功则删除自己的分片
									kv.startCommand(Op{
										ClientId: int64(kv.gid),
										SeqId:    kv.config.Num,
										Type:     RemoveShard,
										ShardId:  args.ShardId,
									}, RemoveShardTimeout) // 为什么不怕这步失败？
									return // 退出
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
			continue
		case !kv.allReceived():
			// 没有全部接收完毕
			kv.logger.Debug("not allReceived!")
			kv.mu.Unlock()
			continue
		default:
			// 现在问题：所有Leader ShardKV都认为自己收发完毕
			// 但实际上Follower ShardKV没有收到
			// 全部收发完毕
			curConfig := kv.config
			mck := kv.mck
			kv.mu.Unlock()
			newConfig := mck.Query(curConfig.Num + 1)
			kv.logger.Debug("allSent and allReceived!")
			kv.logger.Debug("kv.lastConfig=%+v", kv.lastConfig)
			kv.logger.Debug("kv.config=%+v", kv.config)
			for shardId, shard := range kv.shards {
				if shard.KvMap == nil {
					continue
				}
				kv.logger.Debug("kv.shards[%d]=%+v", shardId, shard)
			}
			kv.logger.Debug("--------------------------------------------------")
			if newConfig.Num != curConfig.Num+1 {
				time.Sleep(UpConfigLoopInterval)
				continue
			}
			// 调用Raft同步，更新Config
			kv.startCommand(Op{
				ClientId: int64(kv.gid), // groupId
				SeqId:    newConfig.Num, // configNum
				Type:     UpdateConfig,  // UpdateConfig
				UpConfig: newConfig,     // Config
			}, UpdateConfigTimeout)
		}
	}
}

func (kv *ShardKV) cloneShard(configNum int, kvMap map[string]string) Shard {
	migrateShard := Shard{
		ConfigNum: configNum,
		KvMap:     make(map[string]string),
	}
	for k, v := range kvMap {
		migrateShard.KvMap[k] = v
	}
	return migrateShard
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
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.logger = *debugutils.NewLogger(fmt.Sprintf("ShardKV %d-%d", kv.gid, kv.me), ShardKVDefaultLogLevel)

	kv.readPersist(persister.ReadSnapshot())

	kv.logger.Debug("success make ShardKV=%+v", kv)

	//
	go kv.applyMsgHandler()
	go kv.configDetector()

	return kv
}
