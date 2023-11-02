package kvraft

import (
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/debugutils"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	Type    OpType
	ID      int
	Version int
	Key     string
	Value   string
	Leader  int // only for newleader command
}

type OpReply struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// debugutils
	logger debugutils.Logger
	kvMap  map[string]string

	dupMap map[int]int // key: ID, value: version

	// 接收Op回调的注册表
	replyMap map[int]chan OpReply // key: index, value: opReply
	cmdMap   map[int]Op           // key: index, value: Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 判断kv的rf是否是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// not leader
		reply.Err = ErrWrongLeader
		return
	}
	kv.logger.Info("Get begin, args=%+v", args)
	// 发送指令
	cmd := Op{
		Type: GetOp,
		Key:  args.Key,
	}
	index, _, _ := kv.rf.Start(cmd)
	// 注册一个接收回调的chan
	kv.logger.Info("success make chan[%d]", index)
	kv.replyMap[index] = make(chan OpReply)
	ch := kv.replyMap[index] // 为了避免并发读写
	// 释放锁并监听
	kv.mu.Unlock()
	opReply := <-ch
	// 获得锁并处理
	kv.mu.Lock()
	// 关闭chan并删除在replyMap中的
	kv.logger.Info("success close chan[%d], opReply=%+v", index, opReply)
	reply.Err = opReply.Err
	reply.Value = opReply.Value
	kv.logger.Info("Get end, reply=%+v", reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 判断kv的rf是否是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		// not leader
		reply.Err = ErrWrongLeader
		return
	}
	kv.logger.Info("PutAppend begin, args=%+v", args)
	// 重复查询
	_, ok := kv.dupMap[args.ID]
	if !ok {
		// 未注册过，注册
		kv.dupMap[args.ID] = -1
	}
	if args.Version <= kv.dupMap[args.ID] {
		// 已经处理过了，快速回复
		reply.Err = OK
		return
	}
	// 发送指令
	cmd := Op{
		Type:    args.Op,
		ID:      args.ID,
		Version: args.Version,
		Key:     args.Key,
		Value:   args.Value,
	}
	index, _, _ := kv.rf.Start(cmd)
	// 注册一个接收回调的chan
	kv.logger.Info("success make chan[%d]", index)
	kv.replyMap[index] = make(chan OpReply)
	ch := kv.replyMap[index] // 为了避免并发读写
	kv.cmdMap[index] = cmd
	// 释放锁并监听
	kv.mu.Unlock()
	opReply := <-ch
	// 获得锁并处理
	kv.mu.Lock()
	// 删除在replyMap中的注册
	reply.Err = opReply.Err
	kv.logger.Info("PutAppend end, reply=%+v", reply)
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

func (kv *KVServer) closeChanDeleteKey(ch chan OpReply, index int) {
	// Only the sender should close a channel, never the receiver
	close(ch) // chan应该仅由发送方关闭（*）
	delete(kv.replyMap, index)
	delete(kv.cmdMap, index)
}

// 处理raft的apply
// kvserver(command) -> kv.rf ... after apply: -> kv.rf(command) -> kvserver
func (kv *KVServer) applyListener() {
	for !kv.killed() {
		// 从applyCh接收apply消息
		// 可以保证从applyCh接收的消息一定是多数认可的消息，且是有序的
		kv.logger.Debug("applyListener: listening")
		msg := <-kv.applyCh
		kv.logger.Debug("applyListener: receive apply message %+v", msg)
		kv.mu.Lock()
		switch {
		case msg.CommandValid:
			// command
			index := msg.CommandIndex
			cmd, ok := msg.Command.(Op)
			if !ok {
				panic("ERROR: Command cannot tranvert msg.Command to Op")
			}
			kv.logger.Debug("msg.CommandValid, cmd=%+v", cmd)
			switch cmd.Type {
			case GetOp:
				// Get
				value, ok := kv.kvMap[cmd.Key]
				reply := OpReply{}
				if !ok {
					// key not exists
					kv.logger.Error("Get: key=[%s] not exists", cmd.Key)
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
					reply.Value = value
				}
				kv.logger.Info("want to send kv.getReplyMap[%d]", index)
				// 查看index的chan是否存在
				if ch, ok := kv.replyMap[index]; ok {
					// 存在
					kv.logger.Debug("chan kv.getReplyMap[%d] exists", index)
					// 释放锁发送，完成后获得锁
					kv.mu.Unlock()
					ch <- reply
					kv.mu.Lock()
					kv.closeChanDeleteKey(ch, index)
				} else {
					kv.logger.Warn("chan kv.getReplyMap[%d] not exists", index)
				}
				kv.logger.Info("success send kv.getReplyMap[%d]", index)
			case PutOp, AppendOp:
				// 首先检查是否是重复的
				_, ok := kv.dupMap[cmd.ID]
				if !ok {
					kv.dupMap[cmd.ID] = -1
				}
				if cmd.Version <= kv.dupMap[cmd.ID] {
					// 接收到重复的（旧的）cmd
					kv.logger.Info("applyListener: cmd.Version <= kv.dupMap[cmd.ID]")
					ch, ok := kv.replyMap[index]
					oldCmd := kv.cmdMap[index]
					if ok {
						if cmd != oldCmd {
							// 矛盾了？
							for index, ch := range kv.replyMap {
								ch <- OpReply{
									Err: ErrWrongLeader,
								}
								kv.closeChanDeleteKey(ch, index)
							}
						} else {
							kv.replyMap[index] <- OpReply{
								Err: OK,
							}
							kv.closeChanDeleteKey(ch, index)
						}
					} else {
						for index, ch := range kv.replyMap {
							ch <- OpReply{
								Err: ErrWrongLeader,
							}
							kv.closeChanDeleteKey(ch, index)
						}
					}
					kv.mu.Unlock()
					continue
				}
				kv.dupMap[cmd.ID] = cmd.Version
				switch cmd.Type {
				case PutOp:
					// Put
					kv.kvMap[cmd.Key] = cmd.Value
					kv.logger.Info("Put: key=[%s] value=[%s]", cmd.Key, cmd.Value)
				case AppendOp:
					// Append
					kv.kvMap[cmd.Key] = kv.kvMap[cmd.Key] + cmd.Value
					kv.logger.Info("Append: key=[%s] value=[%s], after append value=[%s]",
						cmd.Key, cmd.Value, kv.kvMap[cmd.Key])
				}
				reply := OpReply{
					Err: OK,
				}
				kv.logger.Info("want to send kv.putAppendReplyMap[%d]", index)
				// 查看index的chan是否存在
				if ch, ok := kv.replyMap[index]; ok {
					// 存在
					kv.logger.Debug("chan kv.putAppendReplyMap[%d] exists", index)
					// 释放锁发送，完成后获得锁
					kv.mu.Unlock()
					ch <- reply
					kv.mu.Lock()
					kv.closeChanDeleteKey(ch, index)
				} else {
					kv.logger.Warn("chan kv.putAppendReplyMap[%d] not exists", index)
				}
				kv.logger.Info("success send kv.putAppendReplyMap[%d]", index)
			case NewLeader:
				// 判断是否是leader对应kvserver
				if cmd.Leader == kv.me {
					kv.mu.Unlock()
					continue
				}
				// 丢弃所有chanMap
				for index, ch := range kv.replyMap {
					kv.logger.Info("wanto to delete kv.replyMap[%d]", index)
					ch <- OpReply{
						Err: ErrWrongLeader,
					}
					kv.closeChanDeleteKey(ch, index)
				}
			default:
				//
				panic("Unknown op type")
			}
		case msg.SnapshotValid:
			// TODO snapshot
			kv.logger.Error("TODO")
		default:
			// 既不是CommandValid，也不是SnapshotValid
			// 新的leader产生
			kv.logger.Info("new leader appear")
			kv.rf.Start(Op{
				Type:   NewLeader,
				Leader: kv.me,
			})
		}
		kv.mu.Unlock()
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.logger = *debugutils.NewLogger(fmt.Sprintf("KVServer %d", kv.me), debugutils.Slient)
	kv.kvMap = make(map[string]string)
	kv.dupMap = make(map[int]int)

	kv.replyMap = make(map[int]chan OpReply)
	kv.cmdMap = make(map[int]Op)

	kv.logger.Debug("success make kv=%+v", kv)

	go kv.applyListener()

	return kv
}
