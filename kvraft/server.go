package kvraft

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
)

type Command struct {
	ClientId int
	SeqId    int
	Type     OpType
	Key      string
	Value    string
	Leader   int // only for newleader command
}

type CommandReply struct {
	ClientId int
	SeqId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister

	// Your definitions here.
	// debugutils
	logger debugutils.Logger
	// maps
	kvMap   map[string]string         // state machine
	seqMap  map[int]int               // key: ck.clientId, value: ck.seqId
	chanMap map[int]chan CommandReply // key: index, value: CommandReply
	// cmdMap  map[int]Command           // key: index, value: Command

	lastIncludedIndex int
}

func (kv *KVServer) String() string {
	return fmt.Sprintf("{me=%d maxraftstate=%d}", kv.me, kv.maxraftstate)
}

func (kv *KVServer) getWaitCh(index int) chan CommandReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.chanMap[index]
	if !ok {
		kv.chanMap[index] = make(chan CommandReply)
		ch = kv.chanMap[index]
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 判断是否killed
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// 判断kv的rf是否是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logger.Debug("Get begin, args=%+v", args)
	// 发送指令
	cmd := Command{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     GetOp,
		Key:      args.Key,
	}
	index, _, _ := kv.rf.Start(cmd)
	// 结果回调channel
	ch := kv.getWaitCh(index)
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听
	select {
	case opReply := <-ch:
		if cmd.ClientId != opReply.ClientId || cmd.SeqId != opReply.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
	kv.logger.Debug("Get end, reply=%+v", reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// 判断是否killed
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	// 判断kv的rf是否是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.logger.Debug("%s begin, args=%+v", args.Op, args)
	// 发送指令
	cmd := Command{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, _, _ := kv.rf.Start(cmd)
	// 结果回调channel
	ch := kv.getWaitCh(index)
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听
	select {
	case opReply := <-ch:
		if cmd.ClientId != opReply.ClientId || cmd.SeqId != opReply.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
	kv.logger.Debug("PutAppend end, reply=%+v", reply)
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

// 判断是否是重复的
func (kv *KVServer) ifDuplicate(clientId, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastSeqId, exists := kv.seqMap[clientId]
	if !exists {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) closeChanDeleteKey(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.chanMap[index]) // chan应该仅由发送方关闭（*）
	delete(kv.chanMap, index)
}

// 处理raft的apply
// kvserver(command) -> kv.rf ... after apply: -> kv.rf(command) -> kvserver
func (kv *KVServer) applyMsgHandler() {
	for !kv.killed() {
		// 从applyCh接收apply消息
		// 可以保证从applyCh接收的消息一定是多数认可的消息，且是有序的
		msg := <-kv.applyCh
		// kv.logger.Debug("applier: receive apply message %+v", msg)
		switch {
		case msg.CommandValid:
			// CommandValid
			index := msg.CommandIndex
			cmd, ok := msg.Command.(Command)
			if !ok {
				panic("ERROR: cannot tranvert msg.Command to Command")
			}
			if !kv.ifDuplicate(cmd.ClientId, cmd.SeqId) {
				// 不是重复的指令
				kv.mu.Lock()
				switch cmd.Type {
				case GetOp:
				case PutOp:
					kv.kvMap[cmd.Key] = cmd.Value
				case AppendOp:
					kv.kvMap[cmd.Key] += cmd.Value
				default:
					fmt.Printf("cmd=%+v\n", cmd)
					panic("Unknown cmd.Type")
				}
				kv.seqMap[cmd.ClientId] = cmd.SeqId
				// 判断是否需要快照
				if kv.maxraftstate != -1 &&
					float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*RaftstateLoadFactor {
					kv.makeSnapshot(index)
				}
				kv.mu.Unlock()
			}
			// 发送消息（考虑中途变更leader情况）
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.getWaitCh(index) <- CommandReply{
					ClientId: cmd.ClientId,
					SeqId:    cmd.SeqId,
				}
				kv.closeChanDeleteKey(index)
			}
		case msg.SnapshotValid:
			kv.readPersist(msg.Snapshot)
		default:
			fmt.Printf("msg=%+v\n", msg)
			panic("Unknown msg type")
		}
	}
}

func (kv *KVServer) makeSnapshot(index int) {
	// 本方法不加锁，建议调用该方法时持有锁
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvMap map[string]string
	var seqMap map[int]int
	if d.Decode(&kvMap) != nil ||
		d.Decode(&seqMap) != nil {
		kv.logger.Error("cannot readPersist")
	} else {
		kv.mu.Lock()
		kv.kvMap = kvMap
		kv.seqMap = seqMap
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
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister

	// You may need initialization code here.
	kv.logger = *debugutils.NewLogger(fmt.Sprintf("KVServer %d", kv.me), debugutils.Slient)
	kv.kvMap = make(map[string]string)
	kv.seqMap = make(map[int]int)
	kv.chanMap = make(map[int]chan CommandReply)
	// kv.cmdMap = make(map[int]Command)
	kv.lastIncludedIndex = kv.rf.GetLastIncludedIndex()

	kv.readPersist(persister.ReadSnapshot())

	kv.logger.Debug("success make kv=%+v", kv)

	go kv.applyMsgHandler()

	return kv
}
