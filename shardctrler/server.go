package shardctrler

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/debugutils"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead int32

	// debugutils
	logger debugutils.Logger

	//
	configs []Config             // indexed by config num
	seqMap  map[int]int          // key: ck.clientId, value: ck.seqId
	chanMap map[int]chan OpReply // key: index, value: CommandReply
}

func (sc *ShardCtrler) String() string {
	return fmt.Sprintf("{me=%d}",
		sc.me)
}

type Op struct {
	ClientId int
	SeqId    int
	Type     OpType
	// JoinOp
	Servers map[int][]string
	// LeaveOp
	GIDs []int
	// MoveOp
	Shard int
	GID   int
	// QueryOp
	Num int
}

type OpReply struct {
	ClientId int
	SeqId    int
	// Only for QueryReply
	Config Config
}

// 判断是否是重复的
func (sc *ShardCtrler) ifDuplicate(clientId, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastSeqId, exists := sc.seqMap[clientId]
	if !exists {
		return false
	}
	return seqId <= lastSeqId
}

func (sc *ShardCtrler) closeAndDelete(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 为避免panic: send on closed channel，不手动关闭通道
	// close(sc.chanMap[index]) // chan应该仅由发送方关闭（*）
	delete(sc.chanMap, index)
}

func (sc *ShardCtrler) applyMsgHandler() {
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.logger.Debug("applier: receive apply message %+v", msg)
		switch {
		case msg.CommandValid:
			index := msg.CommandIndex
			op, ok := msg.Command.(Op)
			if !ok {
				panic("ERROR: cannot tranvert msg.Command to Op")
			}
			opReply := OpReply{
				ClientId: op.ClientId,
				SeqId:    op.SeqId,
			}
			if !sc.ifDuplicate(op.ClientId, op.SeqId) {
				sc.logger.Debug("not duplicate cmd[%d]=%+v", index, op)
				// 不是重复的指令
				sc.mu.Lock()
				switch op.Type {
				case JoinOp:
					sc.execJoin(op)
				case LeaveOp:
					sc.execLeave(op)
				case MoveOp:
					sc.execMove(op)
				case QueryOp:
					opReply.Config = sc.execQuery(op)
				default:
					fmt.Printf("cmd=%+v\n", op)
					panic("Unknown cmd.Type")
				}
				sc.seqMap[op.ClientId] = op.SeqId
				sc.mu.Unlock()
			}
			// 发送消息（考虑中途变更leader情况）
			if _, isLeader := sc.rf.GetState(); isLeader {
				sc.logger.Debug("is leader, send to chan[%d], reply=%+v", index, opReply)
				sc.getWaitCh(index) <- opReply
			}
		case msg.SnapshotValid:
			// 不需要快照
		default:
			fmt.Printf("msg=%+v\n", msg)
			panic("Unknown msg type")
		}
	}
}

// preCheck
func (sc *ShardCtrler) preCheck(args *BaseArgs, reply *BaseReply, op string, argsMap map[any]any) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, isLeader := sc.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return false
	}
	var sb strings.Builder
	for k, v := range argsMap {
		sb.WriteString(fmt.Sprintf("%s=%+v ", k, v))
	}
	sc.logger.Debug("[RPC][begin][%s]<-[%d]:[%d], args={%s}",
		op, args.ClientId, args.SeqId, sb.String())
	return true
}

// postCheck
func (sc *ShardCtrler) postCheck(args *BaseArgs, reply *BaseReply, op string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.Err = OK
	sc.logger.Debug("[RPC][end][%s]->[%d]:[%d], reply=%+v", op, args.ClientId, args.SeqId, reply)
}

func (sc *ShardCtrler) getWaitCh(index int) chan OpReply {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.chanMap[index]
	if !ok {
		// （*）这里设置一个有1缓冲的，否则死锁
		sc.chanMap[index] = make(chan OpReply, 1) // 内存泄漏？不会，因为会关闭通道
		ch = sc.chanMap[index]
	}
	return ch
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	argsMap := map[any]any{
		"Servers": args.Servers,
	}
	if !sc.preCheck(&args.BaseArgs, &reply.BaseReply, JoinOp, argsMap) {
		return
	}
	// 发射指令
	op := Op{
		ClientId: args.BaseArgs.ClientId,
		SeqId:    args.BaseArgs.SeqId,
		Type:     JoinOp,
		Servers:  args.Servers,
	}
	index, _, _ := sc.rf.Start(op)
	// 结果回调channel
	ch := sc.getWaitCh(index)
	defer sc.closeAndDelete(index) // 超时或完成后关闭通道，防止内存泄漏
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听
	select {
	case opReply := <-ch:
		if op.ClientId != opReply.ClientId || op.SeqId != opReply.SeqId {
			reply.BaseReply.WrongLeader = true
			reply.BaseReply.Err = ErrWrongLeader
		} else {
			reply.BaseReply.WrongLeader = false
			reply.BaseReply.Err = OK
		}
	case <-timer.C:
		reply.BaseReply.WrongLeader = true
		reply.BaseReply.Err = ErrWrongLeader
	}
	sc.postCheck(&args.BaseArgs, &reply.BaseReply, JoinOp)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	argsMap := map[any]any{
		"GIDs": args.GIDs,
	}
	if !sc.preCheck(&args.BaseArgs, &reply.BaseReply, LeaveOp, argsMap) {
		return
	}
	// 发射指令
	op := Op{
		ClientId: args.BaseArgs.ClientId,
		SeqId:    args.BaseArgs.SeqId,
		Type:     LeaveOp,
		GIDs:     args.GIDs,
	}
	index, _, _ := sc.rf.Start(op)
	// 结果回调channel
	ch := sc.getWaitCh(index)
	defer sc.closeAndDelete(index) // 超时或完成后关闭通道，防止内存泄漏
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听
	select {
	case opReply := <-ch:
		if op.ClientId != opReply.ClientId || op.SeqId != opReply.SeqId {
			reply.BaseReply.WrongLeader = true
			reply.BaseReply.Err = ErrWrongLeader
		} else {
			reply.BaseReply.WrongLeader = false
			reply.BaseReply.Err = OK
		}
	case <-timer.C:
		reply.BaseReply.WrongLeader = true
		reply.BaseReply.Err = ErrWrongLeader
	}
	sc.postCheck(&args.BaseArgs, &reply.BaseReply, LeaveOp)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	argsMap := map[any]any{
		"Shard": args.Shard,
		"GID":   args.GID,
	}
	if !sc.preCheck(&args.BaseArgs, &reply.BaseReply, MoveOp, argsMap) {
		return
	}
	// 发射指令
	op := Op{
		ClientId: args.BaseArgs.ClientId,
		SeqId:    args.BaseArgs.SeqId,
		Type:     MoveOp,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	index, _, _ := sc.rf.Start(op)
	// 结果回调channel
	ch := sc.getWaitCh(index)
	defer sc.closeAndDelete(index) // 超时或完成后关闭通道，防止内存泄漏
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听
	select {
	case opReply := <-ch:
		if op.ClientId != opReply.ClientId || op.SeqId != opReply.SeqId {
			reply.BaseReply.WrongLeader = true
			reply.BaseReply.Err = ErrWrongLeader
		} else {
			reply.BaseReply.WrongLeader = false
			reply.BaseReply.Err = OK
		}
	case <-timer.C:
		reply.BaseReply.WrongLeader = true
		reply.BaseReply.Err = ErrWrongLeader
	}
	sc.postCheck(&args.BaseArgs, &reply.BaseReply, MoveOp)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	argsMap := map[any]any{
		"Num": args.Num,
	}
	if !sc.preCheck(&args.BaseArgs, &reply.BaseReply, QueryOp, argsMap) {
		return
	}
	// 发射指令
	op := Op{
		ClientId: args.BaseArgs.ClientId,
		SeqId:    args.BaseArgs.SeqId,
		Type:     QueryOp,
		Num:      args.Num,
	}
	index, _, _ := sc.rf.Start(op)
	// 结果回调channel
	ch := sc.getWaitCh(index)
	defer sc.closeAndDelete(index) // 超时或完成后关闭通道，防止内存泄漏
	// 设置超时Timer
	timer := time.NewTicker(RequestWaitTime)
	defer timer.Stop()
	// 监听
	select {
	case opReply := <-ch:
		if op.ClientId != opReply.ClientId || op.SeqId != opReply.SeqId {
			reply.BaseReply.WrongLeader = true
			reply.BaseReply.Err = ErrWrongLeader
		} else {
			reply.BaseReply.WrongLeader = false
			reply.BaseReply.Err = OK
			reply.Config = opReply.Config
		}
	case <-timer.C:
		reply.BaseReply.WrongLeader = true
		reply.BaseReply.Err = ErrWrongLeader
	}
	sc.logger.Debug("reply.Config=%+v", reply.Config)
	sc.postCheck(&args.BaseArgs, &reply.BaseReply, QueryOp)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	//
	sc.chanMap = make(map[int]chan OpReply)
	sc.seqMap = make(map[int]int)
	sc.logger = *debugutils.NewLogger(fmt.Sprintf("ShardCtrler %d", sc.me), ShardCtrlerDefaultLogLevel)

	sc.logger.Debug("success make sc=%+v", sc)

	go sc.applyMsgHandler()

	return sc
}
