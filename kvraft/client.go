package kvraft

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
	leaderId int // 记录上一个可用的server
	// debugutils
	logger debugutils.Logger
}

func (ck *Clerk) String() string {
	return fmt.Sprintf("{clientId=%d  seqId=%d leaderId=%d}", ck.clientId, ck.seqId, ck.leaderId)
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
	// You'll have to add code here.
	idMu.Lock()
	ck.clientId = ID
	ID++
	idMu.Unlock()
	ck.seqId = 0
	ck.leaderId = 0
	ck.logger = *debugutils.NewLogger(fmt.Sprintf("Clerk %d", ck.clientId), debugutils.Slient)

	ck.logger.Debug("success make ck=%+v", ck)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		Key:      key,
	}
	ck.seqId++
	ck.logger.Info("Get RPC args=%+v", args)
	// 不断依次向所有服务器发送请求
	for i := 0; ; i++ {
		server := (ck.leaderId + i) % len(ck.servers)
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.leaderId = server
				return reply.Value
			case ErrWrongLeader:
				if i != 0 && i%len(ck.servers) == 0 {
					// 已经查询完整的一轮，均无可应答KVServer
					time.Sleep(NoLeaderSleepTime)
				}
				continue
			case ErrNoKey:
				ck.leaderId = server
				return reply.Value
			default:
				panic("Unknown reply.Err")
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	args := PutAppendArgs{
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
		Op:       op,
		Key:      key,
		Value:    value,
	}
	ck.seqId++
	ck.logger.Info("PutAppend RPC args=%+v", args)
	// 不断依次向所有服务器发送请求
	for i := 0; ; i++ {
		server := (ck.leaderId + i) % len(ck.servers)
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.leaderId = server
				return
			case ErrWrongLeader:
				if i != 0 && i%len(ck.servers) == 0 {
					// 已经查询完整的一轮，均无可应答KVServer
					time.Sleep(NoLeaderSleepTime)
				}
				continue
			case ErrNoKey:
				ck.leaderId = server
				return
			default:
				panic("Unknown reply.Err")
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
