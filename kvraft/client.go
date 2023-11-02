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
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	ID         int
	logger     debugutils.Logger
	lastServer int // 记录上一个可用的server
	version    int // 版本号
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
	ck.ID = ID
	ID++
	idMu.Unlock()
	ck.logger = *debugutils.NewLogger(fmt.Sprintf("Clerk %d", ck.ID), debugutils.Slient)
	ck.lastServer = 0
	ck.version = 0

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
		Key: key,
	}
	ck.logger.Info("Get RPC args=%+v", args)
	// 不断依次向所有服务器发送请求
	for i := 0; ; i++ {
		server := (i + ck.lastServer) % len(ck.servers)
		reply := GetReply{}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		switch reply.Err {
		case OK:
			ck.logger.Info("OK\n")
			ck.lastServer = server
			return reply.Value
		case ErrWrongLeader:
			if i != 0 && i%len(ck.servers) == 0 {
				// 已经查询完整的一轮，均无可应答KVServer
				time.Sleep(NotLeaderSleepTime)
			}
			continue
		case ErrNoKey:
			ck.logger.Info("ErrNoKey\n")
			ck.lastServer = server
			return reply.Value
		default:
			panic("Unknown reply.Err")
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
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var opType OpType
	switch op {
	case "Put":
		opType = PutOp
	case "Append":
		opType = AppendOp
	default:
		panic("Unknown op type")
	}
	args := PutAppendArgs{
		Version: ck.version,
		ID:      ck.ID,
		Op:      opType,
		Key:     key,
		Value:   value,
	}
	ck.version++
	ck.logger.Info("PutAppend RPC args=%+v", args)
	// 不断依次向所有服务器发送请求
	// 这里Clerk发送的请求只能串行化访问，
	// 即必须等待leader server回应（心跳间隔时间）后才能发送下一个请求
	// 可以让leader server快速回应，默认认为leader顺利执行
	for i := 0; ; i++ {
		server := (i + ck.lastServer) % len(ck.servers)
		reply := PutAppendReply{}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		switch reply.Err {
		case OK:
			ck.logger.Info("OK\n")
			ck.lastServer = server
			return
		case ErrWrongLeader:
			if i != 0 && i%len(ck.servers) == 0 {
				// 已经查询完整的一轮，均无可应答KVServer
				time.Sleep(NotLeaderSleepTime)
			}
			continue
		case ErrNoKey:
			ck.logger.Info("ErrNoKey\n")
			ck.lastServer = server
			return
		default:
			panic("Unknown reply.Err")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
