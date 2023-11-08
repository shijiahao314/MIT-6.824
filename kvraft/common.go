package kvraft

import (
	"fmt"
	"time"

	"6.5840/debugutils"
)

// some configs
const (
	// time
	NoLeaderSleepTime = 100 * time.Millisecond
	RequestWaitTime   = 100 * time.Millisecond
	// kv.persister.RaftStateSize() should <= maxraftstate * RaftstateLoadFactor
	RaftstateLoadFactor = 0.9
	// default loglevel: Slient DebugLevel
	ServerDefaultLogLevel = debugutils.Slient
	ClientDefaultLogLevel = debugutils.Slient
)

// Err
type Err string

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoKey       = "ErrNoKey"
)

// OpType
type OpType string

const (
	GetOp     = "GetOp"
	PutOp     = "PutOp"
	AppendOp  = "AppendOp"
	NewLeader = "NewLeader"
)

// Get
type GetArgs struct {
	ClientId int // ck.clientId
	SeqId    int // 当前ck发送的第i个请求
	Key      string
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply *GetReply) String() string {
	return fmt.Sprintf("{Err=%s Value=[%.10s]..}", reply.Err, reply.Value)
}

// PutAppend
type PutAppendArgs struct {
	ClientId int    // ck.clientId
	SeqId    int    // 当前ck发送的第i个请求
	Op       OpType // "Put" or "Append"
	Key      string
	Value    string
}

type PutAppendReply struct {
	Err Err
}
