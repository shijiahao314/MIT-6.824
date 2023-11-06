package kvraft

import (
	"fmt"
	"time"

	"6.5840/debugutils"
)

// some configs
const (
	NoLeaderSleepTime   = 100 * time.Millisecond
	RaftstateLoadFactor = 0.9 // kv.persister.RaftStateSize() should <= maxraftstate * RaftstateLoadFactor
	RequestWaitTime     = 100 * time.Millisecond
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
