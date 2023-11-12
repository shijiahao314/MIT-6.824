package shardkv

import (
	"time"

	"6.5840/debugutils"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

// some configs
const (
	ShardKVDefaultLogLevel = debugutils.DebugLevel
	ClientDefaultLogLevel  = debugutils.DebugLevel
	//
	NoLeaderSleepTime = 100 * time.Millisecond
	RequestWaitTime   = 100 * time.Millisecond
)

const (
	OK                  = "OK"
	ErrAlreadyKilled    = "ErrAlreadyKilled"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ShardNotArrive      = "ShardNotArrive" // 分片还未到达
	ErrInconsistentData = "ErrInconsistentData"
	ErrRequestTimeOut   = "ErrRequestTimeOut" // request timeout
)

type Err string

// OpType
type OpType string

const (
	GetOp        = "GetOp"
	PutOp        = "PutOp"
	AppendOp     = "AppendOp"
	UpdateConfig = "UpdateConfig"
)

// Put or Append
type PutAppendArgs struct {
	ClientId int64
	SeqId    int
	Op       OpType // "Put" or "Append"
	Key      string
	Value    string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId int64
	SeqId    int
	Key      string
}

type GetReply struct {
	Err   Err
	Value string
}

type CommandReply struct {
	ClientId int64
	SeqId    int
	Err      Err
}
