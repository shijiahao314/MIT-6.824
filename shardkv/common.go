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
	ShardKVDefaultLogLevel = debugutils.Slient
	ClientDefaultLogLevel  = debugutils.Slient
	//
	NoLeaderSleepTime  = 100 * time.Millisecond
	RequestWaitTime    = 100 * time.Millisecond
	WrongGroupWaitTime = 100 * time.Millisecond
	//
	UpConfigLoopInterval = 100 * time.Millisecond
	//
	GetTimeout         = 500 * time.Millisecond
	PutAppendTimeout   = 500 * time.Millisecond
	UpConfigTimeout    = 500 * time.Millisecond
	AddShardTimeout    = 500 * time.Millisecond
	RemoveShardTimeout = 500 * time.Millisecond
)

type Err string

const (
	OK                  = "OK"
	ErrAlreadyKilled    = "ErrAlreadyKilled"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ShardNotArrive      = "ShardNotArrive" // 分片还未到达
	ConfigNotArrive     = "ConfigNotArrive"
	ErrInconsistentData = "ErrInconsistentData"
	ErrRequestTimeOut   = "ErrRequestTimeOut" // request timeout

)

// OpType
type OpType string

const (
	GetOp    = "GetOp"
	PutOp    = "PutOp"
	AppendOp = "AppendOp"
	//
	AddShard     = "AddShard"
	RemoveShard  = "RemoveShard"
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

// ShardKV向对应ShardCtrler发送拉取配置文件请求以及回复
type AddShardArgs struct {
	GroupId     int
	ShardId     int
	Shard       Shard
	LastApplied map[int64]int
	ConfigNum   int
}

type AddShardReply struct {
	Err Err
}
