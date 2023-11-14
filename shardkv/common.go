package shardkv

import (
	"fmt"
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
	// log level
	ShardKVDefaultLogLevel = debugutils.Slient
	ClientDefaultLogLevel  = debugutils.Slient
	// Client wait time
	RequestWaitTime    = 100 * time.Millisecond
	WrongGroupWaitTime = 100 * time.Millisecond
	// Server config detector time interval
	UpConfigLoopInterval = 100 * time.Millisecond
	// Server some timeout
	GetTimeout          = 500 * time.Millisecond
	PutAppendTimeout    = 500 * time.Millisecond
	AddShardTimeout     = 500 * time.Millisecond
	RemoveShardTimeout  = 500 * time.Millisecond
	UpdateConfigTimeout = 500 * time.Millisecond
	// Server raftstate load factor
	RaftstateLoadFactor = 0.9
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
	Type     OpType // "Put" or "Append"
	Key      string
	Value    string
}

func (args *PutAppendArgs) String() string {
	key_str := args.Key
	if len(key_str) > 6 {
		key_str = key_str[:2] + ".." + key_str[len(key_str)-2:]
	}
	value_str := args.Value
	if len(value_str) > 6 {
		value_str = value_str[:2] + ".." + value_str[len(value_str)-2:]
	}
	return fmt.Sprintf("{ClientId:%d SeqId:%d Type:%s Key:%s Value:%s}",
		args.ClientId, args.SeqId, args.Type, key_str, value_str)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId int64
	SeqId    int
	Key      string
}

func (args *GetArgs) String() string {
	key_str := args.Key
	if len(key_str) > 6 {
		key_str = key_str[:2] + ".." + key_str[len(key_str)-2:]
	}
	return fmt.Sprintf("{ClientId:%d SeqId:%d Key:%s}",
		args.ClientId, args.SeqId, key_str)
}

type GetReply struct {
	Err   Err
	Value string
}

func (reply *GetReply) String() string {
	value_str := reply.Value
	if len(value_str) > 6 {
		value_str = value_str[:2] + ".." + value_str[len(value_str)-2:]
	}
	return fmt.Sprintf("{Err:%s Value:%s}", reply.Err, value_str)
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
