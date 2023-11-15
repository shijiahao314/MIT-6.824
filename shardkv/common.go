package shardkv

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"6.5840/debugutils"
	"6.5840/shardctrler"
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
	AddShard    = "AddShard"
	RemoveShard = "RemoveShard"
	UpdataShard = "UpdataShard"
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
	LastApplied map[int64]int // clientId -> seqId
	ConfigNum   int
}

type AddShardReply struct {
	Err Err
}

// models
type Op struct {
	ClientId int64
	SeqId    int
	Type     OpType
	Key      string             // for: GetOp, PutOp, AppendOp
	Value    string             // for: PutOp, AppendOp
	ShardId  int                // for: AddShard, RemoveShard, UpdateConfig
	Shard    Shard              // for: AddShard
	SeqMap   map[int64]int      // for: AddShard
	UpConfig shardctrler.Config // for: UpdateConfig
}

func (op Op) String() string {
	var sb strings.Builder
	sb.WriteString("{")
	sb.WriteString(fmt.Sprintf("ClientId:%d ", op.ClientId))
	sb.WriteString(fmt.Sprintf("SeqId:%d ", op.SeqId))
	sb.WriteString(fmt.Sprintf("Type:%s ", op.Type))
	switch op.Type {
	case GetOp:
		sb.WriteString(fmt.Sprintf("Key:%s", op.Key))
	case PutOp:
		sb.WriteString(fmt.Sprintf("Key:%s ", op.Key))
		sb.WriteString(fmt.Sprintf("Value:%s", op.Value))
	case AppendOp:
		sb.WriteString(fmt.Sprintf("Key:%s ", op.Key))
		sb.WriteString(fmt.Sprintf("Value:%s", op.Value))
	case AddShard:
		sb.WriteString(fmt.Sprintf("ShardId:%d ", op.ShardId))
		sb.WriteString(fmt.Sprintf("Shard:%+v ", op.Shard))
		sb.WriteString(fmt.Sprintf("SeqMap:%+v", op.SeqMap))
	case RemoveShard:
		sb.WriteString(fmt.Sprintf("ShardId:%d", op.ShardId))
	case UpdataShard:
		sb.WriteString(fmt.Sprintf("ShardId:%d ", op.ShardId))
		sb.WriteString(fmt.Sprintf("UpConfig:%+v", op.UpConfig))
	}
	sb.WriteString("}")
	return sb.String()
}

type Shard struct {
	ConfigNum int
	KvMap     map[string]string
}

var smu sync.Mutex

func (sd Shard) String() string {
	smu.Lock()
	defer smu.Unlock()
	var KvMap_str strings.Builder
	for k, v := range sd.KvMap {
		length := len(v)
		if len(v) > 6 {
			v = v[0:2] + ".." + v[len(v)-2:]
		}
		KvMap_str.WriteString(k + ":" + v + "(len=" + strconv.Itoa(length) + ") ")
	}
	return fmt.Sprintf("{ConfigNum:%d KvMap:[%s]}", sd.ConfigNum, KvMap_str.String())
}
