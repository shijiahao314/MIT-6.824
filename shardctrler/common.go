package shardctrler

import (
	"time"

	"6.5840/debugutils"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// some configs
const (
	NoLeaderSleepTime = 100 * time.Millisecond
	RequestWaitTime   = 100 * time.Millisecond
	// default log level
	ShardCtrlerDefaultLogLevel = debugutils.DebugLevel
	ClientDefaultLogLevel      = debugutils.Slient
)

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type OpType string

const (
	JoinOp  = "JoinOp"
	LeaveOp = "LeaveOp"
	MoveOp  = "MoveOp"
	QueryOp = "QueryOp"
)

// Err
type Err string

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

// Base
type BaseArgs struct {
	ClientId int
	SeqId    int
}

type BaseReply struct {
	WrongLeader bool
	Err         Err
}

// Join
type JoinArgs struct {
	BaseArgs BaseArgs
	Servers  map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	BaseReply BaseReply
}

// Leave
type LeaveArgs struct {
	BaseArgs BaseArgs
	GIDs     []int
}

type LeaveReply struct {
	BaseReply BaseReply
}

// Move
type MoveArgs struct {
	BaseArgs BaseArgs
	Shard    int
	GID      int
}

type MoveReply struct {
	BaseReply BaseReply
}

// Query
type QueryArgs struct {
	BaseArgs BaseArgs
	Num      int // desired config number
}

type QueryReply struct {
	BaseReply BaseReply
	Config    Config
}
