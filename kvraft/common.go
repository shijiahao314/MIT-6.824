package kvraft

import "time"

// time
const (
	NotLeaderSleepTime = 100 * time.Millisecond
)

type OpType int

const (
	GetOp = iota
	PutOp
	AppendOp
	NewLeader
)

// Err
type Err string

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNoKey       = "ErrNoKey"
)

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Version int    // ck.Version
	ID      int    // ck.ID
	Op      OpType // "Put" or "Append"
	Key     string
	Value   string
}

type PutAppendReply struct {
	Err Err
}
