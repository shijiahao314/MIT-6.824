package raft

import (
	"fmt"
	"strconv"
	"time"

	"6.5840/debugutils"
)

// some configs
const (
	heartbeatInterval   = 50 * time.Millisecond // 心跳间隔
	heartbeatTimeoutMin = 150                   // 心跳超时min
	heartbeatTimeoutMax = 500                   // 心跳超时max

	// default log level
	RaftDefaultLogLevel        = debugutils.Slient
	ApplyHelperDefaultLogLevel = debugutils.Slient
)

// raft state
type RaftState int

const (
	Follower = iota
	Candidate
	Leader
)

// 1.request vote
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// 2.append entries
type AppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	Success  bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	Conflict bool
	XTerm    int // term in the conflicting entry (if any)
	XIndex   int // 期待leader发送的下一个Index
}

// 2.1 apply msg
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	Command      interface{}

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (msg ApplyMsg) String() string {
	command := msg.Command
	if value, ok := msg.Command.(int); ok {
		str := strconv.Itoa(value)
		if len(str) > 6 {
			command = fmt.Sprintf("%.4s..", str)
		} else {
			command = value
		}
	} else if value, ok := msg.Command.(string); ok {
		if len(value) > 6 {
			command = fmt.Sprintf("%.4s..", value)
		} else {
			command = value
		}
	}
	return fmt.Sprintf("{CommandValid=%t CommandIndex=%d Command=%v SnapshotValid=%t len(Snapshot)=%d SnapshotTerm=%d SnapshotIndex=%d}",
		msg.CommandValid, msg.CommandIndex, command, msg.SnapshotValid, len(msg.Snapshot), msg.SnapshotTerm, msg.SnapshotIndex)
}

// 3.install snapshot
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("{Term=%d LeaderId=%d LastIncludedIndex=%d LastIncludedTerm=%d}",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.Term)
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}
