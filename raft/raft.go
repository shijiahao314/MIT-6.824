package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840lab2/labgob"
	"6.5840lab2/labrpc"

	// debug tools
	"6.5840lab2/debugutils"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower = iota
	Candidate
	Leader
)

const (
	heartbeatInterval   = 50 * time.Millisecond // 心跳间隔
	heartbeatTimeoutMin = 150
	heartbeatTimeoutMax = 500
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 日志
	logger debugutils.Logger

	// Persistent state on all servers (Updated on stable storage before responding to RPCs)
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)
	log         Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// 选举相关
	state         State
	r             *rand.Rand
	leaderTimeout time.Time // leader过期时间

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// apply
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// snapshot, also persistent state on all servers
	lastIncludedIndex int // index of snapshot (initialized to 0)
	lastIncludedTerm  int // term of snapshot (initialized to -1)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist(nil)
	}
}

func (rf *Raft) setLeaderTimeout() {
	heartbeatTimeout := time.Duration(heartbeatTimeoutMin+rf.r.Intn(heartbeatTimeoutMax-heartbeatTimeoutMin)) * time.Millisecond
	rf.leaderTimeout = time.Now().Add(heartbeatTimeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	if snapshot == nil {
		rf.logger.Info("rf.persist(): save raftstate")
	} else {
		rf.logger.Info("rf.persist(): save raftstate and snapshot")
	}
	// encode raftstate
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	// save raftstate and snapshot
	rf.logger.Warn("rf.persister.Save(raftstate, snapshot)")
	rf.persister.Save(raftstate, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// read raftstate from rf.persister
	rf.logger.Info("rf.readPersist(): read raftstate")
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	var lastIncludedIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		rf.logger.Error("rf.readPersist(): failed to read raftstate")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludeTerm
	}
}

// 进行同步
func (rf *Raft) apply() {
	// 唤醒所有applyCond.Wait()的goroutine
	rf.logger.Info("rf.apply(): rf.applyCond.Broadcast(), rf.commitIndex=[%d]", rf.commitIndex)
	// rf.applyCond.Broadcast()获得rf.mu
	rf.applyCond.Broadcast()
}

// 用于向客户端apply的goroutine
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log.lastLog().Index {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.get(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			rf.logger.Info(" -> [Client], want to apply command %v", applyMsg)
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			rf.logger.Info("apply command success")
		} else {
			rf.logger.Info("rf.applyCond.Wait(), rf.commitIndex=[%d], rf.lastApplied=[%d]",
				rf.commitIndex, rf.lastApplied)
			// rf.applyCond.Wait()释放rf.mu
			rf.applyCond.Wait()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// If command received from client: append entry to local log,
	// respond after entry applied to state machine
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	if !isLeader {
		// not leader
		return index, term, isLeader
	}
	index = rf.nextIndex[rf.me]
	rf.log.append(Entry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: command,
	})
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.persist(nil)
	rf.logger.Info("<- [Client], receive command %d", command)
	rf.logger.Info("after receive, rf.log=%v", rf.log)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
		if rf.state == Leader {
			go rf.leaderCheckSendAppendEntries(true)
		} else {
			// not leader
			// heartbeatTimeout: heartbeatTimeoutMin ~ heartbeatTimeoutMax ms
			if time.Now().After(rf.leaderTimeout) {
				// 超时选举
				go rf.leaderElection()
			}
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.logger = *debugutils.NewLogger(fmt.Sprintf("%d", me))
	// 2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = makeEmptyLog()

	rf.state = Follower
	rf.r = rand.New(rand.NewSource(int64(rf.me)))
	rf.setLeaderTimeout()

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
