package raft

import (
	"bytes"

	"6.5840lab2/labgob"
)

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

/*
Receiver implementation:
1. Reply immediately if term < currentTerm
2. Create new snapshot file if first chunk (offset is 0)
3. Write data into snapshot file at given offset
4. Reply and wait for more data chunks if done is false
5. Save snapshot file, discard any existing or partial snapshot
with a smaller index
6. If existing log entry has same index and term as snapshot’s
last included entry, retain log entries following it and reply
7. Discard the entire log
8. Reset state machine using snapshot contents (and load
snapshot’s cluster configuration)
*/

// Invoked by leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 本实验不采用分片
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.logger.Info("<- [%d], receive InstallSnapshot, args=%v", args.LeaderId, args)
	if args.Term < rf.currentTerm {
		// 1. Reply immediately if term < currentTerm
		rf.logger.Info("-> [%d], args.Term < rf.currentTerm, reply=%v", reply)
		return
	}
	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	r := bytes.NewBuffer(args.Data)
	d := labgob.NewDecoder(r)
	var CommandIndex int
	var xlog []interface{}
	if d.Decode(&CommandIndex) != nil || d.Decode(&xlog) != nil {
		rf.logger.Error("InstallSnapshot: failed to read data")
		return
	}
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or partial snapshot with a smaller index
	// set raft state
	rf.logger.Info("CommandIndex=%d, rf.log=%v", CommandIndex, rf.log)
	// rf.log.setEntries(rf.log.after(CommandIndex))
	rf.log.truncateAfter(CommandIndex)
	rf.lastIncludedIndex = rf.log.get(CommandIndex).Index
	rf.lastIncludedTerm = rf.log.get(CommandIndex).Term
	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if rf.log.lastLog().Term == rf.lastIncludedTerm {
		// term相同才删除该项？为什么
		// rf.log.setEntries(rf.log.after(rf.lastIncludedIndex + 1))
		rf.log.truncateAfter(rf.lastIncludedIndex + 1)
		// apply
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: rf.lastIncludedIndex,
			SnapshotTerm:  rf.lastIncludedTerm,
		}
		rf.mu.Unlock()
		rf.logger.Info(" -> [Client], want to apply snapshot %v", applyMsg)
		// applyCh是无缓冲的，需要释放锁
		rf.applyCh <- applyMsg
		rf.logger.Info("apply snapshot success1")
		rf.mu.Lock()
		rf.lastApplied = rf.log.lastLog().Index
		rf.logger.Info("apply snapshot success2")
		return
	}
	// 7. Discard the entire log
	rf.log.setEntries(make([]Entry, 0))
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	rf.readPersist(rf.persister.ReadRaftState())
	// apply
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapshotTerm:  rf.lastIncludedTerm,
	}
	rf.mu.Unlock()
	// applyCh是无缓冲的，需要释放锁
	rf.logger.Info(" -> [Client], want to apply snapshot %v", applyMsg)
	// applyCh是无缓冲的，需要释放锁
	rf.applyCh <- applyMsg
	rf.logger.Info("apply snapshot success1")
	rf.mu.Lock()
	rf.lastApplied = rf.log.lastLog().Index
	rf.logger.Info("apply snapshot success2")

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// leader send install snapshot rpc to server
func (rf *Raft) leaderSendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
}

func (rf *Raft) installSnapshot() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		//
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.logger.Info("-> [%d], installSnapshot, args={%d, %d, %d, %d}",
			server, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
		go rf.leaderSendInstallSnapshot(server, &args)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Info("Snapshot: index=[%d], rf.lastIncludedIndex=[%d]", index, rf.lastIncludedIndex)
	if index < rf.lastIncludedIndex {
		// 无需更新
		return
	}
	// Raft节点本身不进行快照，而是接收Client的快照
	// 根据快照更新自身 log lastIncludedIndex lastIncludedTerm
	// 这里应该需要循环发送到index
	entry := rf.log.get(index)
	if entry.Index != 0 {
		// Index为index的Entey存在
		rf.lastIncludedIndex = index
		rf.lastIncludedTerm = entry.Term
		// rf.log.setEntries(rf.log.after(index + 1))
		rf.log.truncateAfter(index + 1)
	}
	// persist
	rf.persist(snapshot)
}
