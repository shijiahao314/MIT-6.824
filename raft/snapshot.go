package raft

import "fmt"

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

// Invoked by leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 本实验不采用分片
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	rf.logger.Info("<- [%d], receive InstallSnapshot, args=%+v", args.LeaderId, args)
	if args.Term < rf.currentTerm {
		rf.logger.Warn("-> [%d], args.Term < rf.currentTerm, reply=%+v", args.LeaderId, reply)
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 收到的快照比自己的快照旧
		rf.logger.Warn("args.LastIncludedIndex=[%d] <= rf.lastIncludedIndex=[%d]",
			args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}
	// 修剪log
	rf.log.truncateAfter(args.LastIncludedIndex, args.LastIncludedTerm)
	// 更新raftstate
	rf.resetLeaderTimeout()
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// persist
	rf.persist(args.Data)
	// apply snapshot
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.mu.Unlock()
	rf.logger.Info(" -> [Client], want to apply snapshot %v", applyMsg)
	// applyCh是无缓冲的，需要释放锁
	rf.applyCh <- applyMsg
	rf.mu.Lock()
	rf.logger.Info("apply snapshot success")
	rf.lastApplied = args.LastIncludedIndex
	rf.logger.Info("after install snapshot, rf=%v", rf)
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
	rf.logger.Info("<- [%d]: leaderSendInstallSnapshot, rf.currentTerm=[%d], reply=%v",
		server, rf.currentTerm, reply)
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	// 只更新nextIndex，不更新matchIndex
	// rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Info("Snapshot: index=[%d], rf.lastIncludedIndex=[%d]", index, rf.lastIncludedIndex)
	if index < rf.lastIncludedIndex {
		// 无需更新
		return
	}
	// Raft节点本身不进行快照，而是接收Client的快照
	// 根据快照更新自身 log lastIncludedIndex lastIncludedTerm
	entry := rf.log.index(index)
	// 修剪log
	rf.log.truncateAfter(index, entry.Term)
	// 更新raftstate
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = entry.Term
	// persist
	rf.persist(snapshot)
}
