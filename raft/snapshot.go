package raft

// Invoked by leader to send chunks of a snapshot to a follower.
// Leaders always send chunks in order.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 本实验不采用分片
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	rf.logger.Info("<- [%d], receive InstallSnapshot, args=%+v", args.LeaderId, args)
	if args.Term < rf.currentTerm {
		rf.logger.Warn("-> [%d], args.Term < rf.currentTerm, reply=%+v", args.LeaderId, reply)
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 收到的快照比自己的快照旧（不新）
		rf.logger.Warn("outdated InstallSnapshot: args.LastIncludedIndex=[%d] <= rf.lastIncludedIndex=[%d]",
			args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}
	rf.resetLeaderTimeout()
	// change rf
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.log.truncateAfter(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persist(args.Data)
	if rf.lastApplied > args.LastIncludedIndex {
		return
	}
	// apply snapshot
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyHelper.tryApply(applyMsg)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// leader send install snapshot rpc to server
func (rf *Raft) leaderSendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, &reply)
	if !ok || rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Info("<- [%d]: leaderSendInstallSnapshot, reply=%+v", server, reply)
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	// 为什么只更新nextIndex，不更新matchIndex
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
	if index <= rf.lastIncludedIndex {
		return
	}
	// change rf
	entry := rf.log.index(index)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = entry.Term
	rf.lastApplied = index
	rf.log.truncateAfter(index, entry.Term)
	rf.persist(snapshot)
}
