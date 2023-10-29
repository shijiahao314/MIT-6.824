package raft

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
	Success     bool // if success
	Term        int  // currentTerm, for leader to update itself
	LastApplied int
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
	reply.Success = false
	reply.LastApplied = rf.lastApplied
	rf.logger.Info("<- [%d], receive InstallSnapshot, args=%v", args.LeaderId, args)
	if args.Term < rf.currentTerm {
		// 1. Reply immediately if term < currentTerm
		rf.logger.Info("-> [%d], args.Term < rf.currentTerm, reply=%v", args.LeaderId, reply)
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 收到的快照比自己的快照旧
		rf.logger.Info("rf.lastApplied=[%d]", rf.lastApplied)
		rf.logger.Warn("args.LastIncludedIndex=[%d] <= rf.lastIncludedIndex=[%d]",
			args.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 2. Create new snapshot file if first chunk (offset is 0)
	// 3. Write data into snapshot file at given offset
	// 4. Reply and wait for more data chunks if done is false
	// 5. Save snapshot file, discard any existing or
	// partial snapshot with a smaller index

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	if rf.log.lastLog().Index <= args.LastIncludedIndex {
		// 日志最后一项Index比收到的快照LastIncludedIndex小
		rf.log.clean()
	} else {
		// 7. Discard the entire log
		// 日志最后一项Index大于等于收到的快照LastIncludedIndex
		rf.log.truncateAfter(args.LastIncludedIndex)
	}
	// apply
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
	rf.lastApplied = args.LastIncludedIndex
	rf.logger.Info("apply snapshot success")
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	// rf.readPersist(rf.persister.ReadRaftState())
	rf.logger.Info("after install snapshot, rf=%v", rf)
	rf.logger.Info("rf.log=%v", rf.log)
	rf.persist(args.Data)
	reply.Success = true
	reply.LastApplied = rf.lastApplied
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
	if reply.Success {
		// 成功
		rf.matchIndex[server] = rf.lastIncludedIndex
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
	} else {
		rf.matchIndex[server] = reply.LastApplied
		rf.nextIndex[server] = reply.LastApplied + 1
	}
}

// func (rf *Raft) installSnapshot() {
// 	for server := range rf.peers {
// 		if server == rf.me {
// 			continue
// 		}
// 		//
// 		args := InstallSnapshotArgs{
// 			Term:              rf.currentTerm,
// 			LeaderId:          rf.me,
// 			LastIncludedIndex: rf.lastIncludedIndex,
// 			LastIncludedTerm:  rf.lastIncludedTerm,
// 			Data:              rf.persister.ReadSnapshot(),
// 		}
// 		rf.logger.Info("-> [%d], installSnapshot, args={%d, %d, %d, %d}",
// 			server, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
// 		go rf.leaderSendInstallSnapshot(server, &args)
// 	}
// }

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
