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
		// 无需进行快照
		return
	}
	// 进行快照
	// rf.log = [lastIncludedIndex, lastIncludedIndex + 1, ..., lastIncludedIndex + x, ...]
	// lastIncludedIndex + x = index
	entry := rf.log.at(index)
	rf.lastIncludedIndex = entry.Index
	rf.lastIncludedTerm = entry.Term
	// trim log
	rf.logger.Info("before trim, log=%v", rf.log)
	rf.log.setEntries(rf.log.slice(entry.Index + 1))
	rf.logger.Info("after trim, log=%v", rf.log)
	// persist
	rf.persist(snapshot)
}
