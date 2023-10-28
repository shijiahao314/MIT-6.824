package raft

type AppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	// In practice, we doubt this optimization is necessary,
	// since failures happen infrequently and
	// it is unlikely that there will be many inconsistent entries.
	Conflict bool
	XTerm    int // term in the conflicting entry (if any)
	XIndex   int // index of first entry with that term (if any)
	XLen     int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Invoked by leader to replicate log entries
	// also used as heartbeat
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term > rf.currentTerm {
		// 大于自身任期
		rf.logger.Info("<- [%d], args.Term > rf.currentTerm, accept AppendEntries", args.LeaderId)
		rf.setNewTerm(args.Term)
		return
	}
	if args.Term < rf.currentTerm {
		// 任期小于自身，拒绝
		rf.logger.Warn("<- [%d], args.Term < rf.currentTerm, reject AppendEntries", args.LeaderId)
		return
	}
	// 等于自身任期
	rf.setLeaderTimeout()
	if rf.state == Candidate {
		// If AppendEntries RPC received from new leader: convert to follower
		rf.state = Follower
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(args.Entries) != 0 {
		rf.logger.Error("rf.log.lastLog().Index=[%d], args.PrevLogIndex=[%d]", rf.log.lastLog().Index, args.PrevLogIndex)
	}

	// 考虑rf.log为空的情况
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		rf.logger.Info("[%v]: Conflict1 XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log.get(args.PrevLogIndex).Term != args.PrevLogTerm {
		rf.logger.Warn("args.PrevLogIndex=%d, args.PrevLogTerm=%d", args.PrevLogIndex, args.PrevLogTerm)
		rf.logger.Warn("rf.log=%v", rf.log)
		rf.logger.Warn("rf.log.get(args.PrevLogIndex).Term=%d", rf.log.get(args.PrevLogIndex).Term)
		rf.logger.Warn("args.PrevLogTerm=%d", args.PrevLogTerm)
		reply.Conflict = true
		xTerm := rf.log.get(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.get(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		rf.logger.Info("Conflict2 XTerm %v, XIndex %v, XLen %v", reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	// 日志同步
	for idx, entry := range args.Entries {
		// args: [{10 2 30} {10 3 1000}]
		//   rf: [{1 1 10} {1 2 20}]
		// 寻找第一个
		rf.logger.Info("args.Entries=%v, rf.log=%v", args.Entries, rf.log)
		if entry.Index <= rf.log.lastLog().Index && entry.Term != rf.log.get(entry.Index).Term {
			rf.logger.Error("entry.Index=%d, rf.log.lastLog().Index=%d", entry.Index, rf.log.lastLog().Index)
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			// rf.log.truncate(entry.Index)
			rf.log.truncateBefore(entry.Index)
			// rf: [{1 1 10}]
			rf.persist(nil)
		}
		// 不能用else-if，因为上面修改了rf.log
		if entry.Index > rf.log.lastLog().Index {
			// Append any new entries not already in the log
			rf.log.append(args.Entries[idx:]...)
			rf.logger.Info("append %v", args.Entries[idx:])
			rf.logger.Info("After append, rf.log=%v", rf.log)
			rf.persist(nil)
			break
		}
	}
	//
	// rf.logger.Info("after sync, rf.log=%v", rf.log)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderCheckCommit() {
	if rf.state != Leader {
		return
	}
	for idx := rf.commitIndex + 1; idx <= rf.log.lastLog().Index; idx++ {
		// 依次检查每一个Entry是否达到多数节点提交条件
		if rf.log.get(idx).Term != rf.currentTerm {
			continue
		}
		count := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchIndex[server] >= idx {
				count++
			}
			if (count << 1) > len(rf.peers) {
				// 超过半数节点
				rf.commitIndex = idx
				rf.logger.Info("commit count [%d / %d] , rf.apply(), rf.commitIndex=[%d]", count, len(rf.peers), rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

// Leader给server发送AppendEntries
func (rf *Raft) leaderSendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}
	if len(args.Entries) != 0 {
		rf.logger.Info("-> [%d], AppendEntries args=%v", server, args)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		if len(args.Entries) != 0 {
			rf.logger.Info("<- [%d], AppendEntries reply=%v", server, reply)
		}
		if reply.Success {
			// If successful: update nextIndex and matchIndex for follower
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[server] = max(rf.nextIndex[server], next)
			rf.matchIndex[server] = max(rf.matchIndex[server], match)
			if len(args.Entries) != 0 {
				rf.logger.Info("rf.matchIndex[%d]=[%d], rf.nextIndex[%d]=[%d]", server, rf.matchIndex[server], server, rf.nextIndex[server])
			}
		} else if reply.Conflict {
			rf.logger.Info("[%v]: Conflict from %v %#v", rf.me, server, reply)
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen
			} else {
				// Leader找自己log中term为XTerm的最大Entry的Index: lastLogInXTerm
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				rf.logger.Info("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[server] = lastLogInXTerm
				} else {
					rf.nextIndex[server] = reply.XIndex
				}
			}
			rf.logger.Info("rf.nextIndex[%d]=[%d]", server, rf.nextIndex[server])
		} else if rf.nextIndex[server] > 1 {
			// If AppendEntries fails because of log inconsistency:
			// decrement nextIndex and retry
			rf.nextIndex[server]--
		}
		if len(args.Entries) != 0 {
			rf.logger.Info("rf.nextIndex[%d]=[%d]", server, rf.nextIndex[server])
		}
		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		rf.leaderCheckCommit()
	}
}

func (rf *Raft) leaderCheckSendAppendEntries(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[server]
		if nextIndex <= rf.lastIncludedIndex {
			// 应该发送快照
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.logger.Info("-> [%d], leaderSendInstallSnapshot, args=%v", server, args)
			go rf.leaderSendInstallSnapshot(server, &args)
			// 该server处理完毕
			continue
		}
		// leader的最后一项日志
		lastLog := rf.log.lastLog()
		// 如果lastLog.Index == 0（rf.log无数据）,
		// 且nextIndex > rf.lastIncludedIndex
		// 那么无论如何都无法满足server所需数据
		prevLog := rf.log.get(nextIndex - 1)
		// rf.logger.Info("nextIndex=%d, rf.log=%v, prevLog=%v", nextIndex, rf.log, prevLog)
		if prevLog.Index == 0 {
			// rf.logger.Info("prevLog not existed, use lastIncluded instead")
			// prevLog不存在于rf.log中，那么用lastIncluded代替
			prevLog.Index = rf.lastIncludedIndex
			prevLog.Term = rf.lastIncludedTerm
			// rf.logger.Info("prevLog=%v", prevLog)
		}
		if lastLog.Index >= nextIndex {
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			// 0 <= rf.lastIncludedIndex < nextIndex <= lastLog.Index
			// 一定有所需要的数据（至少有一条）
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      rf.log.between(nextIndex, lastLog.Index+1),
				LeaderCommit: rf.commitIndex,
			}
			// rf.logger.Info("rf.log=%v", rf.log)
			// rf.logger.Info("nextIndex=%d, lastLog.Index+1=%d", nextIndex, lastLog.Index+1)
			// rf.logger.Info("rf.log.between(nextIndex, lastLog.Index+1)=%v", rf.log.between(nextIndex, lastLog.Index+1))
			rf.logger.Info("-> [%d], leaderSendAppendEntries, args=%v", server, args)
			go rf.leaderSendAppendEntries(server, &args)
		} else if heartbeat {
			// 没有所需要的数据，发送心跳包
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, 0),
				LeaderCommit: rf.commitIndex,
			}
			go rf.leaderSendAppendEntries(server, &args)
		}
	}
}
