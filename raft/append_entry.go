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
	// 2C
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
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		rf.logger.Info("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		rf.logger.Info("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	// if args.PrevLogIndex >= len(rf.log) {
	// 	return
	// } else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// 	// 这里还需要
	// 	rf.logger.Info("args=%v, len(rf.log)=[%d] rf.log[args.PrevLogIndex].Term=[%d]",
	// 		args, len(rf.log), rf.log[args.PrevLogIndex].Term)
	// 	return
	// }

	// 日志同步
	for idx, entry := range args.Entries {
		// args: [{10 2 30} {10 3 1000}]
		//   rf: [{1 1 10} {1 2 20}]
		// 寻找第一个
		if entry.Index <= rf.log.lastLog().Index && entry.Term != rf.log.at(entry.Index).Term {
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			rf.log.truncate(entry.Index)
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
	rf.logger.Info("args.LeaderCommit=[%d], rf.commitIndex=[%d]", args.LeaderCommit, rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
	rf.setLeaderTimeout()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderCommitRule() {
	if rf.state != Leader {
		return
	}
	for idx := rf.commitIndex + 1; idx <= rf.log.lastLog().Index; idx++ {
		rf.logger.Info("rf.log.at(%d).Term=[%d], rf.currentTerm=[%d]", idx, rf.log.at(idx).Term, rf.currentTerm)
		if rf.log.at(idx).Term != rf.currentTerm {
			continue
		}
		count := 1
		for server := range rf.peers {
			rf.logger.Info("rf.matchIndex[%d]=[%d], idx=[%d]", server, rf.matchIndex[server], idx)
			if server != rf.me && rf.matchIndex[server] >= idx {
				count++
			}
			if (count << 1) > len(rf.peers) {
				// 超过半数节点
				rf.logger.Info("超过半数节点")
				rf.commitIndex = idx
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

// Leader给server发送AppendEntries
func (rf *Raft) leaderSendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}
	rf.logger.Info("-> [%d], AppendEntries args=%v", server, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		rf.logger.Info("<- [%d], AppendEntries reply=%v", server, reply)
		if reply.Success {
			// If successful: update nextIndex and matchIndex for follower
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[server] = max(rf.nextIndex[server], next)
			rf.matchIndex[server] = max(rf.matchIndex[server], match)
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
		rf.logger.Info("rf.nextIndex[%d]=[%d]", server, rf.nextIndex[server])
		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		rf.leaderCommitRule()
	}
}

func (rf *Raft) appendEntries(heartbeat bool) {
	// 最后一个日志
	lastLog := rf.log.lastLog()
	// 给每一个server
	// rf.logger.Info("lastLog.Index=[%d]", lastLog.Index)
	for server := range rf.peers {
		if server == rf.me {
			rf.setLeaderTimeout()
			continue
		}
		// rf.logger.Info("rf.nextIndex[%d]=[%d]", server, rf.nextIndex[server])
		if lastLog.Index >= rf.nextIndex[server] || heartbeat {
			// If last log index ≥ nextIndex for a follower:
			// send AppendEntries RPC with log entries starting at nextIndex
			// nextIndex: 下一个要存放的entry的index
			// 获取leader存储的每个server的nextIndex
			nextIndex := rf.nextIndex[server]
			if nextIndex <= 0 {
				// 未初始化，则初始化为1
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				// 如果server的nextIndex超过leader最后一个entry的index
				// 则强制同步
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(server, &args)
		}
	}
}
