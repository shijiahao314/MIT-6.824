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
	// since failures hppen infrequently and
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
		rf.logger.Info("<- [%d], args.Term > rf.currentTerm, setNewTerm", args.LeaderId)
		rf.setNewTerm(args.Term)
		return
	}
	if args.Term < rf.currentTerm {
		// 任期小于自身，拒绝
		rf.logger.Warn("<- [%d], args.Term < rf.currentTerm, reject AppendEntries", args.LeaderId)
		return
	}
	// 等于自身任期
	rf.resetLeaderTimeout()
	if rf.state == Candidate {
		// If AppendEntries RPC received from new leader: convert to follower
		rf.state = Follower
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(args.Entries) != 0 {
		rf.logger.Error("rf.log.lastIndex()=[%d], args.PrevLogIndex=[%d]", rf.log.lastIndex(), args.PrevLogIndex)
	}
	// leader认为server已经有包含PrecLogIndex及之前的日志项
	// 因此leader发送的args.Entry只有PrecLogIndex之后的的日志项
	// 若server存在没有PrecLogIndex及之前的日志项，则矛盾
	if rf.lastIncludedIndex < args.PrevLogIndex {
		if rf.log.lastIndex() < args.PrevLogIndex {
			// server不包含PrevLogIndex
			reply.Conflict = true
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = rf.lastApplied + 1
			rf.logger.Info("Conflict1 XTerm %v, XIndex %v, XLen %v", reply.XTerm, reply.XIndex, reply.XLen)
			return
		}
		if rf.log.index(args.PrevLogIndex).Term != args.PrevLogTerm {
			// server有PrevLogIndex，但是Term不一致
			rf.logger.Warn("args.PrevLogIndex=%d, args.PrevLogTerm=%d", args.PrevLogIndex, args.PrevLogTerm)
			rf.logger.Warn("rf.log=%+v", rf.log)
			rf.logger.Warn("rf.log.get(args.PrevLogIndex).Term=%d", rf.log.index(args.PrevLogIndex).Term)
			rf.logger.Warn("args.PrevLogTerm=%d", args.PrevLogTerm)
			reply.Conflict = true
			xTerm := rf.log.index(args.PrevLogIndex).Term
			for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
				if rf.log.index(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex
					break
				}
			}
			reply.XTerm = xTerm
			// reply.XLen = rf.log.len()
			reply.XLen = rf.lastApplied + 1
			rf.logger.Info("Conflict2 XTerm %v, XIndex %v, XLen %v", reply.XTerm, reply.XIndex, reply.XLen)
			return
		}
	}
	// 日志同步
	for idx, entry := range args.Entries {
		// 寻找第一个矛盾的
		if entry.Index <= rf.log.lastIndex() && entry.Term != rf.log.index(entry.Index).Term {
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it
			rf.log.truncateBefore(entry.Index)
			rf.persist(nil)
		}
		// 不能用else-if，因为上面可能修改了rf.log
		if entry.Index > rf.log.lastIndex() {
			// Append any new entries not already in the log
			rf.log.append(args.Entries[idx:]...)
			rf.logger.Info("append %v", args.Entries[idx:])
			rf.logger.Info("after append, rf.log=%+v", rf.log)
			rf.persist(nil)
			break
		}
	}
	//
	// rf.logger.Info("after sync, rf.log=%v", rf.log)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderCheckCommit() {
	// 本方法不加锁，建议调用该方法时持有锁
	if rf.state != Leader {
		return
	}
	for idx := max(rf.lastIncludedIndex, rf.commitIndex) + 1; idx <= rf.log.lastIndex(); idx++ {
		// 依次检查每一个Entry是否达到多数节点提交条件
		if rf.log.index(idx).Term != rf.currentTerm {
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
	// 判断reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.logger.Info("<- [%d], AppendEntries, reply.Term > rf.currentTerm, reply=%+v", server, reply)
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < rf.currentTerm {
		rf.logger.Warn("<- [%d], AppendEntries, reply.Term < rf.currentTerm, reply=%+v", server, reply)
		return
	}
	if args.Term == rf.currentTerm {
		if len(args.Entries) != 0 {
			rf.logger.Info("<- [%d], AppendEntries reply=%+v", server, reply)
		}
		if reply.Success {
			// 成功，更新match和next
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.matchIndex[server] = max(rf.matchIndex[server], match)
			rf.nextIndex[server] = max(rf.nextIndex[server], next)
		} else if reply.Conflict {
			// 矛盾，检查并设置next
			rf.logger.Info("<- [%d] conflict reply=%+v", server, reply)
			if reply.XTerm == -1 {
				// leader发送的log起始点太靠后（server没有PrevLog），调整next
				rf.nextIndex[server] = reply.XLen
			} else {
				// *较难理解：快速同步-可能不止一次-可以减少发送次数
				// server有PrevLog，但是Term不一致，调整next（Raft强主）
				// 需要leader找自己log中term为XTerm的最大index
				// 若找到，则将next设为该index
				// 否则使用server认为的开始同步点
				lastIndexXTerm := rf.findLastLogInTerm(reply.XTerm)
				if lastIndexXTerm > 0 {
					rf.nextIndex[server] = lastIndexXTerm
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

// leader check to send AppendEntries rpc request
func (rf *Raft) leaderCheckSendAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[server] // leader认为server期望接收的index
		// 检查发送条件，判断应该发送什么
		if nextIndex <= rf.lastIncludedIndex {
			// leader的snapshot里有server所需的index
			// 应该发送快照
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.logger.Info("-> [%d], leaderSendInstallSnapshot, args=%+v", server, args)
			go rf.leaderSendInstallSnapshot(server, &args)
			// 该server处理完毕
			continue
		} else {
			// leader的snapshot里没有server所需index
			prevLogIndex := nextIndex - 1
			prevLogTerm := rf.log.index(prevLogIndex).Term
			lastIndex := rf.log.lastIndex()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []Entry{},
				LeaderCommit: rf.commitIndex,
			}
			if nextIndex <= lastIndex {
				// 但leader的log里有
				// 设置所需发送的Entries
				args.Entries = rf.log.between(nextIndex, lastIndex+1)
				// 发送数据包
				rf.logger.Info("-> [%d], leaderSendAppendEntries, args=%+v", server, args)
				go rf.leaderSendAppendEntries(server, &args)
			} else {
				// 都没有，只发送简单心跳包
				go rf.leaderSendAppendEntries(server, &args)
			}
		}
	}
}
