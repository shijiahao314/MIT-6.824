package raft

// Candidate发送投票请求与接收投票回复
func (rf *Raft) candidateRequestVote(server int, args *RequestVoteArgs, voteCount *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		// RPC发送或接收失败
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		// 回复的任期大于请求任期
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		// 回复的任期小于请求任期
		rf.logger.Warn("candidateRequestVote: old reply=%v, args.Term=%d", reply, args.Term)
		return
	}
	if !reply.VoteGranted {
		// 拒绝为自己投票
		return
	}
	*voteCount++
	rf.logger.Info("<- [%d], get vote, now vote count [%d / %d]", server, *voteCount, len(rf.peers))
	// 下面代码可能会执行多次，使用sync.Once确保只执行一次？
	// 本段使用了rf.mu.Lock()，且下面修改了rf.state，因此不会执行多次，可以不用sync.Once
	if rf.state == Candidate && (*voteCount<<1) > len(rf.peers) && args.Term == rf.currentTerm {
		// 成为leader
		rf.state = Leader
		rf.logger.Info("become leader, rf=%v", rf)
		// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		// repeat during idle periods to prevent election timeouts
		// 成为Leader后，立即同步（注册）一次，防止选举超时
		lastLogIndex := rf.log.lastLog().Index
		for server := range rf.peers {
			// 下面这个是错误的，为什么？
			// if server == rf.me {
			// 	// 设置自身的matchIndex和nextIndex
			// 	rf.matchIndex[server] = max(rf.lastIncludedIndex, rf.lastApplied)
			// 	rf.nextIndex[server] = rf.matchIndex[server] + 1
			// 	continue
			// }
			// 初始化leader对于所有server的matchIndex和nextIndex
			rf.matchIndex[server] = 0
			rf.nextIndex[server] = lastLogIndex + 1
		}
		go rf.leaderCheckSendAppendEntries()
	}
}

// 发起选举
func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(nil)
	rf.state = Candidate
	rf.resetLeaderTimeout()
	rf.logger.Info("timeout leader election, rf=%+v", rf)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastLog().Index,
		LastLogTerm:  rf.log.lastLog().Term,
	}
	voteCount := 1
	for server := range rf.peers {
		if server != rf.me {
			go rf.candidateRequestVote(server, &args, &voteCount)
		}
	}
}
