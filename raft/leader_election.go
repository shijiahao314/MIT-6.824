package raft

// Candidate接收投票结果回调
func (rf *Raft) candidateRequestVote(server int, args *RequestVoteArgs, voteCount *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		// RPC发送失败
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
		return
	}
	if !reply.VoteGranted {
		return
	}
	*voteCount++
	rf.logger.Info("<- [%d], get vote, now vote count [%d / %d]", server, *voteCount, len(rf.peers))
	// 下面代码可能会执行多次，使用sync.Once确保只执行一次？
	// 本段使用了rf.mu.Lock()，且下面修改了rf.state，因此不会执行多次，可以不用sync.Once
	if (*voteCount<<1) > len(rf.peers) && args.Term == rf.currentTerm && rf.state == Candidate {
		// 成为leader
		rf.state = Leader
		lastLogIndex := rf.log.lastLog().Index
		rf.logger.Info("become leader, lastLogIndex=[%d], len(rf.log)=[%d]", lastLogIndex, rf.log.len())
		// 成为Leader后，发挥Raft强主机制，强制同步
		for server := range rf.peers {
			rf.nextIndex[server] = lastLogIndex + 1
			rf.matchIndex[server] = 0
		}
		// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
		// repeat during idle periods to prevent election timeouts
		rf.appendEntries(true)
		// 为什么不使用 go rf.appendEntries(true) 提高并发？
		// 会导致提前释放rf.mu，增加心跳乱序可能性
	}
}

// 设置定时器
func (rf *Raft) leaderElection() {
	// 发起选举
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist(nil)
	rf.state = Candidate
	rf.setLeaderTimeout()
	rf.logger.Info("timeout leader election, rf.currentTerm=[%d]", rf.currentTerm)
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
