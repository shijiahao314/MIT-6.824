package raft

// Candidate发送投票请求与接收投票回复
func (rf *Raft) candidateRequestVote(server int, args *RequestVoteArgs, voteCount *int) {
	rf.logger.Debug("-> [%d], send vote request, args=%+v", server, args)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		// RPC发送或接收失败
		return
	}
	rf.logger.Debug("<- [%d], get vote reply, reply=%+v", server, reply)
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
		// 拒绝给自己投票
		return
	}
	*voteCount++
	rf.logger.Info("<- [%d], get vote, now vote count [%d / %d]", server, *voteCount, len(rf.peers))
	// 下面代码可能会执行多次，使用sync.Once确保只执行一次？
	// 本段使用了rf.mu.Lock()，且下面修改了rf.state，因此不会执行多次，可以不用sync.Once
	if rf.state == Candidate && (*voteCount<<1) > len(rf.peers) && args.Term == rf.currentTerm {
		// 成为leader
		rf.state = Leader
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for server := range rf.nextIndex {
			rf.nextIndex[server] = rf.log.lastIndex() + 1
		}
		rf.resetHeartbeatTimeout()
		rf.logger.Info("become leader, rf=%v", rf)
		// 成为Leader后，立即同步（注册）一次，防止选举超时
		// go func() {
		// 	// 这里是apply一次空ApplyMsg，告知KVServer新的leader产生
		// 	rf.applyCh <- ApplyMsg{}
		// }()
		go rf.leaderHeartBeat()
		go rf.leaderProcess(rf.currentTerm)
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
