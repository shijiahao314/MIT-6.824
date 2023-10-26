package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate's requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Invoked by candidates to gather votes
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		// 任期小于自身，拒绝
		rf.logger.Info("<- [%d], get vote request, args.Term=[%d], rf.currentTerm=[%d]",
			args.CandidateId, args.Term, rf.currentTerm)
		rf.logger.Warn("args.Term < rf.currentTerm, reject vote request")
		return
	}
	rf.logger.Info("<- [%d], get vote request, args.Term=[%d], rf.currentTerm=[%d]",
		args.CandidateId, args.Term, rf.currentTerm)
	if args.Term > rf.currentTerm {
		// 大于自身任期
		rf.logger.Info("args.Term > rf.currentTerm, rf.setNewTerm(args.Term)")
		rf.setNewTerm(args.Term)
		reply.Term = rf.currentTerm
	}
	upToDate := false
	if args.LastLogTerm > rf.log.lastLog().Term {
		upToDate = true
	} else if args.LastLogTerm == rf.log.lastLog().Term && args.LastLogIndex >= rf.log.lastLog().Index {
		upToDate = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		// 未投票或者投的是发送方
		// 为其投票续租
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		reply.VoteGranted = true
	}
	rf.logger.Info("upToDate=%v, reply=%v", upToDate, reply)
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
