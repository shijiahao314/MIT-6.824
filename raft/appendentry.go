package raft

import (
	"container/heap"
	"time"
)

// Invoked by leader to replicate log entries
// also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	reply.Success = false
	if len(args.Entries) != 0 {
		rf.logger.Debug("<- [%d], receive AppendEntries, args=%+v", args.LeaderId, args)
	}
	if args.Term < rf.currentTerm {
		// 任期小于自身，拒绝
		rf.logger.Warn("<- [%d], args.Term < rf.currentTerm, reject AppendEntries", args.LeaderId)
		return
	}
	if args.Term > rf.currentTerm {
		// 大于自身任期
		rf.logger.Debug("<- [%d], args.Term > rf.currentTerm, setNewTerm", args.LeaderId)
		rf.setNewTerm(args.Term)
	}
	// 等于自身任期
	if rf.state == Candidate {
		// If AppendEntries RPC received from new leader: convert to follower
		rf.state = Follower
	}
	// 网络不可靠
	if args.PrevLogIndex < rf.log.LastIncludedIndex {
		rf.logger.Warn("network unreliable: outdated AppendEntries")
		return
	}
	rf.resetLeaderTimeout()
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// leader认为server已经有包含PrecLogIndex及之前的日志项
	// 因此leader发送的args.Entry只有PrecLogIndex之后的的日志项
	// 若server存在没有PrecLogIndex及之前的日志项，则矛盾
	if args.PrevLogIndex > 0 {
		// 不为xxx
		if rf.log.lastIndex() < args.PrevLogIndex {
			// server不包含PrevLogIndex
			reply.Conflict = true
			reply.XTerm = -1
			reply.XIndex = rf.log.lastIndex() + 1 // 期待leader发送的下一个Index
			rf.logger.Warn("Conflict: XIndex=[%d]", reply.XIndex)
			return
		}
		if rf.log.index(args.PrevLogIndex).Term != args.PrevLogTerm {
			// server有PrevLogIndex，但是Term不一致
			reply.Conflict = true
			xTerm := rf.log.index(args.PrevLogIndex).Term
			// 寻找rf.log中从args.PrevLogIndex向左第一个term不同xTerm的index
			for xIndex := args.PrevLogIndex; xIndex > rf.log.LastIncludedIndex; xIndex-- {
				if rf.log.index(xIndex-1).Term != xTerm {
					reply.XIndex = xIndex
					break
				}
			}
			reply.XTerm = xTerm
			rf.logger.Warn("Conflict: XTerm=[%d], XIndex=[%d]", reply.XTerm, reply.XIndex)
			return
		}
	}
	// 日志同步
	for idx, entry := range args.Entries {
		// 从左到右遍历args.Entries，寻找第一个entry.Index
		if entry.Index <= rf.log.LastIncludedIndex {
			continue
		}
		if entry.Index <= rf.log.lastIndex() && entry.Term != rf.log.index(entry.Index).Term {
			// 发现index相同但term不同的情况（矛盾），删除rf.log中index及之后的项
			rf.log.truncateBefore(entry.Index)
		}
		// 不能用else-if，因为上面可能修改了rf.log
		if entry.Index > rf.log.lastIndex() {
			// Append any new entries not already in the log
			rf.log.append(args.Entries[idx:]...)
			rf.logger.Debug("append %v", args.Entries[idx:])
			rf.logger.Debug("after append, rf.log=%+v", rf.log)
			break
		}
	}
	rf.persist(nil)
	//
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastIndex())
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func findKthLargest(nums []int, k int) int {
	h := &IntHeap{}
	heap.Init(h)
	for _, item := range nums {
		heap.Push(h, item)
		if h.Len() > k {
			heap.Pop(h)
		}
	}
	return (*h)[0]
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
	if reply.Term < rf.currentTerm {
		rf.logger.Warn("<- [%d], AppendEntries, reply.Term < rf.currentTerm, reply=%+v", server, reply)
		return
	}
	if reply.Term > rf.currentTerm {
		rf.logger.Debug("<- [%d], AppendEntries, reply.Term > rf.currentTerm, reply=%+v", server, reply)
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term != rf.currentTerm {
		rf.logger.Warn("<- [%d], AppendEntries, args.Term != rf.currentTerm, reply=%+v", server, reply)
		return
	}
	if len(args.Entries) != 0 {
		rf.logger.Debug("<- [%d], AppendEntries reply=%+v", server, reply)
	}
	lastIndex := rf.nextIndex[server]
	if reply.Success {
		// 成功，更新match和next
		// match := args.PrevLogIndex + len(args.Entries)
		// next := match + 1
		// rf.matchIndex[server] = max(rf.matchIndex[server], match)
		// rf.nextIndex[server] = max(rf.nextIndex[server], next)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		if len(args.Entries) != 0 {
			rf.logger.Debug("rf.nextIndex[%d]=[%d]->[%d]", server, lastIndex, rf.nextIndex[server])
		}
		// If there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N
		majorityMatchIndex := findKthLargest(rf.matchIndex, len(rf.peers)/2+1)
		if majorityMatchIndex > rf.commitIndex && rf.log.index(majorityMatchIndex).Term == rf.currentTerm {
			rf.commitIndex = majorityMatchIndex
			rf.logger.Debug("leader matchIndex: %v", rf.matchIndex)
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
		}
	} else if reply.Conflict {
		// 矛盾，检查并设置next
		rf.logger.Debug("<- [%d] conflict reply=%+v", server, reply)
		if reply.XTerm == -1 {
			// leader发送的log起始点太靠后（server没有PrevLog），调整next
			rf.nextIndex[server] = reply.XIndex
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
		rf.logger.Debug("rf.nextIndex[%d]=[%d]->[%d]", server, lastIndex, rf.nextIndex[server])
	} else if rf.nextIndex[server] > 1 {
		// If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry
		rf.nextIndex[server]--
		rf.logger.Debug("rf.nextIndex[%d]=[%d]->[%d]", server, lastIndex, rf.nextIndex[server])
	}
}

// func (rf *Raft) leaderCheckCommit() {
// 	// 本方法不加锁，建议调用该方法时持有锁
// 	if rf.state != Leader {
// 		return
// 	}
// 	for idx := max(rf.lastIncludedIndex, rf.commitIndex) + 1; idx <= rf.log.lastIndex(); idx++ {
// 		// 依次检查每一个Entry是否达到多数节点提交条件
// 		if rf.log.index(idx).Term != rf.currentTerm {
// 			continue
// 		}
// 		count := 1
// 		for server := range rf.peers {
// 			if server != rf.me && rf.matchIndex[server] >= idx {
// 				count++
// 			}
// 			if (count << 1) > len(rf.peers) {
// 				// 超过半数节点
// 				rf.commitIndex = idx
// 				rf.logger.Info("commit count [%d / %d] , rf.apply(), rf.commitIndex=[%d]", count, len(rf.peers), rf.commitIndex)
// 				if rf.commitIndex > rf.lastApplied {
// 					rf.apply()
// 				}
// 				break
// 			}
// 		}
// 	}
// }

// leader发送给特定server的go程，为特定的server发送数据/心跳
func (rf *Raft) sendServerProcess(server int, currentTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.state != Leader || rf.currentTerm != currentTerm {
			// 不是leader或不是currentTerm任期，则退出
			return
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
		} else {
			// leader的snapshot里没有server所需index
			var prevLogIndex int
			var prevLogTerm int
			if nextIndex-1 < rf.log.LastIncludedIndex {
				// prevLogIndex < rf.log.LastIncludedIndex | rf.log.Entries[0]
				prevLogIndex = 0
				prevLogTerm = 0
			} else {
				prevLogIndex = nextIndex - 1
				prevLogTerm = rf.log.index(prevLogIndex).Term
			}
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
		// 使用Wait，提高响应速度（来信息可以立即响应），会主动释放锁
		rf.newLogCome.Wait()
	}
}

// leader进程，为每一个server启用一个goroutine发送数据/心跳
func (rf *Raft) leaderProcess(currentTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendServerProcess(server, currentTerm)
	}
}

// leader检测是否该发送heartbeat的go程
func (rf *Raft) leaderHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		sleepTime := time.Until(rf.leaderTimeout)
		rf.mu.Unlock()
		time.Sleep(sleepTime)
		rf.mu.Lock()
		if rf.state != Leader {
			return
		}
		if time.Now().After(rf.leaderTimeout) {
			rf.broadcastLog()
		}
	}
}

// reset leader send to server heartbeat timeout
func (rf *Raft) resetHeartbeatTimeout() {
	// 本方法不加锁，建议调用该方法时持有锁
	rf.leaderTimeout = time.Now().Add(heartbeatInterval)
}
