package raft

// 从最后一个向左查找rf.log
func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i >= rf.log.LastIncludedIndex; i-- {
		term := rf.log.index(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}
