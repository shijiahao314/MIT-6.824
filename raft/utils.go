package raft

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.index(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}
