package raft

func (rf *Raft) switchToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Sstate = Candidate

}
