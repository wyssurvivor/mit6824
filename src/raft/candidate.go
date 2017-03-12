package raft


// add lock outside
func (rf *Raft) switchToLeader() {
	rf.Sstate = Leader
	for index,_:=range rf.peers {
		if index == rf.me {
			continue
		}
		rf.MatchIndex[index]=0
		rf.NextIndex[index] = len(rf.Logs)
	}
	go func() {
		rf.LeaderCron()
	}()
}

func (rf *Raft) switchToFollower(term int) {
	rf.CurrentTerm = term
	rf.Sstate = Follower
	go func() {
		rf.FollowerCron()
	}()
}

func (rf *Raft) CandidateCron() {

}
