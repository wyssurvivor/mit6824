package raft

import "time"

func (rf *Raft) FollowerCron() {
	for  {
		if rf.Sstate != Follower {
			break;
		}
		select {
		case <-rf.heartBeatCh:
			rf.resetElectionTimeout()
		case <-time.After(time.Millisecond * time.Duration(rf.ElectionTimeout)):
			rf.resetElectionTimeout()
			rf.switchToCandidate()
		}
	}
}
