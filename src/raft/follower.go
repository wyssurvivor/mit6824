package raft

import (
	"time"
)

func (rf *Raft) FollowerCron() {
	for  {
		if rf.Sstate != Follower {
			break;
		}
		select {
		case <-rf.heartBeatCh:
			rf.resetElectionTimeout()
		case <-time.After(time.Millisecond * time.Duration(rf.ElectionTimeout)):
			go func() {
				rf.switchToCandidate()
			}()
			break
		}
	}
}

func (rf *Raft) switchToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.Sstate !=Follower {
		return
	}
	rf.Sstate = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	//rf.resetElectionTimeout()
	rvChan := make(chan *RequestVoteReply, len(rf.peers) - 1)
	//termTmp := rf.CurrentTerm
	for index, _ := range rf.peers {
		peerIndex := index
		if peerIndex == rf.me {
			continue
		}
		go func() {
			voteArgs := RequestVoteArgs{}
			voteArgs.CandidateId = rf.me
			voteArgs.Term = rf.CurrentTerm
			voteArgs.LastLogIndex = len(rf.Logs) //logs first index is 1
			voteArgs.LastLogTerm = rf.Logs[voteArgs.LastLogIndex].Term
			reply := new(RequestVoteReply)
			requestResult:=rf.sendRequestVote(peerIndex, voteArgs, reply)
			if requestResult {
				rvChan <- reply
			}
		}()
	}

	winCh:=make(chan bool)   //接收 是否 精选leader成功 的 channel

	go func() {
		totalVote := 1 //vote itself
		for totalVote<len(rf.peers)/2+1 {
			reply := <-rvChan
			if reply.Term>rf.CurrentTerm {
				//todo:need to add lock here,but don't know how to do
				rf.CurrentTerm = reply.Term
				rf.switchToFollower()
				break
			}
			if reply.VoteGranted {
				totalVote++
			}
		}

		if totalVote>len(rf.peers)/2 {
			winCh<-true
		} else {
			winCh<-false
		}
	}()

	rf.resetElectionTimeout() //reset election timeout config
	select {
	case <-rf.heartBeatCh:   //valid rpc request received ,switch to Follower,the sender make sure args.Term>=currentTerm
		rf.switchToFollower()
	case electionResult:=<-winCh:  // election wins, switch to leader
		if electionResult {
			rf.switchToLeader()
		}
	case time.After(time.Millisecond*time.Duration(rf.ElectionTimeout)):  // election time out ,start a new election
		go func() {
			rf.switchToCandidate()
		}()
	}
}
