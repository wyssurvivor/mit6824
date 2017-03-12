package raft

import "time"

const LEADER_HEARTBEAT_TIMEOUT = 300 // must be smaller than ElectionTimeout,or a follower will switch to candidate

func (rf *Raft) LeaderSendAppendEntries() {
	countCh:=make(chan int ,len(rf.peers))

	for index,_:=range rf.peers {
		if index == rf.me {
			continue
		}
		peerIndex:=index

		go func(){
			reply:=new(AppendEntryReply)
			appendResult:=false
			for   {
				args:=AppendEntryArgs{}
				args.Term = rf.CurrentTerm
				args.LeaderId = rf.me
				args.Entries = rf.Logs[rf.NextIndex[peerIndex]:]
				args.PrevLogIndex = rf.NextIndex[peerIndex]-1
				args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
				args.LeaderCommit = rf.CommitedIndex

				appendResult = rf.sendAppendEntry(peerIndex,args, reply)
				if !appendResult { //if request failed,send request again directly
					continue
				}
				if reply.Term > rf.CurrentTerm { //if any follower's Term is bigger than leader's now,switch to Follower
					rf.mu.Lock()
					rf.switchToFollower()
					rf.mu.Unlock()
				} else {
					if reply.Success {//if replicate on follower peerIndex succeed,update matchIndex\nextIndex and send 1 to countCH
						rf.UpdateNextIndexAndMatchIndex(args.PrevLogIndex+len(args.Entries), peerIndex)
						countCh<-rf.MatchIndex[peerIndex]
						break
					} else { // if failed,decret nextIndex and retry.no need to add locks here
						rf.NextIndex[peerIndex] = rf.NextIndex[peerIndex]-1
					}
				}
			}

		}()
	}

	smallestReplicatedIndex :=0
	for count:=1;count<len(rf.peers)/2;count++ { //if a majority of  followers appendentry succeed,get the smallest matchindex
		replicatedIndex:=<-countCh
		if replicatedIndex<smallestReplicatedIndex {
			smallestReplicatedIndex = replicatedIndex
		}
	}

	rf.UpdateCommitIndex(smallestReplicatedIndex)

}

func (rf *Raft) UpdateNextIndexAndMatchIndex(matchIndex int,peerIndex int ) {
	rf.mu.Lock()
	if(matchIndex>rf.MatchIndex[peerIndex]) {
		rf.MatchIndex[peerIndex] = matchIndex
		rf.NextIndex[peerIndex] = rf.MatchIndex[peerIndex] + 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) UpdateCommitIndex(index int) {
	rf.mu.Lock()

	if index>rf.CommitedIndex && rf.Logs[index].Term == rf.CurrentTerm{ //if index is bigger than committedInde now and term is equal
		rf.CommitedIndex = index
	}
	rf.mu.Unlock()
}

// run this function in a single goroutine when a server is switched to leader
func (rf *Raft) LeaderCron() {
	rf.LeaderSendAppendEntries()// send heart beat request to all followers on changing to Leader
	for   {
		select {
		case <-time.After(time.Millisecond*time.Duration(LEADER_HEARTBEAT_TIMEOUT)):
			if rf.Sstate!=Leader {
				break
			}
			rf.LeaderSendAppendEntries()
		}
	}

}


