package raft


const APPEND_ENTRY_TIMEOUT = 100 // must be smaller than ElectionTimeout,or a follower will switch to candidate

func (rf *Raft) leaderSendAppendEntries(logs []LogEntry,prevLogIndex int, prevLogTerm int) {
	args := AppendEntryArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.Entries = logs
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm

	lastLogIndex:=args.PrevLogIndex+len(logs)

	countCh:=make(chan int ,len(rf.peers)/2+1)
	countCh<-1//the leader itself

	for index,_:=range rf.peers {
		if index == rf.me {
			continue
		}
		peerIndex:=index
		go func(){
			reply:=new(AppendEntryReply)
			appendResult:=false
			for   {
				appendResult = rf.sendAppendEntry(peerIndex,args, reply)
				if !appendResult { //if request failed,send request again directly
					continue
				}
				if reply.Term > rf.CurrentTerm { //if any follower's Term is bigger than leader's now,switch to Follower
					rf.mu.Lock()
					rf.Sstate = Follower
					rf.mu.Unlock()
				} else {
					if reply.Success {//if replicate on follower peerIndex succeed,update matchIndex\nextIndex and send 1 to countCH
						rf.updateNextIndexAndMatchIndex(args.PrevLogIndex+len(args.Entries), peerIndex)
						countCh<-1
						break
					} else { // if failed,minus prevLogIndex and update entries field of args and send request again
						args.PrevLogIndex = args.PrevLogIndex-1
						args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
						args.Entries = rf.Logs[args.PrevLogIndex+1:lastLogIndex]
					}
				}
			}

		}()
	}

	totalReplicated := 0
	for count:=1;count<len(rf.peers)/2+1;count++ { //count replicated numbers
		totalReplicated += <-countCh
	}

	rf.updateCommitIndex(lastLogIndex)

}

func (rf *Raft) updateNextIndexAndMatchIndex(matchIndex int,peerIndex int ) {
	rf.mu.Lock()
	if(matchIndex>rf.MatchIndex[peerIndex]) {
		rf.MatchIndex[peerIndex] = matchIndex
		rf.NextIndex[peerIndex] = rf.MatchIndex[peerIndex] + 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitIndex(index int) {
	rf.mu.Lock()

	if index>rf.CommitedIndex && rf.Logs[index].Term == rf.CurrentTerm{
		rf.CommitedIndex = index
	}
	rf.mu.Unlock()
}
