package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"../labrpc"
	"log"
	"bytes"
	"encoding/gob"
	"github.com/nsf/termbox-go"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"


type ServerState int

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term		int
	Command		interface{}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	applyCh chan 	ApplyMsg
	chaseMu		sync.Mutex

	//persistent state on all servers
	CurrentTerm	int
	VotedFor	int
	Logs		[]LogEntry
	CurrentIndex	int

	//volatile state on al servers
	CommitedIndex	int
	LastApplied	int

	//reinitialized after election
	NextIndex	[]int
	MatchIndex	[]int

	Sstate		ServerState
	stateCh		chan ServerState
	ElectionTimeout		int
	ElectionTimestamp	int64
	HeartBeatTimeout	int



	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	term = rf.CurrentTerm
	if rf.Sstate == Leader {
		isleader = true;
	} else {
		isleader = false;
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w:=new(bytes.Buffer)
	e:=gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.CurrentIndex)
	e.Encode(rf.CommitedIndex)
	e.Encode(rf.LastApplied)

	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if len(data) == 0 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r:=bytes.NewBuffer(data)
	d:=gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.CurrentIndex)
	d.Decode(&rf.CommitedIndex)
	d.Decode(&rf.LastApplied)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term		int
	CandidateId	int
	LastLogIndex	int
	LastLogTerm	int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.

	Term		int
	VoteGranted	bool
}

type AppendEntryArgs struct {
	Term 		int
	LeaderId	int
	PrevLogIndex	int
	Entries		[]LogEntry
	LeaderCommit	int
}

type AppendEntryReply struct {
	Term 		int
	Success		bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if !rf.checkAndUpdate(args.Term) {
		reply.VoteGranted = false
		return
	}

	reply.Term=rf.CurrentTerm
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		thisLastIndex := len(rf.Logs)-1
		thisLastTerm := rf.Logs[thisLastIndex].Term
		if args.LastLogTerm < thisLastTerm {
			reply.VoteGranted = false
			return
		} else if args.LastLogTerm == thisLastTerm {
			if args.LastLogIndex < thisLastIndex {
				reply.VoteGranted = false
			} else {
				reply.VoteGranted = true
			}
		} else {
			reply.VoteGranted = true
		}
	}

	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok:= rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}




//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.applyLogEntry()
	}()

	return rf
}



// my code below


func (rf *Raft) serverStateMonite() {
	for {
		time.Sleep(time.Millisecond*5)
		rf.ElectionTimeout = rf.ElectionTimeout-5
		if rf.ElectionTimeout <= 0&&rf.Sstate!=Leader {
			rf.resetElectionTimeout()
			rf.switchToCandidate(rf.ElectionTimeout)
		}
	}
}

//this function should be executed in a single goroutine and only that goroutine
//can execute this func
func (rf *Raft) applyLogEntry() {
	for {
		time.Sleep(time.Millisecond*100)
		rf.chaseMu.Lock()

		if(rf.LastApplied<rf.CommitedIndex) {
			logentry := rf.Logs[rf.LastApplied+1]
			msg := new(ApplyMsg)
			msg.Command = logentry.Command
			msg.Index = rf.LastApplied+1
			msg.UseSnapshot = false
			rf.applyCh <- *msg
			rf.LastApplied++
		}

		rf.chaseMu.Unlock()
	}
}

func (rf *Raft) switchToCandidate(electionTimeout int) {
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.resetElectionTimeout()
	rvChan:=make(chan *RequestVoteReply ,len(rf.peers)-1)
	termTmp:=rf.CurrentTerm
	for index,_:=range  rf.peers {
		if index == rf.me {
			continue
		}
		go func(){
			voteArgs:=RequestVoteArgs{}
			voteArgs.CandidateId=rf.me
			voteArgs.Term=rf.CurrentTerm
			voteArgs.LastLogIndex=rf.CurrentIndex
			voteArgs.LastLogTerm=rf.Logs[rf.CurrentIndex].Term
			reply:=new(RequestVoteReply)
			rf.sendRequestVote(index, voteArgs, reply)
			rvChan<-reply
		}()
	}

	totalVote:=1//vote itself

	for iter:=0;iter<len(rf.peers)-1;iter++ {
		reply:=<-rvChan
		if rf.checkAndUpdate(reply.Term) {
			return
		}
		if reply.VoteGranted {
			totalVote++
		}
	}

	if termTmp == rf.CurrentTerm {
		rf.mu.Lock()
		defer  rf.mu.Unlock()
		if termTmp < rf.CurrentTerm {
			return
		}

		if totalVote>len(rf.peers)/2 {
			rf.Sstate = Leader
			rf.sendHeartBeatToAllOthers()
		}
	}



}

func (rf *Raft) sendHeartBeatToAllOthers() {
	for i:=0;i<len(rf.peers);i++ {
		if i!=rf.me {
			continue
		}

		args:=AppendEntryArgs{}
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = 0
		args.LeaderCommit = rf.CommitedIndex
		rf.sendAppendEntry(i, args, new(AppendEntryReply))
	}
}

func (rf *Raft) checkAndUpdate(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock() //???? needs add lock?
	if term<rf.CurrentTerm {
		return false
	} else {
		rf.CurrentTerm = term
		rf.Sstate = Follower
		return true
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.ElectionTimeout = rand.Intn(2000) + 5000
}
