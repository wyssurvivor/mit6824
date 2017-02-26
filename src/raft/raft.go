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
	"encoding/json"
)

// import "bytes"
// import "encoding/gob"

type ServerState int

const (
	Follower  = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
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

	applyCh chan ApplyMsg
	chaseMu sync.Mutex

	//persistent state on all servers
	CurrentTerm  int
	VotedFor     int //initialized to be -1
	Logs         []LogEntry
	CurrentIndex int

	//volatile state on al servers
	CommitedIndex int
	LastApplied   int

	//reinitialized after election
	NextIndex  []int
	MatchIndex []int

	Sstate            ServerState
	ElectionTimeout   int
	ElectionTimestamp int64
	HeartBeatTimeout  int

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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.

	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	reply.Term = rf.CurrentTerm
	voteResult:=false
	stateTransfer:=false

	if args.Term>rf.CurrentTerm {
		voteResult=true
		stateTransfer=true
	} else if args.Term == rf.CurrentTerm {
		if rf.VotedFor!=-1 || rf.VotedFor == args.CandidateId {
			thisLastIndex:=len(rf.Logs)-1
			thisLastTerm:=rf.Logs[thisLastIndex].Term
			if thisLastTerm<args.LastLogTerm {
				voteResult = true
			} else if thisLastTerm == args.LastLogTerm {
				if thisLastIndex<=args.LastLogIndex {
					voteResult = true
				}
			}
		}
	}

	reply.VoteGranted = voteResult
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if voteResult == true {
		rf.VotedFor = args.CandidateId
		rf.resetElectionTimeout()
	}

	if stateTransfer == true {
		rf.CurrentTerm = args.Term
		rf.Sstate = Follower
	}

	return
}

func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {

	reply.Term = rf.CurrentTerm
	if args.Term<rf.CurrentTerm {
		reply.Success=false
		return
	} else {
		rf.CurrentTerm = args.Term
		rf.Sstate = Follower
		if args.PrevLogIndex>=0 && args.PrevLogIndex <=rf.CurrentIndex {
			if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.Success = false
				return
			}
		} else {
			reply.Success = false
			return
		}

		newLogsIndex:=0
		i:=args.PrevLogIndex
		for ;i<rf.CurrentIndex&&newLogsIndex<len(args.Entries);i++ {
			newLogsIndex++
			if rf.Logs[i].Term!=args.Entries[newLogsIndex].Term {
				break
			}
		}
		//todo delete entries following
		for ;i<rf.CurrentTerm&&newLogsIndex<len(args.Entries);i++ {
			newLogsIndex++
			rf.Logs[i]=args.Entries[newLogsIndex]
		}

		if args.LeaderCommit>rf.CommitedIndex {
			if args.LeaderCommit<len(rf.Logs)-1 {
				rf.CommitedIndex = args.LeaderCommit
			} else {
				rf.CommitedIndex = len(rf.Logs)-1
			}
		}
		reply.Success = true
		return
	}
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
	DPrintf("REQUEST:%d sendRequestVote to %d,args--%s", rf.me, server, string(json.Marshal(args)))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("RESPOND:%d respond sendRequestVote to %d,reply--%s", server, rf.me, string(json.Marshal(reply)))
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	DPrintf("REQUEST:%d sendAppendEntry to %d,args--%s", rf.me, server, string(json.Marshal(args)))
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	DPrintf("RESPOND:%d respond sendAppendEntry to %d,reply--%s", server, rf.me, string(json.Marshal(reply)))
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

	go func(){
		rf.applyLogEntry()
	}()

	go func(){
		rf.serverStateMonite()
	}()
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

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs=make([]LogEntry,10)
	rf.Logs = append(rf.Logs,LogEntry{-1,nil})
	rf.CurrentIndex=0

	rf.CommitedIndex = 0
	rf.LastApplied = 0

	rf.NextIndex = make([]int,len(peers))
	rf.MatchIndex = make([]int, len(peers))

	rf.Sstate = Candidate
	rf.resetElectionTimeout()

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// my code below

func (rf *Raft) serverStateMonite() {
	for {
		currentMillis := getMilliSeconds()
		interval := currentMillis - rf.ElectionTimestamp
		if interval > int64(rf.ElectionTimeout) && rf.Sstate != Leader {
			rf.switchToCandidate()
		}
	}
}

//this function should be executed in a single goroutine and only that goroutine
//can execute this func
func (rf *Raft) applyLogEntry() {
	for {
		time.Sleep(time.Millisecond * 5)
		rf.chaseMu.Lock()

		if (rf.LastApplied < rf.CommitedIndex) {
			logentry := rf.Logs[rf.LastApplied + 1]
			msg := new(ApplyMsg)
			msg.Command = logentry.Command
			msg.Index = rf.LastApplied + 1
			msg.UseSnapshot = false
			rf.applyCh <- *msg
			rf.LastApplied++
		}

		rf.chaseMu.Unlock()
	}
}

func (rf *Raft) switchToCandidate() {
	if rf.Sstate == Leader {
		return
	}
	rf.Sstate = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.resetElectionTimeout()
	rvChan := make(chan *RequestVoteReply, len(rf.peers) - 1)
	termTmp := rf.CurrentTerm
	for index, _ := range rf.peers {
		peerIndex := index
		if peerIndex == rf.me {
			continue
		}
		go func() {
			voteArgs := RequestVoteArgs{}
			voteArgs.CandidateId = rf.me
			voteArgs.Term = rf.CurrentTerm
			voteArgs.LastLogIndex = rf.CurrentIndex
			voteArgs.LastLogTerm = rf.Logs[rf.CurrentIndex].Term
			reply := new(RequestVoteReply)
			rf.sendRequestVote(peerIndex, voteArgs, reply)
			rvChan <- reply
		}()
	}

	totalVote := 1 //vote itself

	for iter := 0; iter < len(rf.peers)-1; iter++ {
		reply := <-rvChan
		if reply.Term>rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.Sstate = Follower
			return
		}
		if reply.VoteGranted {
			totalVote++
		}
	}

	if termTmp == rf.CurrentTerm && rf.Sstate != Leader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if termTmp < rf.CurrentTerm || rf.Sstate == Leader {
			return
		}

		if totalVote > len(rf.peers)/2 {
			rf.Sstate = Leader
			rf.sendHeartBeatToAllOthers()
		}
	}

}

func (rf *Raft) sendHeartBeatToAllOthers() {
	for i := 0; i < len(rf.peers); i++ {
		index := i
		if index == rf.me {
			continue
		}

		go func() {
			args := AppendEntryArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = 0
			args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
			args.LeaderCommit = rf.CommitedIndex
			rf.sendAppendEntry(index, args, new(AppendEntryReply))
		}()
	}
}

func (rf *Raft) checkAndUpdate(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock() //???? needs add lock?
	if term <= rf.CurrentTerm {
		return true
	} else {
		rf.CurrentTerm = term
		rf.Sstate = Follower
		return false
	}
}

// need to add locks beyond this function
func (rf *Raft) resetElectionTimeout() {
	rf.ElectionTimeout = rand.Intn(400) + 500
	rf.ElectionTimestamp = getMilliSeconds()
}

func getMilliSeconds() int64 {
	return time.Now().Round(time.Millisecond).UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
}
