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

import (
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

// definition of various time
const (
	HeartbeatTime = 100*time.Millisecond
	CheckETimeoutTime = 1*time.Millisecond
	AddRandomTime = 150
	MinElectionoutTime = 300
)

// definition of node state
const (
	Leader = 1
	Candidate = 0
	Follower = -1
)

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

// definition of log entries
type LogEntry struct {
	Term 		int
	Command 	interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// from Raft Paper
	currentTerm		int			// persistent state - all servers
	votedFor		int
	logEntries 		[]LogEntry //log entries
	commitIndex		int			// volatile state - all servers
	lastApplied		int
	nextIndex		[]int		// volatile state - leaders
	matchIndex		[]int

	// self
	electionTimeout	int		// nodes' electionTimeout
	receivedVotes	int		// number of votes received
	nodeState		int		// determine the identity of the node
	priorHeartbeat	int64	// last heartbeat from leader
	applyMsgCh		chan ApplyMsg	// lab2
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = false
	if rf.nodeState==Leader {
		isleader = true
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// lab3
func (rf *Raft) persist() {
	// Your code here.
	//the variables of persistent state on all servers
	/*
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.logEntries)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)*/
}

//
// restore previously persisted state.
// lab3
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// for safety: check the length of the array - data
	/*
	if data == nil || len(data) <=0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logEntries)*/
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	// lab1
	Term			int			// round number
	CandidateId		int			// candidate's id for requestiong vote
	// lab2
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	// lab1
	Term			int
	VoteGranted		bool
	//
}

type AppendEntriesArgs struct {
	// lab1
	Term			int
	LeaderId		int
	// lab2
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	//lab1
	Term			int
	HeartbeatRep	bool	// named Success in lab2
	//lab2
	MatchIndex	int
}

// lab2
func (rf *Raft) isLogEntriesUp2date (args RequestVoteArgs) bool{

	up2date := true									// initialized to true
	lastLogIndex := len(rf.logEntries)-1
	if lastLogIndex >= 0 {
		lastLogTerm := rf.logEntries[lastLogIndex].Term
		if lastLogTerm > args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm &&
				lastLogIndex > args.LastLogIndex) {		// if log in args isn't more up-to-date
			up2date = false
		}
	}
	return up2date
}

func (rf *Raft) getLogEntriesLastIndexTerm() (int, int) {
	lastLogIndex := -1
	lastLogTerm := 0
	if len(rf.logEntries) > 0 {
		lastLogIndex = len(rf.logEntries)-1
	}
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logEntries[lastLogIndex].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) change2Candidate() {
	//this node becomes a candidate
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(AddRandomTime)+MinElectionoutTime   // reset electionTimeout 300~450
	// change to candidate state
	rf.nodeState=Candidate
	rf.votedFor = rf.me
	rf.currentTerm += 1
	rf.receivedVotes = 1   // vote for itself
}

func (rf *Raft) change2Follower() {
	rf.votedFor = -1
	rf.receivedVotes = 0
	rf.nodeState = Follower
}

//
// example RequestVote RPC handler.
// lab1
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("RequesVote,server:",rf.me, rf.currentTerm, "args.candidatdid, term:",args.CandidateId,args.Term)
	// reset node to Follower
	if rf.currentTerm < args.Term {
		rf.change2Follower()
		rf.currentTerm = args.Term
	}
	// if votedFor is null or candidateId
	if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId){
		if rf.isLogEntriesUp2date(args) {		// candidate’s log is at least as up-to-date as receiver’s log
			rf.nodeState = Follower
			rf.votedFor = args.CandidateId
			rf.priorHeartbeat = time.Now().UnixNano() / 1e6
			reply.Term = args.Term
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

	} else if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

// lab1
// lab2
func (rf *Raft) checkElectionTimeout() {
	for  {
		nowTime := time.Now().UnixNano()/1e6
		if rf.nodeState!=Leader && (nowTime-rf.priorHeartbeat)>int64(rf.electionTimeout) {
			rf.mu.Lock()
			//fmt.Println("server:",rf.me,rf.nodeState,nowTime - rf.priorHeartbeat,
			//rf.electionTimeout, nowTime, rf.priorHeartbeat)
			//this node becomes a candidate
			rf.change2Candidate()
			lastLogIndex, lastLogTerm := rf.getLogEntriesLastIndexTerm()
			msg := RequestVoteArgs{rf.currentTerm, rf.me,
				lastLogIndex,lastLogTerm} //
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					go func(server int) {	//并发执行，不阻塞
						rmsg := new(RequestVoteReply)
						ok := rf.sendRequestVote(server, msg, rmsg)
						if ok {
							rf.HandleRequestVoteReply(rmsg)
						}
						return
					}(server)
				}
			}
			rf.mu.Unlock()
		}
		time.Sleep(CheckETimeoutTime)
	}
}

// lab1
func (rf *Raft) HandleRequestVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == reply.Term && reply.VoteGranted {
		rf.receivedVotes += 1
		//fmt.Println("HandleRequestVoteReply:",rf.me,rf.receivedVotes)
		if rf.votedFor == rf.me && rf.receivedVotes>len(rf.peers)/2 { //rf.nodeState = Candidate
			rf.nodeState = Leader
			for server := 0; server < len(rf.peers); server++ {		  //Reinitialized after election
				if server != rf.me {
					go func (server int) { //并发执行
						rf.nextIndex[server] = len(rf.logEntries)
						rf.matchIndex[server] = 0
						return
					}(server)
				}
			}
		}
	} else if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.change2Follower()
		rf.priorHeartbeat = time.Now().UnixNano() / 1e6
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// lab1
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func(rf *Raft) Compare2LeaderCommit(LeaderCommit int) {

	if len(rf.logEntries)-1 >= LeaderCommit {
		rf.commitIndex = LeaderCommit
	} else {
		rf.commitIndex = len(rf.logEntries)-1
	}
	//fmt.Println("before commit1 server:",rf.me, rf.lastApplied, rf.commitIndex)
	for item := rf.lastApplied+1; item <= rf.commitIndex; item++ {
		rf.applyMsgCh <- ApplyMsg{Index: item+1, Command: rf.logEntries[item].Command}
		//fmt.Println("commit1 server:",rf.me, rf.logEntries[item].Command)
	}
	rf.lastApplied = rf.commitIndex
}

// lab1
// lab2
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("me:",rf.me,"currentterm:", rf.currentTerm,
	//"args.term:",args.Term, args.LeaderId, rf.votedFor)
	if rf.currentTerm <= args.Term  {
		rf.currentTerm = args.Term
		rf.change2Follower()
		reply.Term = args.Term
		// check for consistency
		index := args.PrevLogIndex
		if index >= 0 &&(len(rf.logEntries)-1 < index || rf.logEntries[index].Term != args.PrevLogTerm) { // inconsistent
				reply.HeartbeatRep = false
				reply.MatchIndex = index  	//return the args.PrevLogIndex
		} else {							// consistent
			if len(args.Entries) !=0 {
				if index >= 0 {
					rf.logEntries = rf.logEntries[:index+1]
				}
				rf.logEntries = append(rf.logEntries, args.Entries...)
				reply.MatchIndex = len(rf.logEntries)-1
				//fmt.Println("AppendEntries len!=0, server:", rf.me, rf.logEntries, rf.lastApplied,reply.MatchIndex)
			} else {
				rf.Compare2LeaderCommit(args.LeaderCommit)
				reply.MatchIndex = index
				//fmt.Println("AppendEntries len=0, server:", rf.me, rf.logEntries, rf.lastApplied,reply.MatchIndex)
			}
			reply.HeartbeatRep = true
			rf.priorHeartbeat = time.Now().UnixNano() / 1e6
		}
	} else {
		reply.Term = rf.currentTerm
		reply.HeartbeatRep = false
	}
}

func (rf *Raft) sendAppendEntries2Server(server int) {
	if server < 0 {
		return
	}
	prevLogTerm := 0
	var entries []LogEntry

	prevLogIndex := rf.nextIndex[server]-1
	if len(rf.logEntries) > 0 {
		if prevLogIndex == -1 {
			prevLogTerm = -1
			entries = rf.logEntries[:len(rf.logEntries)]
		} else if prevLogIndex < len(rf.logEntries) {						// wrong when test TestBackup
			//fmt.Println(len(rf.logEntries), prevLogIndex)
			prevLogTerm = rf.logEntries[prevLogIndex].Term
			entries = rf.logEntries[prevLogIndex+1 : len(rf.logEntries)]
		}
	}
	//fmt.Println("sendAppendEntries,", rf.me, "server", server, prevLogIndex, prevLogTerm, entries, rf.logEntries)
	msg := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex,
		prevLogTerm,entries, rf.commitIndex}
	rmsg := new(AppendEntriesReply)
	ok := rf.sendAppendEntries(server, msg, rmsg)
	if ok {
		rf.HandleAppendEntriesReply(server, rmsg)
	}
}

// lab1: no real payload
// lab2: complete AppendEntries
func (rf *Raft) LeaderHeartbeatAppendEntries() {
	for {
		//fmt.Println("LeaderHeartbeat:", rf.me,
		//rf.currentTerm, isleader, rf.receivedVotes)
		if rf.nodeState == Leader {
			rf.mu.Lock()
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					go rf.sendAppendEntries2Server(server)
				}
			}
			rf.mu.Unlock()
		}
		time.Sleep(HeartbeatTime)
	}
}

// lab2
func (rf *Raft) HandleAppendEntriesReply(server int, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.nodeState == Leader && rf.currentTerm ==reply.Term {
		if reply.HeartbeatRep { //
			rf.nextIndex[server] = reply.MatchIndex + 1
			rf.matchIndex[server] = reply.MatchIndex
			beqN := 0
			N := reply.MatchIndex
			for server_i :=0; server_i<len(rf.peers);server_i++ {
				if server_i != rf.me {
					if N >=0 && rf.matchIndex[server_i] >= N {
						beqN += 1
					}
				}
			}
			// a majority of matchIndex[i]>=N (not including leader, so with "=")
			if beqN >= len(rf.peers)/2 && rf.logEntries[N].Term == rf.currentTerm &&
				rf.commitIndex < N {
				rf.commitIndex = N
				// leader commit
				for item := rf.lastApplied+1; item <= rf.commitIndex; item++ {
					rf.applyMsgCh <- ApplyMsg{Index: item+1, Command: rf.logEntries[item].Command}
					//fmt.Println("commit server:",rf.me, rf.logEntries[item].Command)
				}
				rf.lastApplied = rf.commitIndex
			}

		} else { // decrease 1(not +1) to match;
			//fmt.Println("failed", server)
			rf.nextIndex[server] -= 1         			   // or: rf.nextIndex[server] = reply.index
			go rf.sendAppendEntries2Server(server)		   // resend msg to the server
		}
	} else if rf.commitIndex < reply.Term {
		rf.currentTerm = reply.Term
		rf.change2Follower()
		rf.priorHeartbeat = time.Now().UnixNano() / 1e6
	}
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
	if rf.nodeState == Leader {
		log := LogEntry{Command: command, Term: rf.currentTerm}
		rf.logEntries = append(rf.logEntries, log)
		index = len(rf.logEntries)
		term = rf.currentTerm
		//fmt.Println("server:",rf.me,"entry:", rf.logEntries)
	} else {
		isLeader = false
	}
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

// lab1
func (rf *Raft) initialize(applyCh chan ApplyMsg) {
	// lab1
	rf.currentTerm = 0
	rf.change2Follower()
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(AddRandomTime)+MinElectionoutTime   //300~450
	rf.priorHeartbeat = time.Now().UnixNano() / 1e6

	// lab2
	rf.applyMsgCh = applyCh
	// if there are consistent logEntries
	rf.commitIndex = len(rf.logEntries)-1
	rf.lastApplied = len(rf.logEntries)-1
	length := len(rf.peers)
	rf.nextIndex = make([]int, length)
	rf.matchIndex = make([]int, length)
	//fmt.Println(rf.commitIndex, rf.lastApplied)
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

	// Your initialization code here.
	rf.initialize(applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//
	go rf.checkElectionTimeout()
	go rf.LeaderHeartbeatAppendEntries()

	return rf
}
