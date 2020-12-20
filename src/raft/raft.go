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

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//
	currentTerm		int
	votedFor		int
	electionTimeout	int

	//
	receivedVotes	int
	priorHeartbeat	int64
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
	if rf.votedFor == rf.me && rf.receivedVotes>len(rf.peers)/2 {
		isleader = true
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
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int			// round number
	CandidateId		int
	//
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int
	VoteGranted		bool
	//
}

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
}

type AppendEntriesReply struct {
	Term			int
	HeartbeatRep	bool
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("RequesVote,server:",rf.me)
	if rf.currentTerm < args.Term {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.priorHeartbeat=time.Now().UnixNano() / 1e6
		//fmt.Println("RequesVote, server:",rf.me,"heartbeat:",rf.priorHeartbeat)
		reply.Term = args.Term
		reply.VoteGranted = true
	}
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
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

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.HeartbeatHandle", args, reply)
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

func (rf *Raft) initialize() {

	rf.currentTerm = 0
	rf.votedFor = -1
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout=rand.Intn(300)+150   //150~300
	rf.priorHeartbeat = time.Now().UnixNano() / 1e6
	rf.receivedVotes = 0
	//fmt.Println("initialize server:",rf.me, "timeout:",rf.electionTimeout)
}

func (rf *Raft) checkElectionTimeout() {
	for  {
		nowTime := time.Now().UnixNano()/1e6
		_, isleader := rf.GetState()
		if !isleader && (nowTime-rf.priorHeartbeat)>int64(rf.electionTimeout) {
			rf.mu.Lock()
			//fmt.Println("server:",rf.me,isleader,nowTime - rf.priorHeartbeat, rf.electionTimeout, nowTime, rf.priorHeartbeat)
			//this node becomes a candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.receivedVotes = 1   //
			msg := RequestVoteArgs{rf.currentTerm, rf.me}
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					go func(server int) {	//并发执行，不阻塞
						rmsg := new(RequestVoteReply)
						ok := rf.sendRequestVote(server, msg, rmsg)
						//fmt.Println("checkTimeout aftersendvote:", server, " ", rf.me, ok)
						if ok {
							if rmsg.Term == rf.currentTerm && rmsg.VoteGranted {
								rf.receivedVotes += 1
							}
						}
						return
					}(server)
				}
			}
			//fmt.Println("allsent server:", rf.me, "currentTerm:", rf.currentTerm, "receivedVotes:", rf.receivedVotes, "timeout:", rf.electionTimeout)
			rf.mu.Unlock()
			time.Sleep(10*time.Millisecond)
		}
	}
}

func (rf *Raft) LeaderHeartbeat() {
	for {
		_, isleader := rf.GetState()
		//fmt.Println("LeaderHeartbeat:", rf.me, rf.currentTerm, isleader, rf.receivedVotes)
		if isleader {
			rf.mu.Lock()
			msg := AppendEntriesArgs{rf.currentTerm, rf.me}
			for server := 0; server < len(rf.peers); server++ {
				if server != rf.me {
					go func (server int) { //并发执行，不阻塞
						rmsg := new(AppendEntriesReply)
						if rf.sendAppendEntries(server, msg, rmsg) {
							if rmsg.HeartbeatRep {
								//fmt.Println("server:", server, "replay:", rmsg.HeartbeatRep)
							}
						}
						return
					}(server)
				}
			}
			rf.mu.Unlock()
		}
		time.Sleep(10*time.Millisecond)
	}
}

func (rf *Raft) HeartbeatHandle(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("me:",rf.me,"currentterm:", rf.currentTerm, "args.term:",args.Term, args.LeaderId, rf.votedFor)
	if rf.currentTerm <= args.Term  {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.receivedVotes = 0
		rf.priorHeartbeat=time.Now().UnixNano() / 1e6
		reply.Term = args.Term
		reply.HeartbeatRep = true
		//fmt.Println(rf.me, rf.votedFor)
	}

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
	rf.initialize()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//
	go rf.checkElectionTimeout()
	go rf.LeaderHeartbeat()

	return rf
}
