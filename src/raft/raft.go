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
import "sync/atomic"
import "github.com/nsiregar/mit-go/src/labrpc"

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	_ = iota
	Candidate
	Follower
	Leader
)

type event struct {
	Payload     interface{}
	ReturnValue []byte
	c           chan error
}

type LogEntry struct {
	Term  int
	Index int
	Value []byte
}

type voteInfo struct {
	count    int32
	votedFor int
}

func newVoteInfo() *voteInfo {
	return &voteInfo{
		count:    0,
		votedFor: -1,
	}
}
func (v *voteInfo) Load() int32 {
	return atomic.LoadInt32(&v.count)
}

func (v *voteInfo) Incr() {
	atomic.AddInt32(&v.count, 1)
}
func (v *voteInfo) voteFor(i int) {
	v.votedFor = i
	v.Incr()
}
func (v *voteInfo) reset() {
	atomic.StoreInt32(&v.count, 0)
	v.votedFor = -1
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm  int
	currentState State
	leaderId     int

	entries []*LogEntry

	vi         *voteInfo
	matchIndex []int
	nextIndex  []int
	evChan     chan *event
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
	// Your code here (2A).
	return rf.currentTerm, rf.currentState == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LastLogIndex int
	LastLogTerm  int
	CandidateId  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Granted bool
	Term    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Granted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.currentState == Candidate {
			rf.currentState = Follower
		} else if rf.currentState == Leader {
			rf.currentState = Follower
			//todo 关闭心跳
		}
		rf.leaderId = -1
	} else {
		if rf.vi.Load() != -1 && int(rf.vi.Load()) != args.CandidateId {
			return
		}
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {

	} else {

	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) Run() {

	for {
		switch rf.currentState {
		case Leader:
			rf.leaderLoop()
		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.CandidateLoop()
		}

	}
}

func (rf *Raft) leaderLoop() {

	rf.vi.reset()
	for _, p := range rf.peers {
		p.Call("Raft.AppendEntries")
	}
	for rf.currentState == Leader {

	}

}

func (rf *Raft) sendAppendEntriesRequest() {

}

func (rf *Raft) followerLoop() {

	timeoutCh := time.Tick(rf.getElectionTimeout())
	for rf.currentState == Follower {
		select {
		case <-timeoutCh:
			rf.currentState = Candidate
			rf.leaderId = -1
			return
		case e := <-rf.evChan:
			switch _ := e.Payload.(type) {
			case *RequestVoteArgs:
			}
		}
	}
}

func (rf *Raft) getElectionTimeout() time.Duration {
	s := rand.NewSource(time.Now().UnixMicro())
	t := 150 + rand.New(s).Int()%150
	return time.Duration(t) * time.Millisecond
}

func (rf *Raft) lastLogInfo() (term int, index int) {
	if len(rf.entries) == 0 {
		return rf.currentTerm, 0
	}
	lastLog := rf.entries[len(rf.entries)-1]
	return lastLog.Term, lastLog.Index
}

func (rf *Raft) CandidateLoop() {
	timeoutCh := time.Tick(rf.getElectionTimeout())
	isVoted := false
	rf.vi.reset()

	respChan := make(chan *RequestVoteReply, len(rf.peers))
	for rf.currentState == Candidate {

		if !isVoted {
			rf.currentTerm++
			rf.vi.voteFor(rf.me)
			lastLogTerm, lastLogIndex := rf.lastLogInfo()
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(id int) {
					vr := &RequestVoteArgs{
						Term:         rf.currentTerm,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
						CandidateId:  rf.me,
					}
					reply := &RequestVoteReply{}
					vote := rf.sendRequestVote(id, vr, reply)
					if vote {
						respChan <- reply
					}
				}(i)

			}

			isVoted = true
		}

		select {
		case rsp := <-respChan:
			if rsp.Granted && rsp.Term == rf.currentTerm {
				rf.vi.Incr()
			}

			if int(rf.vi.Load()) >= rf.getQuorumSize() {
				rf.currentState = Leader
				return
			}

			if rsp.Term > rf.currentTerm {
				rf.currentTerm = rsp.Term
				rf.leaderId = -1
				rf.currentState = Follower
				return
			}

		case <-timeoutCh:
			timeoutCh = time.Tick(rf.getElectionTimeout())
			isVoted = false
			rf.vi.reset()
		case e := <-rf.evChan:
			switch _ := e.Payload.(type) {
			case *RequestVoteArgs:
			}
		}
	}
}

func (rf *Raft) getQuorumSize() int {
	return len(rf.peers)/2 + 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.vi = newVoteInfo()
	rf.evChan = make(chan *event, 10)
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.entries = make([]*LogEntry, 0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Run()

	return rf
}
