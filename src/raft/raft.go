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
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	size      int                 // the size of peers

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent stat on servers
	currentTerm int // last term server has seen(initialized to 0 on first boot, increase monotonically)
	voteFor     int // candidate id that received vote in current term(or -1 if none)
	//log         []ApplyMsg // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// Volatile state on all servers
	//commitIndex  int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	//lastApplied  int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	state     int // the state on all servers
	lastState int // teh state of last daemon
	//lastLogIndex int // index of last log entry
	//lastLogTerm  int // term of last log entry

	// Volatile state on leaders (Reinitialized after election)
	//nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	//matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	receiveAppendEntryChan chan int
	receiveRequestVoteChan chan int
}

const (
	LEADER    = 0
	CANDIDATE = 1
	FOLLOWER  = 2
)

const FOLLOWER_TIMEOUT = 200
const HEARTBEAT_INTERVAL = 100
const CANDIDATE_TIMEOUT_LEFT = 300
const CANDIDATE_TIMEOUT_RIGHT = 600

type AppendEntryArgs struct {
	TERM      int // leader's term
	LEADER_ID int // so follower can redirect clients
	//prevLogIndex int           // index of log entry immediately preceding new ones
	//preLogTerm   int           // term of prevLogIndex entry
	//entries      []interface{} // log entries to store(empty for heartbeat; may send more than one for efficiency)
	//leaderCommit int           // leader's commit index
}

type AppendEntryReply struct {
	TERM    int  // currentTerm, for leader to update itself
	SUCCESS bool // true if follower contained entry matching preLogIndex and prevLogTerm
}

//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.TERM < rf.currentTerm {
		reply.SUCCESS = false
		reply.TERM = rf.currentTerm
		return
	}

	if args.TERM > rf.currentTerm {
		rf.currentTerm = args.TERM
		rf.state = FOLLOWER
		rf.voteFor = -1
	}
	reply.TERM = rf.currentTerm
	reply.SUCCESS = true

	select {
	case rf.receiveAppendEntryChan <- args.LEADER_ID:
		return
	case <-rf.receiveAppendEntryChan:
		rf.receiveAppendEntryChan <- args.LEADER_ID
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM         int // candidate's term
	CANDIDATE_ID int // candidate requesting vote
	//lastLogIndex int // index of candidate's last log entry
	//lastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	TERM         int  // currentTerm, for candidate to update itself
	VOTE_GRANTED bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.TERM < rf.currentTerm {
		reply.VOTE_GRANTED = false
		reply.TERM = rf.currentTerm
		return
	}

	if args.TERM > rf.currentTerm {
		rf.currentTerm = args.TERM
		rf.state = FOLLOWER
		rf.voteFor = args.CANDIDATE_ID
	}

	if rf.voteFor != -1 && rf.voteFor != args.CANDIDATE_ID {
		reply.VOTE_GRANTED = false
		reply.TERM = rf.currentTerm
		return
	}
	reply.VOTE_GRANTED = true
	reply.TERM = rf.currentTerm
	select {
	case rf.receiveRequestVoteChan <- args.CANDIDATE_ID:
		return
	case <- rf.receiveRequestVoteChan:
		rf.receiveRequestVoteChan <- args.CANDIDATE_ID
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) getState() string {
	switch rf.state {
	case LEADER:
		return "LEADER"
	case FOLLOWER:
		return "FOLLOWER"
	case CANDIDATE:
		return "CANDIDATE"
	default:
		return "UNKNOWN"
	}
}

func (rf *Raft) daemon() {
	for ; ; {
		IPrintf("Now server %d term %d is %s\n", rf.me, rf.currentTerm, rf.getState())
		if rf.state == FOLLOWER {
			rf.followerDaemon()
		} else if rf.state == LEADER {
			rf.leaderDaemon()
		}
	}
}
func (rf *Raft) followerDaemon() {
	timeout := time.After(FOLLOWER_TIMEOUT * time.Millisecond)
	select {
	case <-rf.receiveAppendEntryChan:
		return
	case <-rf.receiveRequestVoteChan:
		return
	case <-timeout:
		rf.startLeaderElection()
	}
}
func (rf *Raft) startLeaderElection() {
	rf.mu.Lock()
	rf.voteFor = rf.me
	rf.state = CANDIDATE
	rf.currentTerm += 1

	args := &RequestVoteArgs{}
	args.TERM = rf.currentTerm
	args.CANDIDATE_ID = rf.me
	IPrintf("Server %d start term %d leader election", rf.me, rf.currentTerm)
	rf.mu.Unlock()


	voteFor := make(chan bool)
	becomeLeader := make(chan bool)
	go func(voteChan chan bool) {
		half := rf.size / 2
		total := 1
		for ; ; {
			success := <-voteChan
			if success {
				total += 1
			}
			if total > half {
				becomeLeader <- true
				return
			}
		}
	}(voteFor)

	for i := 0; i < rf.size; i++ {
		serverId := i
		if i != rf.me {
			go func(voteChan chan bool, args *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				reply.VOTE_GRANTED = false
				ok := rf.sendRequestVote(serverId, args, reply)
				if ok {
					voteFor <- reply.VOTE_GRANTED
				} else {
					voteFor <- false
				}
			}(voteFor, args)
		}
	}

	length := time.Duration(rand.Intn(CANDIDATE_TIMEOUT_RIGHT-CANDIDATE_TIMEOUT_LEFT) + CANDIDATE_TIMEOUT_LEFT)
	timeout := time.After(length * time.Millisecond)
	select {
	case serverId := <-rf.receiveAppendEntryChan:
		IPrintf("Server %d receive append entry request from server %d\n", rf.me, serverId)
	case serverId := <-rf.receiveRequestVoteChan:
		IPrintf("Server %d receive request vote request from server %d\n", rf.me, serverId)
	case <-becomeLeader:
		IPrintf("Server %d become leader\n", rf.me)
		rf.voteFor = -1
		rf.state = LEADER
	case <-timeout:
		IPrintf("Server %d wait to long for %d millseconds", rf.me, length)
		rf.startLeaderElection()
	}
}

func (rf *Raft) leaderDaemon() {
	args := &AppendEntryArgs{}
	args.TERM = rf.currentTerm
	args.LEADER_ID = rf.me

	reply := &AppendEntryReply{}
	reply.SUCCESS = false
	for i := 0; i < rf.size; i++ {
		if i != rf.me {
			go rf.sendAppendEntry(i, args, reply)
		}
	}
	<-time.After(HEARTBEAT_INTERVAL * time.Millisecond)
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
	rf.size = len(peers)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.state = FOLLOWER
	rf.lastState = -1
	// todo init log variable
	//rf.commitIndex = 0
	//rf.lastApplied = 0
	//rf.lastLogTerm = 0
	//rf.lastLogIndex = 0
	rf.receiveAppendEntryChan = make(chan int)
	rf.receiveRequestVoteChan = make(chan int)
	// todo init nextIndex and matchIndex

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.daemon()

	return rf
}
