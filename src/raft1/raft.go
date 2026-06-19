package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Command string
	Term int
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	cv		  sync.Cond

	electionTimeout time.Duration // election timeout period
	lastRpcTime time.Time // last received RPC time, updated by RequestVote and AppendEntries?

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


	// persistent state
	currentTerm int
	votedFor int
	log []LogEntry
	state NodeState


	// volatile state (all servers)
	commitIndex int
	lastApplied int
	
	// volatile state (leader-only)
	nextIndex []int
	matchIndex []int

	// easy for calculations
	numVotes int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.lastRpcTime = time.Now()

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if len(rf.log) - 1 >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	i := args.PrevLogIndex + 1
	j := 0
	for i < len(rf.log) {
		if rf.log[i].Term != args.Entries[j].Term {
			rf.log = rf.log[:i]
		}
		j++
		i++
	}

	for j < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[j])
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.lastRpcTime = time.Now()

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.state = Follower
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		moreUpToDate := (args.LastLogTerm > rf.log[len(rf.log) - 1].Term) || (args.LastLogTerm == rf.log[len(rf.log) - 1].Term && args.LastLogIndex	>= len(rf.log) - 1)
		if moreUpToDate {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// Your code here (3B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {

		rf.mu.Lock()
		
		if rf.state == Leader {
			rf.mu.Unlock()

			args := AppendEntriesArgs{}
			args.Entries = make([]LogEntry, 0)

			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = len(rf.log) - 1
			args.PrevLogTerm = rf.log[len(rf.log) - 1].Term

			rf.mu.Unlock()

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {

					go func(peer int) {
						reply := AppendEntriesReply{}
						rf.sendAppendEntries(i, &args, &reply)

						if reply.Term > args.Term {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.mu.Unlock()
							return
						}	
					}(i)
					
				}
			}
		} else {
			rf.mu.Unlock()
		}


		time.Sleep(120 * time.Millisecond)

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Since(rf.lastRpcTime) > rf.electionTimeout && rf.state != Leader {
			
			rf.currentTerm++
			rf.state = Candidate
			rf.votedFor = rf.me
			rf.lastRpcTime = time.Now()
			ms := 800 + (rand.Int63() % 200)
			rf.electionTimeout = time.Duration(ms * int64(time.Millisecond))
			
			rf.mu.Unlock()	
			args := RequestVoteArgs{}
			args.CandidateId = rf.me
			rf.mu.Lock()
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[len(rf.log) - 1].Term
			args.Term = rf.currentTerm
			rf.mu.Unlock()

			numVotes := 1

			for i := 0; i < len(rf.peers); i++ {

				rf.mu.Lock()
				if rf.state == Follower {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				if i != rf.me {

					go func(peer int) {
						reply := RequestVoteReply{}
						rf.sendRequestVote(i, &args, &reply)

						if reply.Term > args.Term {
							rf.mu.Lock()
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							rf.mu.Unlock()
							return
						}

						if reply.VoteGranted {
							numVotes++
							if numVotes >= (len(rf.peers) + 1) / 2 {
								rf.mu.Lock()
								rf.state = Leader
								for i := 0; i < len(rf.peers); i++ {
									rf.nextIndex[i] = len(rf.log)
									rf.matchIndex[i] = 0
								}
								rf.mu.Unlock()
								return
							}
						}
					}(i)
					

				}
			}

		} else {
			rf.mu.Unlock()
		}
		

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		time.Sleep(10 * time.Millisecond)
	}
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	
	
	// initialize from state persisted before a crash
	ms := 900 + (rand.Int63() % 200)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
	rf.lastRpcTime = time.Now()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0].Term = 0
	
	rf.state = Follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}


	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeat()


	return rf
}
