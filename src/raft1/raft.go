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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // latest term this server has seen
	votedFor    int // candidateId that received vote in current term (-1 if none)

	role            string    // 'follower', 'candidate', 'leader'
	lastHeartbeat   time.Time // when we last heard from a leader
	electionTimeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool
	// Your code here (3A).
	term = rf.currentTerm
	isLeader = rf.role == "leader"
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
	Term        int // candidate's term
	CandidateId int // candidate requesting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // current term for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so that the follower can redirect clients
}

type AppendEntriesReply struct {
	Term    int // follower's current term
	Success bool
}

// RequestVote is the RPC HANDLER that runs on a VOTER (any server receiving a vote request).
// It is never called directly — the labrpc framework invokes it automatically when
// another server (a candidate) sends a vote request via sendRequestVote.
//
// Grants the vote if the candidate's term is current and we haven't voted
// for a different candidate this term. Resets the election timer on a granted vote
// so the voter doesn't start a competing election while helping a candidate win.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	candidateTerm := args.Term
	candidateId := args.CandidateId

	// stale candidate
	if candidateTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// this voter is behind
	if candidateTerm > rf.currentTerm {
		rf.stepDown(candidateTerm)
	}

	// if I haven't voted, or already voted for them
	if rf.votedFor == -1 || rf.votedFor == candidateId {
		rf.votedFor = candidateId
		rf.lastHeartbeat = time.Now() // don't start its own election for now

		reply.VoteGranted = true
	}

	reply.Term = rf.currentTerm
}

// sendRequestVote is called by a CANDIDATE to ask one specific peer for a vote.
// Spawned as a goroutine inside startElection() for each peer.
// Blocks until the peer replies or the request times out (returns false on failure).
// The caller must NOT hold rf.mu when calling this — Call() can block for a long time.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lTerm := args.Term
	// leader is outdated
	if lTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// we have a valid leader
	rf.currentTerm = lTerm
	rf.lastHeartbeat = time.Now()
	rf.role = "follower"
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// only the server whose election timer expires first becomes a candidate and start an election
		shouldElect := rf.role != "leader" && time.Since(rf.lastHeartbeat) > rf.electionTimeout
		rf.mu.Unlock()
		if shouldElect {
			rf.startElection()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomDurationBetween(300*time.Millisecond, 500*time.Millisecond)

	electionTerm := rf.currentTerm
	votes := 1 // vote for self

	args := RequestVoteArgs{
		Term:        electionTerm,
		CandidateId: rf.me,
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// for each peer, send vote request
		go func(server int) {
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				return
			}

			// stale election
			if electionTerm != rf.currentTerm || rf.role != "candidate" {
				return
			}

			if reply.VoteGranted {
				votes++

				// got the majority of the votes
				if votes > len(rf.peers)/2 && rf.role == "candidate" {
					rf.role = "leader"
					go rf.sendHeartbeats()
				}
			}
		}(i)
	}
}

// sendHeartbeats is launched as a goroutine when this server becomes leader.
// Sends empty AppendEntries to all peers every 100ms to prevent election timeouts.
// Exits automatically when this server is no longer the leader or is killed.
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != "leader" {
			rf.mu.Unlock()
			return
		}
		// Capture state under lock before releasing — goroutines must not read rf fields directly
		term := rf.currentTerm
		me := rf.me
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == me {
				continue
			}
			go func(server int) {
				args := AppendEntriesArgs{Term: term, LeaderId: me}
				reply := AppendEntriesReply{}
				if ok := rf.sendAppendEntries(server, &args, &reply); ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.stepDown(reply.Term)
					}
					rf.mu.Unlock()
				}
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) stepDown(newTerm int) {
	rf.currentTerm = newTerm
	rf.role = "follower"
	rf.votedFor = -1
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = "follower"
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomDurationBetween(300*time.Millisecond, 500*time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
