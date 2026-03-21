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

const (
	roleFollower  = "follower"
	roleCandidate = "candidate"
	roleLeader    = "leader"
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

	role            string    // roleFollower, roleCandidate, or roleLeader
	lastHeartbeat   time.Time // when we last heard from a leader
	electionTimeout time.Duration

	log         []LogEntry
	commitIndex int
	lastApplied int   // highest index sent to applyCh
	nextIndex   []int // leader only, per peer: next index to send
	matchIndex  []int // leader only, per peer: highest confirmed replicated index
	applyCh     chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == roleLeader
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
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // idx of the candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
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

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int // follower's current term
	Success bool

	// Fast backup hints (only meaningful when Success=false)
	ConflictTerm  int // term of the conflicting entry on the follower
	ConflictIndex int // first index the follower has with that term
}

type LogEntry struct {
	Term    int         // which leader term created this entry
	Command interface{} // the actual command (opaque to Raft - could be anything)
}

// RequestVote is the RPC HANDLER that runs on a VOTER (any server receiving a vote request).
// It is never called directly — the labrpc framework invokes it automatically when
// another server (a candidate) sends a vote request via sendRequestVote.
//
// Grants the vote if: (1) the candidate's term is at least as current as ours,
// (2) we haven't already voted for a different candidate this term, and
// (3) the candidate's log is at least as up-to-date as ours (election restriction,
// Section 5.4.1 — higher last term wins; same last term → longer or equal log wins).
// Resets the election timer on a granted vote to prevent competing elections.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	// stale candidate
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// this voter is behind
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}

	// if I haven't voted, or already voted for them
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		myLastIdx, myLastTerm := rf.lastLogInfo()

		// candidate's up-to-date
		if args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx) {
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now() // don't start its own election for now

			reply.VoteGranted = true
		}
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

// AppendEntries is the RPC HANDLER that runs on a FOLLOWER receiving log entries (or a heartbeat)
// from the leader. It verifies the leader's term, checks that the follower's log matches the
// leader's at PrevLogIndex/PrevLogTerm (Log Matching Property), resolves any conflicts by
// truncating divergent entries, appends new entries, and advances the follower's commitIndex
// based on the leader's LeaderCommit.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader is outdated
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// we have a valid leader
	rf.stepDown(args.Term)
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true

	// log is too short — we don't have an entry at PrevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictTerm = -1           // not a term conflict, just too short
		reply.ConflictIndex = len(rf.log) // "my log is this long, try here"
		return
	}

	// log has an entry at PrevLogIndex but with a different term — logs diverged
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		// walk backward to find the first index with this term
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex > 0 && rf.log[reply.ConflictIndex-1].Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		return
	}

	// walk through leader's entries: skip matching, truncate + append on conflict or gap
	for i := range args.Entries {
		logIdx := args.PrevLogIndex + 1 + i

		// beyond our log — append everything from here onward
		if logIdx >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}

		// conflicting entry — truncate here and append the rest
		if rf.log[logIdx].Term != args.Entries[i].Term {
			rf.log = rf.log[:logIdx]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	// advance commit index based on leader's commit progress
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != roleLeader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	return len(rf.log) - 1, rf.currentTerm, true
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
		shouldElect := rf.role != roleLeader && time.Since(rf.lastHeartbeat) > rf.electionTimeout
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
	rf.role = roleCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomDurationBetween(300*time.Millisecond, 500*time.Millisecond)

	electionTerm := rf.currentTerm
	votes := 1 // vote for self

	lastLogIdx, lastLogTerm := rf.lastLogInfo()
	args := RequestVoteArgs{
		Term:         electionTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIdx,
		LastLogTerm:  lastLogTerm,
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
			if electionTerm != rf.currentTerm || rf.role != roleCandidate {
				return
			}

			if reply.VoteGranted {
				votes++

				// got the majority of the votes — exact equality prevents duplicate launches
				if votes == len(rf.peers)/2+1 {
					rf.role = roleLeader
					// initialize leader state BEFORE launching heartbeats to avoid race
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log) // optimistic - assume everyone is up-to-date
						rf.matchIndex[i] = 0          // pessimistic - haven't confirmed anything yet
					}
					go rf.sendHeartbeats()
				}
			}
		}(i)
	}
}

// sendHeartbeats is launched as a goroutine when this server becomes leader.
// Sends AppendEntries RPCs (heartbeats and/or log entries) to all peers every 100ms.
// Exits automatically when this server is no longer the leader or is killed.
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != roleLeader {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			args := rf.buildAppendEntriesArgs(i)
			go func(server int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.handleAppendEntriesReply(server, &args, &reply)
				}
			}(i, args)
		}
		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// buildAppendEntriesArgs is a helper function that builds the AppendEntriesArgs for a single peer.
// It assumes the caller holds the lock for accessing the Raft instance
func (rf *Raft) buildAppendEntriesArgs(peer int) AppendEntriesArgs {
	prevLogIdx := rf.nextIndex[peer] - 1
	entries := make([]LogEntry, len(rf.log)-rf.nextIndex[peer])
	copy(entries, rf.log[rf.nextIndex[peer]:])

	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  rf.log[prevLogIdx].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

// handleAppendEntriesReply processes the reply from a single peer's AppendEntries RPC.
// It acquires rf.mu itself — the caller must NOT hold the lock.
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
	}

	// stale reply — we're no longer leader or term changed since we sent this RPC
	if rf.role != roleLeader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		// follower accepted — update tracking
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = newMatchIdx
		rf.nextIndex[server] = newMatchIdx + 1

		rf.advanceCommitIndex()
	} else {
		// log inconsistency — use follower's hint to skip entire conflicting terms
		rf.nextIndex[server] = rf.computeNextIndexAfterConflict(reply)
	}
}

// computeNextIndexAfterConflict uses the follower's conflict hint to determine
// the next index to try. Caller must hold rf.mu.
func (rf *Raft) computeNextIndexAfterConflict(reply *AppendEntriesReply) int {
	// follower's log was too short — jump to its length
	if reply.ConflictTerm == -1 {
		return reply.ConflictIndex
	}

	// search our log for the follower's conflict term
	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term == reply.ConflictTerm {
			// we have this term — try one past our last entry with it
			return i + 1
		}
	}

	// we don't have this term — skip to where it starts on follower
	return reply.ConflictIndex
}

// advanceCommitIndex checks if there's a higher index that majority has replicated
// The caller must hold the lock for the Raft instance
func (rf *Raft) advanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		// count how many peers have this entry (leader always has it)
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			return // found the highest N with majority, done
		}
	}
}

// lastLogInfo returns the index and term of the last log entry. Caller must hold rf.mu.
func (rf *Raft) lastLogInfo() (index int, term int) {
	index = len(rf.log) - 1
	return index, rf.log[index].Term
}

func (rf *Raft) stepDown(newTerm int) {
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
	}
	rf.role = roleFollower
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			startIdx := rf.lastApplied + 1
			toSend := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(toSend, rf.log[rf.lastApplied+1:rf.commitIndex+1])
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()

			for idx, entry := range toSend {
				rf.applyCh <- raftapi.ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIdx + idx,
				}
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
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
func Make(
	peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg,
) raftapi.Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = roleFollower
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randomDurationBetween(300*time.Millisecond, 500*time.Millisecond)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine for sending committed entries to the service layer
	go rf.applier()

	return rf
}
