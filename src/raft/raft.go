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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent states
	currentTerm int
	votedFor    int
	log         []Entries
	// volatile state for all servers
	commitIndex     int
	lastApplied     int
	role            Role
	electionTimeout time.Duration
	lastTouchedTime time.Time
	gotVotesMap     []bool
	// volatile state for leaders
	nextIndex  []int
	matchIndex []int
}

type Role string

var IdleEntry = Entries{
	Command: nil,
	Term:    -1,
}

const (
	CANDIDATE Role = "Candidate"
	FOLLOWER  Role = "Follower"
	LEADER    Role = "Leader"
)

type Entries struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
// it must locked..
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.role == LEADER {
		isleader = true
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) ClearVoteMap() {
	for i := 0; i < len(rf.peers); i++ {
		rf.gotVotesMap[i] = false
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DebugOutput(dInfo, "S%d T%d got RV from %d", rf.me, rf.currentTerm, args.CandidateId)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// rule 2 for all servers
	if args.Term > rf.currentTerm {
		DebugOutput(dRole, "S%d T%d down to follower T%d<T%d", rf.me, rf.currentTerm, rf.currentTerm, args.Term)
		rf.role = FOLLOWER
		rf.ClearVoteMap()
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.Term < rf.currentTerm { // ignore...
		reply.VoteGranted = false
		return
	}
	if rf.role != FOLLOWER { // not follower, not give the vote
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // suitable for 2A
		isNewer := false
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term { // laster term
			isNewer = true
		}
		if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1 { // same term but at-least-longer log
			isNewer = true
		}
		if isNewer {
			DebugOutput(dInfo, "S%d T%d grant RV to %d", rf.me, rf.currentTerm, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}
}

// AppendEntries caller send hb to callee
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DebugOutput(dHERT, "S%d got AE from %d", rf.me, args.LeaderId)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// rule 2 for all servers
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		DebugOutput(dRole, "S%d T%d down to follower T%d<T%d", rf.me, rf.currentTerm, rf.currentTerm, args.Term)
		rf.ClearVoteMap()
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.Term < rf.currentTerm { // ignore
		reply.Success = false
		reply.Index = 1
		return
	}
	if rf.role == CANDIDATE && args.LeaderId != rf.me { // down to follower
		DebugOutput(dRole, "S%d T%d down to follower L%d", rf.me, rf.currentTerm, args.LeaderId)
		rf.role = FOLLOWER
		rf.ClearVoteMap()
	}
	// got the hb
	rf.lastTouchedTime = time.Now()

	if args.LeaderId == rf.me { // send to myself, return
		reply.Success = true
		return
	}
	// here, not self->self
	// may still be heartbeat or RP
	if len(rf.log)-1 < args.PrevLogIndex { // log is shorter
		DebugOutput(dInfo, "S%d T%d shorter(%d) than LD%d(%d)", rf.me, rf.currentTerm,
			len(rf.log)-1, args.LeaderId, args.PrevLogIndex)
		reply.Success = false
		reply.Index = len(rf.log) // accelerate the speed of the decreasing of nextIndex, next should start from here
	} else {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // wrong match, may need former
			reply.Success = false
			reply.Index = args.PrevLogIndex // next RPC's entry should start from here
			DebugOutput(dInfo, "S%d T%d diff(%d) from LD %d(%d) at %d", rf.me, rf.currentTerm,
				rf.log[args.PrevLogIndex].Term, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex)
			return // no need copy next...
		}
		if len(rf.log) > args.PrevLogIndex+1 {
			rf.log = rf.log[:args.PrevLogIndex+1] // strip
		}
		if len(args.Entries) > 0 {
			DebugOutput(dInfo, "S%d T%d copy[%d,%d] from %d", rf.me, rf.currentTerm, args.PrevLogIndex+1,
				args.PrevLogIndex+len(args.Entries), args.LeaderId)
			// copy [nextIndex, index] to follower
			for i := 0; i < len(args.Entries); i++ {
				rf.log = append(rf.log, args.Entries[i])
			}
		}
		reply.Success = true // when success, index is useless

		// part5, change commitIndex, this must happen after check
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
			DebugOutput(dCMIT, "S%d T%d CMIT %d", rf.me, rf.currentTerm, rf.commitIndex)
		}
	}
	// never got here
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
// capitalized all field names in structs ,passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DebugOutput(dSIZE, "S%d ->%d Size:%d %d", rf.me, server, unsafe.Sizeof(*args), unsafe.Sizeof(*reply))
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DebugOutput(dSIZE, "S%d ->%d Size:%d %d", rf.me, server, unsafe.Sizeof(*args), unsafe.Sizeof(*reply))
	return ok
}

// use Capital letters
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entries // entries, empty for hb
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Index   int // extra field for reduce rpc's number
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
	index := -1
	term := rf.currentTerm
	isLeader := false
	if rf.role == LEADER {
		isLeader = true
	}
	if isLeader == false { // return false
		rf.mu.Unlock()
		//DebugOutput(dInfo, "S%d T%d RTC_NLD index:%d", rf.me, rf.currentTerm, index)
		return len(rf.log), term, isLeader
	}
	// Your code here (2B).
	DebugOutput(dEntr, "S%d T%d add EnT at %d", rf.me, rf.currentTerm, len(rf.log))
	rf.log = append(rf.log, Entries{
		Command: command,
		Term:    term,
	})
	index = len(rf.log) - 1
	rf.mu.Unlock()
	// wait 10 round to see, if the leader changed.
	// if changed, act as me is not leader
	// if committed, there must be committed
	// if timeout, there may be committed, may wrong.
	// TODO
	// this version is best-effort, but is too slow..
	// the question is, if we don't wait, we must prove
	// the msg can be commit finally (even if the leader changed)
	//
	ccc := 1 // at most check
	for rf.killed() == false && ccc > 0 {
		rf.mu.Lock()
		term = rf.currentTerm
		if rf.commitIndex >= index { // must success
			DebugOutput(dInfo, "S%d T%d RTC_SUSS index:%d", rf.me, rf.currentTerm, index)
			rf.mu.Unlock()
			return index, term, isLeader
		}
		if rf.role != LEADER { // not leader now, fail, act as not leader before
			DebugOutput(dInfo, "S%d T%d RTC_NLD index:%d", rf.me, rf.currentTerm, index)
			rf.log = rf.log[:index]
			rf.mu.Unlock()
			return index, term, false
		}
		if command != rf.log[index].Command { // something got wrong...
			rf.log = rf.log[:index] // undo this log
			DebugOutput(dInfo, "S%d T%d RTC_WRONG index:%d", rf.me, rf.currentTerm, index)
			rf.mu.Unlock()
			return index, term, false
		}
		rf.mu.Unlock()
		ccc--
		ms := 25 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = false
	if rf.role == LEADER {
		isLeader = true
	}
	// may success, who knows?
	DebugOutput(dInfo, "S%d T%d RTC_TOT index:%d", rf.me, rf.currentTerm, index)
	rf.mu.Unlock()
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

func (rf *Raft) TryRequestVote(server int) {
	rf.mu.Lock()
	term := rf.currentTerm
	defer rf.mu.Unlock()
	if rf.role != CANDIDATE {
		return
	}
	DebugOutput(dInfo, "S%d T%d start sendRV to %d", rf.me, rf.currentTerm, server)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	reply := RequestVoteReply{}
	rf.mu.Unlock()
	ok := rf.sendRequestVote(server, &args, &reply)
	rf.mu.Lock()
	if ok {
		// Rule 2 for all servers
		if reply.Term > rf.currentTerm {
			rf.role = FOLLOWER
			DebugOutput(dRole, "S%d T%d down to follower T%d<T%d", rf.me, rf.currentTerm, rf.currentTerm, reply.Term)
			rf.currentTerm = reply.Term
			rf.ClearVoteMap()
		}
		// first check is to prove the term not changed, because line 357
		// release the lock, the term may change, then the vote is from the
		// last term, should not count as this term
		if term == rf.currentTerm && reply.VoteGranted { // got vote
			rf.gotVotesMap[server] = true
		}
		cnt := 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.gotVotesMap[i] {
				cnt++
			}
		}
		if rf.role != LEADER && cnt > 1 && cnt > (len(rf.peers)/2) { // become leader, only one node can't be leader
			DebugOutput(dRole, "S%d become T%d leader", rf.me, rf.currentTerm)
			rf.role = LEADER
			// init lastIndex and matchIndex
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) // lastest index + 1
				rf.matchIndex[i] = 0
			}
			rf.mu.Unlock()
			// try to send hb immediately
			for i := 0; i < len(rf.peers); i++ {
				go rf.TrySendHB(i)
			}
			rf.mu.Lock() // reget the lock
		}
	} else {
		DebugOutput(dError, "S%d SendRV to %d error", rf.me, server)
	}
}

func (rf *Raft) TrySendHB(server int) {
	rf.mu.Lock()
	DebugOutput(dHERT, "S%d T%d SendHB to %d", rf.me, rf.currentTerm, server)
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm, // should prove it's leader's term
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		Entries:      []Entries{},
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock() // release the lock, before wait
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.role = FOLLOWER
			DebugOutput(dRole, "S%d T%d down to follower T%d<T%d", rf.me, rf.currentTerm, rf.currentTerm, reply.Term)
			rf.ClearVoteMap()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
	} else {
		DebugOutput(dError, "S%d T%d SendHB to %d error", rf.me, rf.currentTerm, server)
	}
}

func (rf *Raft) TrySendRP(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	if rf.matchIndex[server] == len(rf.log)-1 { // all matched, no need to sync
		return
	}
	lastTerm := rf.currentTerm
	// [nextIndex, index] is new area
	// nextIndex may larger than index, which means entries are empty, just for check
	// entry[0, index - nextIndex] is actually log[nextIndex, index] 's area
	var tmpEntries []Entries
	for i := rf.nextIndex[server]; i < len(rf.log); i++ {
		tmpEntries = append(tmpEntries, rf.log[i])
	}
	var tmpTerm int
	if rf.nextIndex[server]-1 < len(rf.log) {
		tmpTerm = rf.log[rf.nextIndex[server]-1].Term
	} else { // TODO can't happen?
		DebugOutput(dError, "S%d T%d Bad happen", rf.me, rf.currentTerm)
		tmpTerm = -1
	}
	DebugOutput(dREPL, "S%d T%d Send RP to %d", rf.me, rf.currentTerm, server)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  tmpTerm,
		Entries:      tmpEntries,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	rf.mu.Unlock() // release the lock, before wait
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.role = FOLLOWER
			DebugOutput(dRole, "S%d T%d down to follower T%d<T%d", rf.me, rf.currentTerm, rf.currentTerm, reply.Term)
			rf.ClearVoteMap()
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
		// check still the term
		if rf.currentTerm == lastTerm {
			// handle the reply value of RPC
			if reply.Success {
				rf.nextIndex[server] = len(rf.log)
				rf.matchIndex[server] = len(rf.log) - 1 // all matched..
				DebugOutput(dInfo, "S%d T%d sync ID:%d to %d", rf.me, rf.currentTerm,
					server, len(rf.log)-1)
			} else {
				rf.nextIndex[server] = reply.Index // wait for next turn to send more data...
			}
		}

	} else {
		DebugOutput(dError, "S%d T%d SendRP to %d error", rf.me, rf.currentTerm, server)
	}
}

// for follower's to become candidate
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if len(rf.log) == 0 { // first, sleep
			//rf.log = append(rf.log, IdleEntry)
			rf.mu.Unlock()
		} else {
			if rf.IfElectionTimeout() == true { // start election, 3 roles can start election
				DebugOutput(dInfo, "S%d start election", rf.me)
				// increase term
				rf.currentTerm++
				rf.votedFor = rf.me
				// change role
				rf.role = CANDIDATE
				// clear the votesMap for the new term
				rf.ClearVoteMap()
				// vote for self
				rf.gotVotesMap[rf.me] = true
				// reset election timer
				rf.lastTouchedTime = time.Now()
				rf.ResetElectionTimeout()
				// send RPCs to all other servers
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go rf.TryRequestVote(i)
				}
			} else {
				rf.mu.Unlock()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// only leader will send hb
func (rf *Raft) sendHB() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == LEADER {
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				go rf.TrySendHB(i)
			}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		// TODO What's the suitable hb time?
		// [25, 200]
		ms := 25 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) TryReplica() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == LEADER {
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me { // other
					go rf.TrySendRP(i)
				}
			}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		// TODO What's the suitable replica time?
		ms := 25 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) IncreaseCommitIndex() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.role == LEADER {
			var nextIndex int
			L := rf.commitIndex
			R := len(rf.log) - 1
			for nextIndex = R; nextIndex > L; nextIndex-- {
				cnt := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= nextIndex {
						cnt++
					}
				}
				// TODO, what's the marjority?
				if cnt*2 >= len(rf.peers)-1 && rf.log[nextIndex].Term == rf.currentTerm {
					DebugOutput(dCMIT, "S%d T%d LD_CMIT %d", rf.me, rf.currentTerm, nextIndex)
					rf.commitIndex = nextIndex
					break
				}
			}
		}
		rf.mu.Unlock()
		// TODO What's the suitable check time?
		ms := 25 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) TryApply(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			// apply rf.lastApplied+1
			DebugOutput(dAPPL, "S%d T%d apply %d", rf.me, rf.currentTerm, rf.lastApplied+1)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.lastApplied++
			applyCh <- msg // send msg
		}
		rf.mu.Unlock()
		// TODO What's the suitable check time?
		ms := 25 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) IfElectionTimeout() bool {
	now := time.Now()
	return now.Sub(rf.lastTouchedTime) > rf.electionTimeout
}

// special condition:
// there may be a condition, leader's commitIndex increased, the log replicated, but the nextHB not come
// to the followers, causing the follower's commitIndex not increased. Then one of the followers become
// leader, which cause the nextLeader's commitIndex < lastLeader's commitIndex
// the leader can't increase it's commitIndex until next whole round RP and IncreaseCommitIndex...
// The simply solution is, let electionTimeout much longer, which is enough to send and receive next
// HB, to let follower's commitIndex increase.
func (rf *Raft) ResetElectionTimeout() {
	ms := 300 + (rand.Int63() % 150) // [300, 450)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
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
	// ******************
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.role = FOLLOWER
	rf.lastApplied = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.gotVotesMap = append(rf.gotVotesMap, false)
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.log = append(rf.log, IdleEntry)
	rf.ResetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start heartbeat goroutine, only useful for leader
	go rf.sendHB()
	// start replica goroutine, try to replica to followers
	go rf.TryReplica()
	// start increase commit gorountine
	go rf.IncreaseCommitIndex()
	// start apply goroutine
	go rf.TryApply(applyCh)
	return rf
}
