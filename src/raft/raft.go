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
	"log"
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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Term    int
	Command string
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         raftState
	currentTerm   int
	votedFor      *int
	log           []logEntry
	lastHeartBeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == RS_Leader
	rf.mu.Unlock()

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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type RequestVoteArgs struct {
	Term         int
	CandiateId   int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default return
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastLogIndex := len(rf.log)
	lastLogTerm := rf.log[len(rf.log)-1].Term

	if args.Term < rf.currentTerm {
		log.Printf("[%v] refuse to vote for [%v] in term %v because of higher term %v\n", rf.me, args.CandiateId, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.state = RS_Follower

		log.Printf("[%v] recieve a RequestVote with higher term, change its term to %v, reset votedFor to nil\n", rf.me, rf.currentTerm)
		log.Printf("[%v] turn to follower\n", rf.me)
	}

	isUpToDate := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !isUpToDate {
		log.Printf("[%v] refuse to vote for [%v] in term %v because of higher log entries\n", rf.me, args.CandiateId, args.Term)
	} else if rf.votedFor != nil && *rf.votedFor != args.CandiateId {
		log.Printf("[%v] refuse to vote for [%v] in term %v because it has voted for [%v]", rf.me, args.CandiateId, args.Term, *rf.votedFor)
	} else if (rf.votedFor == nil || *rf.votedFor == args.CandiateId) && isUpToDate {
		rf.currentTerm = args.Term
		rf.votedFor = &args.CandiateId
		rf.lastHeartBeat = time.Now() // According to the paper, heart beat time should reset when voting to somebody
		rf.state = RS_Follower

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		log.Printf("[%v] vote for [%v] in term %v\n", rf.me, args.CandiateId, args.Term)
		return
	} else {
		log.Fatalf("[%v] reach unreachable\n", rf.me)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("[%v] receive AppendEntries from [%v] in term %v\n", rf.me, args.LeaderId, args.Term)

	// State Synchronization
	if args.Term >= rf.currentTerm {
		formerState := rf.state

		rf.currentTerm = args.Term
		rf.state = RS_Follower
		if formerState != RS_Follower {
			log.Printf("[%v] turn to follower\n", rf.me)
		}

		reply.Success = true

		// Update Term
		if args.Term > rf.currentTerm {
			reply.Term = rf.currentTerm
			log.Printf("[%v] find a new leader [%v] in term %v", rf.me, args.LeaderId, args.Term)
		}
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		log.Printf("[%v] refuce to accept AppendEntries from [%v] in term %v because of higher term %v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	rf.lastHeartBeat = time.Now()
	// TODO: Apply log?
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// Heart beat timeout
		if rf.state != RS_Leader && time.Since(rf.lastHeartBeat) > TM_HeartBeatTimeout {
			log.Printf("[%v] heart beat timeout\n", rf.me)
			rf.state = RS_Candiate
			log.Printf("[%v] turn to candiate\n", rf.me)
			// randomization
			rf.mu.Unlock()
			ms := (rand.Int63() % int64(TM_RandomWaitingTime))
			time.Sleep(time.Duration(ms))
			rf.mu.Lock()

			// Start an election, if other one win the election, state will not be Candiate
			// or voting to someone that reset heartbeat time
			if rf.state != RS_Candiate || time.Since(rf.lastHeartBeat) < TM_HeartBeatTimeout {
				rf.mu.Unlock()
			} else if rf.election() { // rf.election hold lock and free when function finish
				go rf.sendHeartBeat()
			}
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) election() bool {
	// hold rf.mu
	var ballotCount = 1
	var ballotMutex = sync.Mutex{}

	rf.votedFor = &rf.me // vote for self
	rf.currentTerm = rf.currentTerm + 1
	log.Printf("[%v] start an election in term %v\n", rf.me, rf.currentTerm)

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandiateId:   rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	votedForTerm := rf.currentTerm
	votedForCandiate := rf.me
	isCancel := false

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(num int) {
			reply := RequestVoteReply{}

			// repeat sending vote request
			ok := false
			for !ok {
				rf.mu.Lock()
				isCandiate := rf.state == RS_Candiate
				isSameTerm := rf.currentTerm == votedForTerm
				rf.mu.Unlock()
				if isCandiate && isSameTerm {
					ok = rf.sendRequestVote(num, &args, &reply)
				} else {
					return
				}
			}
			// log.Printf("[%v] send RequestVote to [%v] successfully\n", votedForCandiate, num)

			if !reply.VoteGranted && reply.Term > votedForTerm { // if there is a higher term, stop election
				rf.mu.Lock()

				// only stop once
				if !isCancel {
					rf.currentTerm = reply.Term
					rf.state = RS_Follower
					rf.votedFor = nil
					log.Printf("[%v] stop election because [%v] has higher term, turn to follower\n", votedForCandiate, num)
					isCancel = true
				}

				rf.mu.Unlock()
			} else if reply.VoteGranted { // granted!
				ballotMutex.Lock()
				ballotCount += 1
				ballotMutex.Unlock()
				log.Printf("[%v] get ballot from [%v]", votedForCandiate, num)
			}
		}(i)
	}
	rf.mu.Unlock()

	var electTimer = time.Now()
	// Waiting, should finish in TM_ElectionTImeout
	for time.Since(electTimer) < TM_ElectionTimeout {
		ballotMutex.Lock()
		ballot := ballotCount
		ballotMutex.Unlock()

		rf.mu.Lock()
		if rf.state == RS_Candiate && ballot >= len(rf.peers)/2+1 { // exceed half, success
			rf.state = RS_Leader
			rf.mu.Unlock()
			log.Printf("[%v] win the election in term %v", rf.me, votedForTerm)
			return true
		} else if rf.state != RS_Candiate { // state changed due to new leader send AppendEntries or a higher term RequetVote
			rf.mu.Unlock()
			log.Printf("[%v] lose the election in term %v", rf.me, votedForTerm)
			return false
		}
		rf.mu.Unlock()

		time.Sleep(TM_ElectionTimeout / 5)
	}
	log.Printf("[%v] election timeout in term %v", votedForCandiate, votedForTerm)
	return false
}

func (rf *Raft) sendHeartBeat() {
	isCancel := false
	for {
		rf.mu.Lock()
		if rf.state != RS_Leader {
			log.Printf("[%v] stop sending heartbeat entries because it is not leader\n", rf.me)
			rf.mu.Unlock()
			break
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.log),
			PrevLogTerm:  rf.log[len(rf.log)-1].Term,
			Entries:      []logEntry{},
			// LeaderCommit: , no implementation
		}

		senderTerm := rf.currentTerm
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(num int) {
				// perhaps no lock protect
				reply := AppendEntriesReply{}

				ok := false
				for !ok {
					rf.mu.Lock()
					isLeader := rf.state == RS_Leader
					isSameTerm := rf.currentTerm == senderTerm
					rf.mu.Unlock()

					if isLeader && isSameTerm {
						ok = rf.sendAppendEntries(num, &args, &reply)
					} else {
						return
					}
				}

				rf.mu.Lock()
				if !reply.Success && reply.Term > senderTerm {
					// Cancel only once
					if !isCancel {
						rf.state = RS_Follower
						rf.votedFor = nil
						rf.currentTerm = reply.Term
						log.Printf("[%v] get a reply from higher term %v, turn to follower\n", rf.me, reply.Term)
						isCancel = true
					}
					rf.mu.Unlock()
					return
				} else if reply.Success {
					// log.Printf("[%v] send heart beat to [%v] successful!\n", rf.me, num)
				} else {
					log.Fatalf("[%v] reach unreachable in sendHeartBeat\n", rf.me)
				}
				rf.mu.Unlock()
			}(i)
		}
		rf.mu.Unlock()
		time.Sleep(TM_HeartBeatInterval) // 5 times per second
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 1
	rf.votedFor = nil
	rf.log = make([]logEntry, 0)
	rf.log = append(rf.log, logEntry{
		Term:    1,
		Command: "",
	})
	rf.mu = sync.Mutex{}
	rf.lastHeartBeat = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	log.Printf("[%v] start!!!!!\n", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
