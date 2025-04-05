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
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/rpcwrapper"
	"6.5840/rpcwrapper/grpc/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type AppendEntriesArgs raft_grpc.AppendEntriesArgs
type AppendEntriesReply raft_grpc.AppendEntriesReply
type RequestVoteArgs raft_grpc.RequestVoteArgs
type RequestVoteReply raft_grpc.RequestVoteReply
type InstallSnapshotArgs raft_grpc.InstallSnapshotArgs
type InstallSnapshotReply raft_grpc.InstallSnapshotReply

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
	Command      []byte
	CommandIndex int64

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int64
	SnapshotIndex int64
}

type logEntry struct {
	Term    int64
	Type    int64
	Index   int64
	Command []byte
}

func EncodeLogEntries(entries []logEntry) []byte {
	b := new(bytes.Buffer)
	e := gob.NewEncoder(b)
	e.Encode(entries)
	return b.Bytes()
}

func DecodeLogEntries(b []byte) []logEntry {
	entries := []logEntry{}
	r := bytes.NewBuffer(b)
	d := gob.NewDecoder(r)
	d.Decode(&entries)
	return entries
}

// A Go object implementing a single Raft peer.
type Raft struct {
	raft_grpc.UnimplementedRaftServer

	mu        sync.Mutex              // Lock to protect shared access to this peer's state
	peers     []*rpcwrapper.ClientEnd // RPC end points of all peers
	persister *Persister              // Object to hold this peer's persisted state
	me        int64                   // this peer's index into peers[]
	dead      int32                   // set by Kill()

	// Persistent state on all servers
	state       raftState
	currentTerm int64
	log         []logEntry
	votedFor    *int64

	// Snapshot
	snapshot      []byte
	snapshotIndex int64
	snapshotTerm  int64

	// Volatile state on all servers
	commitIndex int64
	lastApplied int64

	// Volatile state on leaders
	nextIndex  []int64
	matchIndex []int64

	// HeartBeat
	lastHeartBeat time.Time

	// Apply
	applyCh    chan ApplyMsg
	applyQueue ApplyQueue

	// Cond
	// To avoid livelock, signal these conditon variables when heartbeat and Kill()
	syncCond    sync.Cond // signal every time new operation start
	commitCond  sync.Cond // signal every time matchIndex changes
	enqueueCond sync.Cond // signal every time commitIndex changes
	applyCond   sync.Cond // signal every time applyQueue changes

	// clientends
	peersConns []*grpc.ClientConn
}

// Convert externalIndex to internalIndex
//
// externalIndex:
// the index upper application send to raft
// and the index raft send to upper applicaion
//
// internalIndex:
// the global index raft using inside raft's inner function
func (rf *Raft) internalIndex(externalIndex int64) int64 {
	index := (int64)(0)
	cnt := (int64)(0)
	target := externalIndex - rf.snapshotIndex
	for i := range rf.log {
		i := int64(i)
		index = i
		if rf.log[i].Type != LT_Noop {
			cnt += 1
		}
		if cnt == target {
			break
		}
	}
	return rf.globalIndex(index)
}

// Convert internalIndex to externalIndex
func (rf *Raft) externalIndex(internalIndex int64) int64 {
	index := (int64)(0)
	tartget := internalIndex - rf.log[0].Index
	for i := (int64)(1); i <= tartget; i++ {
		if rf.log[i].Type != LT_Noop {
			index += 1
		}
	}
	return index + rf.snapshotIndex
}

func (rf *Raft) localIndex(globalIndex int64) int64 {
	return globalIndex - rf.log[0].Index
}

func (rf *Raft) globalIndex(localIndex int64) int64 {
	return localIndex + rf.log[0].Index
}

func (rf *Raft) globalLogLen() int64 {
	return int64(len(rf.log)) + rf.log[0].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int64, bool) {

	var term int64
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.state)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	if rf.votedFor == nil {
		e.Encode(-1)
	} else {
		e.Encode(*rf.votedFor)
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	DPrintf("[%v] try to readPersist", rf.me)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var _state raftState
	var _currentTerm int64
	var _log []logEntry
	var _snapshotIndex int64
	var _snapshotTerm int64
	var _votedFor *int64

	if d.Decode(&_state) != nil ||
		d.Decode(&_currentTerm) != nil ||
		d.Decode(&_log) != nil ||
		d.Decode(&_snapshotIndex) != nil ||
		d.Decode(&_snapshotTerm) != nil {
		DPrintf("[%v] readPersist failed", rf.me)
		return
	}
	var tmp int64
	if d.Decode(&tmp) != nil {
		DPrintf("[%v] readPersist failed", rf.me)
		return
	}
	if tmp == -1 {
		_votedFor = nil
	} else {
		_votedFor = &tmp
	}

	rf.state = _state
	rf.currentTerm = _currentTerm
	rf.log = _log
	rf.votedFor = _votedFor
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.snapshotIndex = _snapshotIndex
	rf.snapshotTerm = _snapshotTerm

	rf.lastApplied = rf.log[0].Index
	DPrintf("[%v] readPersist succeed, restart in Term %v as state %v", rf.me, rf.currentTerm, rf.state)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int64, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	prevSnapshotIndex := rf.snapshotIndex
	prevIndex := rf.internalIndex(index)

	if prevSnapshotIndex >= index {
		DPrintf("[%v] refuse to make snapshot because of more up-to-date snapshotindex", rf.me)
		return
	}

	if rf.lastApplied < prevIndex {
		DPrintf("[%v] refuse to make snapshot because rf.lastApplied has changed", rf.me)
		return
	}

	DPrintf("[%v] try to create snapshot from #%v to #%v (external #%v to #%v)", rf.me, rf.log[0].Index, prevIndex, prevSnapshotIndex, index)
	rf.log = rf.log[rf.localIndex(prevIndex):]
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[0].Term

	rf.log[0].Type = LT_Noop
	rf.persist()

	// rf.lastApplied = prevIndex
	// rf.commitIndex = prevIndex
	DPrintf("[%v] create snapshot from #%v to #%v (external #%v to #%v) successfully", rf.me, rf.log[0].Index, prevIndex, prevSnapshotIndex, index)
}

func (rf *Raft) RequestVote(ctx context.Context, args *raft_grpc.RequestVoteArgs) (*raft_grpc.RequestVoteReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v] receive RequestVote from [%v] in term %v\n", rf.me, args.CandidateId, args.Term)

	reply := &raft_grpc.RequestVoteReply{}

	// default return
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastLogIndex := rf.globalLogLen()
	lastLogTerm := rf.log[len(rf.log)-1].Term

	if args.Term < rf.currentTerm {
		DPrintf("[%v] refuse to vote for [%v] in term %v because of more up-to-date entry %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return reply, nil
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.state = RS_Follower
		rf.persist()

		DPrintf("[%v] receive a RequestVote with higher term, change its term to %v\n", rf.me, rf.currentTerm)
	}

	isUpToDate := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !isUpToDate {
		DPrintf("[%v] refuse to vote for [%v] in term %v because of more up-to-date log entry\n", rf.me, args.CandidateId, args.Term)
	} else if rf.votedFor != nil && *rf.votedFor != args.CandidateId {
		DPrintf("[%v] refuse to vote for [%v] in term %v because it has voted for [%v]", rf.me, args.CandidateId, args.Term, *rf.votedFor)
	} else if (rf.votedFor == nil || *rf.votedFor == args.CandidateId) && isUpToDate {
		rf.currentTerm = args.Term
		rf.votedFor = &args.CandidateId
		rf.lastHeartBeat = time.Now() // According to the paper, heart beat time should reset when voting to somebody
		rf.state = RS_Follower
		rf.persist()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("[%v] vote for [%v] in term %v\n", rf.me, args.CandidateId, args.Term)
		return reply, nil
	} else {
		log.Fatalf("[%v] reach unreachable in Raft.RequestVote\n", rf.me)
	}
	return reply, nil
}

func (rf *Raft) AppendEntries(ctx context.Context, args *raft_grpc.AppendEntriesArgs) (*raft_grpc.AppendEntriesReply, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &raft_grpc.AppendEntriesReply{}

	// decode entries
	entries := DecodeLogEntries(args.Entries)

	if len(entries) != 0 {
		DPrintf("[%v] receive AppendEntries from [%v] in term %v from #%v to #%v\n", rf.me, args.LeaderId, args.Term, entries[0].Index, entries[len(entries)-1].Index)
	} else {
		DPrintf("[%v] receive heartbeat from [%v] in term %v", rf.me, args.LeaderId, args.Term)
	}

	// State Synchronization
	if args.Term >= rf.currentTerm {
		formerState := rf.state

		if args.Term > rf.currentTerm {
			rf.votedFor = nil // reset rf.votedFor if rf.currentTerm will change to a higher one
		}
		rf.currentTerm = args.Term
		rf.state = RS_Follower
		rf.persist()
		rf.lastHeartBeat = time.Now()
		if formerState != RS_Follower {
			DPrintf("[%v] turn to follower in term %v\n", rf.me, rf.currentTerm)
		}

		reply.Success = true
		reply.Term = rf.currentTerm
	} else if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("[%v] refuce to accept AppendEntries from [%v] in term %v because of higher term %v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return reply, nil
	}

	updateCommitIndex := func() {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(args.LeaderCommit, rf.globalLogLen()-1)
			DPrintf("[%v] update commitIndex to %v", rf.me, rf.commitIndex)
			rf.enqueueCond.Signal()
		} else {
			rf.enqueueCond.Signal()
		}
	}

	// Log Synchronization
	prevLogIndex := rf.globalLogLen() - 1
	prevLogTerm := rf.log[len(rf.log)-1].Term
	lastApplied := rf.lastApplied

	reply.PrevLogIndex = prevLogIndex
	reply.PrevLogTerm = prevLogTerm
	reply.LastApplied = lastApplied

	// Good case
	if prevLogIndex == args.PrevLogIndex && prevLogTerm == args.PrevLogTerm {
		rf.log = append(rf.log, entries...)
		rf.persist()
		reply.Success = true

		// set new commit index
		updateCommitIndex()

		if len(entries) != 0 { // ignore printing heart beat message
			DPrintf("[%v] append entries from #%v to #%v", rf.me, args.PrevLogIndex+1, rf.globalLogLen()-1)
		}
		return reply, nil
	}

	// fix package from leader
	if prevLogIndex > args.PrevLogIndex && args.PrevLogIndex == rf.lastApplied {
		// prefix entries check
		i := (int64)(0)
		for ; i < (int64)(len(entries)) && rf.localIndex(i+rf.lastApplied+1) < int64(len(rf.log)); i++ {
			if rf.localIndex(i+rf.lastApplied+1) < 0 {
				i = int64(len(entries))
				log.Println("hit branch")
				break
			}
			if entries[i].Term != rf.log[rf.localIndex(i+rf.lastApplied+1)].Term {
				i = -1 // not prefix entries, let i = -1 to make condition false
				break
			}
		}
		if i == (int64(len(entries))) {
			DPrintf("[%v] refuse to use fix package because they are prefix entries", rf.me)
			DPrintf("[%v] lastApplied: #%v prevLogIndex: #%v prevLogTerm: %v commitIndex: #%v", rf.me, reply.LastApplied, reply.PrevLogIndex, reply.PrevLogTerm, rf.commitIndex)
			reply.Success = true
			updateCommitIndex()
			return reply, nil
		}

		rf.log = append(rf.log[:rf.localIndex(rf.lastApplied+1)], entries...) // left-closed and right-open interval for slice
		rf.persist()
		reply.Success = true

		// set new commit index
		updateCommitIndex()

		DPrintf("[%v] receive a fix package from [%v]", rf.me, args.LeaderId)
		DPrintf("[%v] append entries from #%v to #%v", rf.me, args.PrevLogIndex+1, rf.globalLogLen()-1)
		return reply, nil
	}

	// Bad case
	if prevLogIndex != args.PrevLogIndex || prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		if len(entries) != 0 { //ignore printing heart beat message
			DPrintf("[%v] reject to append entries from #%v to #%v with different index #%v or term %v", rf.me, args.PrevLogIndex+1, args.PrevLogIndex+int64(len(entries)), prevLogIndex, prevLogTerm)
			DPrintf("[%v] lastApplied: #%v prevLogIndex: #%v prevLogTerm: %v commitIndex: #%v", rf.me, reply.LastApplied, reply.PrevLogIndex, reply.PrevLogTerm, rf.commitIndex)
		}
		return reply, nil
	}

	// unreachable, for debug
	DPrintf("args: %+v", args)
	DPrintf("prevLogIndex: %+v, prevLogTerm: %+v, lastApplied: %+v", prevLogIndex, prevLogTerm, lastApplied)
	log.Fatalf("[%v] reach unreachable in Raft.AppendEntries", rf.me)
	return nil, nil
}

func (rf *Raft) InstallSnapshot(ctx context.Context, args *raft_grpc.InstallSnapshotArgs) (*raft_grpc.InstallSnapshotReply, error) {
	rf.mu.Lock()
	DPrintf("[%v] receive InstallSnapshot from [%v]", rf.me, args.LeaderId)

	reply := &raft_grpc.InstallSnapshotReply{}
	reply.Term = rf.currentTerm

	// check before install
	if args.Term < rf.currentTerm {
		DPrintf("[%v] refuse to install snapshot because of higher term", rf.me)
		rf.mu.Unlock()
		return reply, nil
	}
	if args.LastIncludedIndex < rf.snapshotIndex {
		DPrintf("[%v] refuse to install snapshot because of more up-to-date snapshot", rf.me)
		rf.mu.Unlock()
		return reply, nil
	}
	if args.LastIncludedIndex == rf.snapshotIndex {
		DPrintf("[%v] refuce to install snapshot because of same up-to-date snapshot", rf.me)
		rf.mu.Unlock()
		return reply, nil
	}

	// Figure 13 step 6
	if rf.localIndex(args.LastIncludedInternal) >= 0 &&
		rf.localIndex(args.LastIncludedInternal) < (int64(len(rf.log))) &&
		args.LastIncludedTerm == rf.log[rf.localIndex(args.LastIncludedInternal)].Term {
		rf.log = rf.log[rf.localIndex(args.LastIncludedInternal):]
		rf.log[0].Type = LT_Noop
		rf.log[0].Term = args.LastIncludedTerm
		rf.log[0].Command = nil
		rf.log[0].Index = args.LastIncludedInternal
	} else {
		rf.log = make([]logEntry, 0)
		// a fill entry to let rf.log start from index 1
		rf.log = append(rf.log, logEntry{
			Index:   args.LastIncludedInternal,
			Term:    args.LastIncludedTerm,
			Type:    LT_Noop,
			Command: nil,
		})
	}

	rf.state = RS_Follower
	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.persist()

	rf.lastApplied = rf.log[0].Index
	rf.commitIndex = rf.log[0].Index
	rf.enqueueCond.Signal()

	// reset state machine
	rf.applyQueue.Clean()
	rf.applyQueue.Enqueue(ApplyMsg{
		CommandValid:  false,
		Snapshot:      args.Data,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	})

	rf.applyCond.Signal()
	rf.mu.Unlock()

	DPrintf("[%v] install snapshot up to #%v (external index #%v) successfully!", rf.me, args.LastIncludedInternal, args.LastIncludedIndex)
	return reply, nil
}

func (rf *Raft) GetClientEnd(server int64) raft_grpc.RaftClient {
	if rf.peersConns[server] == nil {
		conn, err := grpc.NewClient(rf.peers[server].GetAddrAndPort(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			DPrintf("[%v] fail to create client to [%v], Err: %v", rf.me, server, err)
			return nil
		}
		rf.peersConns[server] = conn
	} else if rf.peersConns[server].GetState() == connectivity.Shutdown {
		conn, err := grpc.NewClient(rf.peers[server].GetAddrAndPort(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			DPrintf("[%v] fail to create client to [%v], Err: %v", rf.me, server, err)
			return nil
		}
		rf.peersConns[server] = conn
	}
	return raft_grpc.NewRaftClient(rf.peersConns[server])
}

func (rf *Raft) sendRequestVote(server int64, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	client := rf.GetClientEnd(server)
	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), TM_RPCTimeout)
	defer cancel()

	rep, err := client.RequestVote(ctx, (*raft_grpc.RequestVoteArgs)(args))
	if err != nil {
		DPrintf("[%v] fail to send RequestVote to [%v](%v), %v", rf.me, server, rf.peers[server].GetAddrAndPort(), err)
		return false
	}

	*reply = *(*RequestVoteReply)(rep)

	return true
}

func (rf *Raft) sendAppendEntries(server int64, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	client := rf.GetClientEnd(server)
	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), TM_RPCTimeout)
	defer cancel()

	rep, err := client.AppendEntries(ctx, (*raft_grpc.AppendEntriesArgs)(args))
	if err != nil {
		DPrintf("[%v] fail to send AppendEntries to [%v], %v", rf.me, server, err)
		return false
	}

	*reply = *(*AppendEntriesReply)(rep)
	return true
}

func (rf *Raft) sendInstallSnapshot(server int64, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	client := rf.GetClientEnd(server)
	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), TM_RPCTimeout)
	defer cancel()

	rep, err := client.InstallSnapshot(ctx, (*raft_grpc.InstallSnapshotArgs)(args))
	if err != nil {
		DPrintf("[%v] fail to send InstallSnapshot to [%v], %v", rf.me, server, err)
		return false
	}

	*reply = *(*InstallSnapshotReply)(rep)
	return true
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
func (rf *Raft) Start(command []byte) (int64, int64, bool) {
	index := int64(-1)
	term := int64(-1)
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RS_Leader {
		isLeader = false
		return index, term, isLeader
	}

	newLog := logEntry{
		Index:   rf.globalLogLen(),
		Term:    rf.currentTerm,
		Type:    LT_Normal,
		Command: command,
	}
	rf.log = append(rf.log, newLog)
	rf.persist()
	rf.nextIndex[rf.me] = rf.globalLogLen()
	rf.matchIndex[rf.me] = rf.globalLogLen() - 1

	externIndex := rf.externalIndex(rf.globalLogLen() - 1)
	term = rf.currentTerm
	isLeader = true
	DPrintf("[%v] leader append entry #%v (external #%v) to its log in term %v", rf.me, newLog.Index, externIndex, term)
	rf.syncCond.Signal()
	rf.commitCond.Signal()

	return externIndex, term, isLeader
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

	// signal each condition variable after Kill()
	// to avoid go routine leak
	rf.syncCond.Signal()
	rf.applyCond.Signal()
	rf.commitCond.Signal()
	rf.enqueueCond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		// Heart beat timeout
		if rf.state != RS_Leader && time.Since(rf.lastHeartBeat) > TM_ElectionTimeout {
			rf.state = RS_Candiate
			rf.persist()

			// Start an election, if other one win the election, state will not be Candiate
			// or voting to someone that reset heartbeat time
			rf.mu.Unlock()

			cancelToken := int32(0)
			rf.lastHeartBeat = time.Now() // reset election timer

			go rf.election(&cancelToken)
			for {
				rf.mu.Lock()
				if time.Since(rf.lastHeartBeat) > TM_ElectionTimeout || atomic.LoadInt32(&cancelToken) == 1 ||
					rf.state != RS_Candiate || rf.killed() {
					atomic.StoreInt32(&cancelToken, 1)
					break
				}
				rf.mu.Unlock()
				time.Sleep(30 * time.Millisecond)
			}
		}

		rf.mu.Unlock()
		// pause for a random amount of time
		// milliseconds.
		ms := (rand.Int63()%int64(TM_RandomWaitingTime) + int64(30*time.Millisecond))
		time.Sleep(time.Duration(ms))
	}
}

func (rf *Raft) election(cancelToken *int32) {
	rf.mu.Lock()
	rf.votedFor = &rf.me // vote for self
	rf.currentTerm = rf.currentTerm + 1
	rf.persist()
	DPrintf("[%v] start an election in term %v\n", rf.me, rf.currentTerm)

	ballotCount := int32(1)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.globalLogLen(),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	isCancel := false
	votedForTerm := rf.currentTerm

	for i := range rf.peers {
		i := int64(i)
		if i == rf.me {
			continue
		}

		go func(server int64) {
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("[%v] fail to send RequestVote to [%v]", rf.me, server)
				return
			}

			if atomic.LoadInt32(cancelToken) == 1 {
				return // cancel
			}
			// DPrintf("[%v] send RequestVote to [%v] successfully\n", votedForCandiate, num)

			if !reply.VoteGranted && reply.Term > votedForTerm { // if there is a higher term, stop election
				rf.mu.Lock()

				if !isCancel {
					atomic.StoreInt32(cancelToken, 1) // cancel
					DPrintf("[%v] stop election because [%v] has higher term, turn to follower\n", rf.me, server)
					isCancel = true
				}

				rf.mu.Unlock()
			} else if reply.VoteGranted { // granted!
				atomic.AddInt32(&ballotCount, 1)
				DPrintf("[%v] get ballot from [%v] in term %v", rf.me, server, votedForTerm)
			}
		}(i)
	}
	rf.mu.Unlock()

	for {
		ballot := atomic.LoadInt32(&ballotCount)

		if atomic.LoadInt32(cancelToken) == 1 {
			return
		}

		rf.mu.Lock()
		if rf.state == RS_Candiate && ballot >= (int32)(len(rf.peers)/2+1) { // exceed half, success
			rf.state = RS_Leader
			rf.persist()
			// Reinitialized after election
			for i := range rf.peers {
				i := int64(i)
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = rf.globalLogLen()
				rf.matchIndex[i] = 0
			}
			DPrintf("\033[31m[%v] win the election in term %v\033[0m", rf.me, votedForTerm)
			// Add an empty entry
			// Follow paper P13:
			// "Raft handles this by having each leader com-
			// mit a blank no-op entry into the log at the
			// start of its term."
			newLog := logEntry{
				Index:   rf.globalLogLen(),
				Term:    rf.currentTerm,
				Type:    LT_Noop,
				Command: nil,
			}
			rf.log = append(rf.log, newLog)
			rf.persist()

			rf.nextIndex[rf.me] = rf.globalLogLen()
			rf.matchIndex[rf.me] = rf.globalLogLen() - 1
			DPrintf("[%v] leader append an empty entry #%v in term %v", rf.me, rf.globalLogLen()-1, rf.currentTerm)
			rf.mu.Unlock()

			go rf.serveAsLeader(rf.currentTerm)
			return
		}
		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) applyEntries() {
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			rf.applyCond.Wait()
			rf.mu.Unlock()

			for rf.applyQueue.Size() != 0 && !rf.killed() {
				if applyMsg, ok := rf.applyQueue.Dequeue(); ok {
					rf.applyCh <- applyMsg
					if applyMsg.CommandValid {
						DPrintf("[%v] apply entry #%v to state machine", rf.me, applyMsg.CommandIndex)
					} else if applyMsg.SnapshotValid {
						DPrintf("[%v] apply snapshot up to #%v to state machine", rf.me, applyMsg.SnapshotIndex)
					}
				}
			}
		}
	}()

	for !rf.killed() {
		rf.mu.Lock()
		rf.enqueueCond.Wait()
		for {
			i := rf.lastApplied + 1
			if i <= rf.commitIndex && i < rf.globalLogLen() && rf.localIndex(i) > 0 && rf.log[rf.localIndex(i)].Type == LT_Normal {
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[rf.localIndex(i)].Command,
					CommandIndex:  rf.externalIndex(i),
					SnapshotValid: false,
				}
				rf.applyQueue.Enqueue(msg)
				DPrintf("[%v] enqueue entry #%v to apply queue", rf.me, msg.CommandIndex)

				rf.lastApplied = i
			} else if i <= rf.commitIndex && i < rf.globalLogLen() && rf.localIndex(i) > 0 && rf.log[rf.localIndex(i)].Type == LT_Noop {
				rf.lastApplied = i
			} else {
				break
			}
		}
		rf.mu.Unlock()

		// always signal applyCond after enqueueCond
		rf.applyCond.Signal()
	}
	DPrintf("[%v] stop applyEntries because of death", rf.me)
}

func (rf *Raft) syncEntries(cancelToken *int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// return immediately if cancelToken had been set during lock wating time
	if atomic.LoadInt32(cancelToken) == 1 {
		return
	}

	// currentIndex := len(rf.log) - 1
	for i := range rf.peers {
		i := int64(i)
		if i == rf.me {
			continue
		}

		// fix for snapshot
		realNextIndex := rf.nextIndex[i]
		if rf.nextIndex[i] < rf.globalIndex(1) {
			realNextIndex = rf.globalIndex(1)
		} else if rf.nextIndex[i] > rf.globalLogLen() {
			realNextIndex = rf.globalLogLen() // leader's last log entry (index start at 1)
		}

		// Deep copy to avoid data racing in Raft.AppendEntries()
		entriesBt := EncodeLogEntries(rf.log[rf.localIndex(realNextIndex):])

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: realNextIndex - 1,                           // Leader consider this follower's last log index
			PrevLogTerm:  rf.log[rf.localIndex(realNextIndex-1)].Term, // Leader conside this follower's last log term
			Entries:      entriesBt,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}

		newNextIndex := rf.globalLogLen()

		go func(server int64) {
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				DPrintf("[%v] fail to send AppendEntries to [%v]", rf.me, server)
				return
			}

			if atomic.LoadInt32(cancelToken) == 1 { // is cancelled
				return
			}

			if reply.Success {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.nextIndex[server] = newNextIndex          // apply success, update next index
				if rf.matchIndex[server] <= newNextIndex-1 { //increases monotonically
					rf.matchIndex[server] = newNextIndex - 1
					rf.commitCond.Signal()
				}
			} else {
				rf.handleFailedReply(server, &args, &reply, cancelToken)
			}
		}(i)
	}
}

func (rf *Raft) handleFailedReply(server int64, args *AppendEntriesArgs, reply *AppendEntriesReply, cancelToken *int32) {
	rf.mu.Lock()
	// defer rf.cond.Signal()
	// Case 0: higher term, turn to follower
	if rf.currentTerm < reply.Term {
		atomic.StoreInt32(cancelToken, 1) // Cancel proactively to avoid a rare competitive situation
		rf.currentTerm = max(rf.currentTerm, reply.Term)
		rf.state = RS_Follower

		DPrintf("[%v] get a reply from higher term %v, cancel leader tasks\n", rf.me, reply.Term)
		rf.mu.Unlock()
		return
	}

	// Case 1: same prevLogTerm, lower prevLogIndex
	// Case 2: different prevLogIndex, Leader has same prevLogTerm at prevLogIndex
	if rf.localIndex(reply.PrevLogIndex) >= 0 &&
		(reply.PrevLogTerm == args.PrevLogTerm && reply.PrevLogIndex < args.PrevLogIndex ||
			reply.PrevLogIndex != args.PrevLogIndex && reply.PrevLogIndex < rf.globalLogLen() && reply.PrevLogTerm == rf.log[rf.localIndex(reply.PrevLogIndex)].Term) {

		rf.nextIndex[server] = reply.PrevLogIndex + 1
		DPrintf("[%v] {case 1,2}: set nextIndex[%v] to reply.PrevLogIndex+1: #%v", rf.me, server, reply.PrevLogIndex+1)
		rf.mu.Unlock()
		return
	}

	// Case 3: replying with a higher PrevLogIndex than len(rf.log)
	// e.g. :
	// S0: 2 2 2 2
	// S1: 2 3 3
	// S2: 2 3
	// when S1 send #3 to S0
	// Case 4: reply lower prevLogIndex, but leader doesn't have same prevLogTerm at prevLogIndex
	// e.g. :
	// S0: 2 2 2
	// S1: 2 2 3 3
	// S2: 2 2 3
	// when S1 send #4 to S0
	if rf.localIndex(reply.LastApplied) >= 0 && rf.localIndex(reply.PrevLogIndex) >= 0 &&
		(reply.PrevLogIndex >= args.PrevLogIndex || reply.PrevLogIndex >= rf.globalLogLen() || // check first to avoid rf.log[rf.localIndex(reply.PrevLogIndex)] out of range
			(reply.PrevLogIndex < args.PrevLogIndex && reply.PrevLogTerm != rf.log[rf.localIndex(reply.PrevLogIndex)].Term)) {
		rf.nextIndex[server] = reply.LastApplied + 1
		DPrintf("[%v] {case3,4}: set nextIndex[%v] to reply.LastApplied+1: #%v", rf.me, server, reply.LastApplied+1)
		rf.mu.Unlock()
		return
	}

	// Case 5: entries which are needed to send to servers are only existed in snapshot
	if rf.localIndex(reply.PrevLogIndex) < 0 || rf.localIndex(reply.LastApplied) < 0 || rf.localIndex(reply.PrevLogIndex) < 0 {
		snapshotArgs := InstallSnapshotArgs{
			Term:                 rf.currentTerm,
			LeaderId:             rf.me,
			LastIncludedInternal: rf.log[0].Index,
			LastIncludedIndex:    rf.snapshotIndex,
			LastIncludedTerm:     rf.log[0].Term,
			Data:                 rf.snapshot,
		}
		snapshotReply := InstallSnapshotReply{}

		rf.mu.Unlock()
		DPrintf("[%v] try to send InstallSnapshot to [%v]", rf.me, server)

		ok := rf.sendInstallSnapshot(server, &snapshotArgs, &snapshotReply)
		if !ok {
			DPrintf("[%v] fail to send InstallSnapshot to [%v]", rf.me, server)
			return
		}

		if atomic.LoadInt32(cancelToken) == 1 { // is cancelled
			return
		}

		rf.mu.Lock()
		if rf.currentTerm < snapshotReply.Term {
			atomic.StoreInt32(cancelToken, 1) // Cancel proactively to avoid a rare competitive situation
			DPrintf("[%v] InstallSnapshot but get a reply from higher term %v, cancel leader tasks\n", rf.me, snapshotReply.Term)
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[server] = snapshotArgs.LastIncludedIndex + 1

		DPrintf("[%v] send InstallSnapshot to [%v] successfully", rf.me, server)
		rf.mu.Unlock()
		return
	}

	// unreachable, for debug
	DPrintf("args: %+v", args)
	DPrintf("reply: %+v", reply)
	log.Fatalf("[%v] reach unreachable in Raft.syncEntries", rf.me)
}

func (rf *Raft) commitCheck(cancelToken *int32) {
	// Commit Check
	for {
		if atomic.LoadInt32(cancelToken) == 1 || rf.killed() {
			return
		}
		rf.mu.Lock()
		rf.commitCond.Wait()
		N := rf.commitIndex + 1 // name from paper
		newCommitIndex := N
		changed := false

		for newCommitIndex < rf.globalLogLen() {
			counter := 0
			for _, index := range rf.matchIndex {
				if index >= N {
					counter += 1
				}
			}

			if counter >= len(rf.peers)/2+1 {
				newCommitIndex = N
				changed = true
			} else {
				break
			}

			N++
		}
		if changed && (rf.localIndex(newCommitIndex) < 0 || // if an entry is in snapshot, should we think it is committed?
			newCommitIndex < rf.globalLogLen() && rf.log[rf.localIndex(newCommitIndex)].Term == rf.currentTerm) {
			if rf.commitIndex != newCommitIndex {
				DPrintf("[%v] committed #%v (external #%v)", rf.me, newCommitIndex, rf.externalIndex(newCommitIndex))
			}
			rf.commitIndex = newCommitIndex
		}
		rf.mu.Unlock()

		// always signal applyCond after commitCond
		rf.enqueueCond.Signal()
	}
}

func (rf *Raft) heartBeat(cancelToken *int32) {
	for {
		if atomic.LoadInt32(cancelToken) == 1 {
			return
		}
		rf.syncCond.Signal()
		rf.commitCond.Signal()
		rf.enqueueCond.Signal()
		rf.applyCond.Signal()
		time.Sleep(TM_HeartBeatInterval)
	}
}

func (rf *Raft) serveAsLeader(term int64) {
	cancelToken := int32(0)

	go rf.commitCheck(&cancelToken)
	go rf.heartBeat(&cancelToken)

	for {
		rf.mu.Lock()
		rf.syncCond.Wait()

		if atomic.LoadInt32(&cancelToken) == 1 || rf.killed() || rf.state != RS_Leader || rf.currentTerm != term {
			atomic.StoreInt32(&cancelToken, 1)
			DPrintf("[%v] lose it's power in term %v or dead, cancel all leader tasks", rf.me, term)
			rf.mu.Unlock()
			return
		}

		go rf.syncEntries(&cancelToken)
		rf.mu.Unlock()
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
func Make(peers []*rpcwrapper.ClientEnd, me int64,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}

	// Persistent state on all servers
	rf.currentTerm = 1
	rf.votedFor = nil
	rf.log = make([]logEntry, 0)
	rf.log = append(rf.log, logEntry{
		Index:   0,
		Term:    1,
		Type:    LT_Noop,
		Command: nil, // no-op
	})

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 1 // consider that no-op entry is applied

	rf.lastHeartBeat = time.Now()
	rf.applyCh = applyCh
	rf.applyQueue = ApplyQueue{}
	rf.applyCond = *sync.NewCond(&rf.mu)
	rf.syncCond = *sync.NewCond(&rf.mu)
	rf.commitCond = *sync.NewCond(&rf.mu)
	rf.enqueueCond = *sync.NewCond(&rf.mu)

	rf.peersConns = make([]*grpc.ClientConn, len(peers))

	// initialize from state persisted before a crash
	rf.snapshot = make([]byte, 0)

	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[%v] start in Term %v, on %v", rf.me, rf.currentTerm, rf.peers[rf.me].GetAddrAndPort())

	go func() { // start server, use goroutine to fast return
		if len(rf.snapshot) != 0 {
			// ********VERY VERY IMPORTANT!!!!!!!!!!!!!!!!!!!!!!!!!*******
			// Make() must return quickly, so it should start goroutines
			// for any long-running work.
			// applyCh will receive ApplyMsg only after Make() is done
			// So we need to start goroutine to send ApplyMsg
			// Took me about 3 hours to find this problem
			DPrintf("[%v] try to rebuild state machine from snapshot", rf.me)
			rf.applyQueue.Clean()
			rf.applyQueue.Enqueue(ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.snapshotTerm,
				SnapshotIndex: rf.snapshotIndex,
			})
			rf.lastApplied = rf.log[0].Index
			DPrintf("[%v] rebuild state machine from snapshot", rf.me)
		}

		// Volatile state on leaders
		rf.nextIndex = make([]int64, len(rf.peers))
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.globalLogLen() // initialized to leader last log index + 1 (Figure 2)
		}
		rf.matchIndex = make([]int64, len(rf.peers))

		// // Always start as follower
		// rf.state = RS_Follower

		// start ticker goroutine to start elections
		go rf.ticker()
		// start applyEntries goroutine to apply committed entries
		go rf.applyEntries()

		if rf.state == RS_Leader {
			go rf.serveAsLeader(rf.currentTerm)
		}

		rf.applyCond.Signal()
		rf.mu.Unlock()
	}()

	return rf
}
