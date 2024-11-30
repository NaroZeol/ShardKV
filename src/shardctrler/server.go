package shardctrler

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int
	persister    *raft.Persister

	configs       []Config // indexed by config num
	ckSessions    map[int64]Session
	logRecord     map[int]Op
	confirmMap    map[int]bool
	lastApplied   int
	snapshotIndex int
}

type Session struct {
	LastOp      Op
	LastOpVaild bool
	LastOpIndex int
}

type Op struct {
	Type   string
	Number int64
	// content ?
	ReqNum int64
	CkId   int64
}

type Snapshot struct {
	Configs    []Config
	CkSessions map[int64]Session
	Maker      int // for debug
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.handleNomalRPC(args, reply, OT_Join)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.handleNomalRPC(args, reply, OT_Leave)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.handleNomalRPC(args, reply, OT_Move)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.handleNomalRPC(args, reply, OT_Query)
}

func (sc *ShardCtrler) handleNomalRPC(args GenericArgs, reply GenericReply, opType string) {
	sc.mu.Lock()

	if session := sc.ckSessions[args.getId()]; session.LastOpVaild && session.LastOp.ReqNum == args.getReqNum() {
		if op, ok := sc.logRecord[session.LastOpIndex]; ok && op.Number == session.LastOp.Number {
			// TODO: success reply
			sc.mu.Unlock()
			return
		} // else start a new operation
	}

	op := Op{}
	// TODO: op initializations for different opType

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.mu.Unlock()
		return
	} else {
		// TODO: log for start
	}
	sc.mu.Unlock()

	sc.waittingForCommit(op, index, args, reply, opType)
}

func (sc *ShardCtrler) waittingForCommit(op Op, index int, args GenericArgs, reply GenericReply, opType string) {
	startTime := time.Now()
	for !sc.killed() {
		sc.mu.Lock()
		if index <= sc.lastApplied {
			finishedOp, ok := sc.logRecord[index]
			if ok && finishedOp.Number == op.Number {
				// TODO: create success reply
				sc.mu.Unlock()
				return
			} else if ok && finishedOp.Number != op.Number {
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()

		if time.Since(startTime) > 30*time.Millisecond {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (sc *ShardCtrler) handleApplyMsg() {
	for applyMsg := range sc.applyCh {
		if applyMsg.CommandValid {
			sc.mu.Lock()
			op := applyMsg.Command.(Op)
			if sc.lastApplied+1 != applyMsg.CommandIndex {
				// TODO: log for "apply out of order"
			}

			sc.logRecord[applyMsg.CommandIndex] = op
			sc.lastApplied = applyMsg.CommandIndex

			// stable operation, don't change state machine
			if session := sc.ckSessions[op.CkId]; session.LastOpVaild && session.LastOpIndex < applyMsg.CommandIndex &&
				op.ReqNum <= session.LastOp.ReqNum {
				DPrintf("[Server][%v] stable operation #%v for [%v] ($%v <= $%v), do not change state machine", sc.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, session.LastOp.ReqNum)

				session.LastOp = op
				session.LastOpIndex = applyMsg.CommandIndex
				session.LastOpVaild = true
				sc.ckSessions[op.CkId] = session
				sc.mu.Unlock()
				continue
			}

			s := sc.ckSessions[op.CkId]
			s.LastOp = op
			s.LastOpIndex = applyMsg.CommandIndex
			s.LastOpVaild = true
			sc.ckSessions[op.CkId] = s

			switch op.Type {
			// TODO: actions
			default:
				log.Fatalf("[Server][%v] wrong switch value", sc.me)
			}

			if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate {
				DPrintf("[Server][%v] %v >= %v try to create snapshot up to #%v", sc.persister.RaftStateSize(), sc.maxraftstate, sc.me, applyMsg.CommandIndex)
				for key := range sc.logRecord {
					if sc.confirmMap[key] {
						delete(sc.logRecord, key)
						delete(sc.confirmMap, key)
					}
				}

				newSnapshot := Snapshot{
					Configs:    sc.configs,
					CkSessions: sc.ckSessions,
					Maker:      sc.me,
				}
				buffer := new(bytes.Buffer)
				encoder := labgob.NewEncoder(buffer)
				encoder.Encode(newSnapshot)

				sc.rf.Snapshot(applyMsg.CommandIndex, buffer.Bytes())
				DPrintf("[Server][%v] create snapshot up to #%v successfully", sc.me, applyMsg.CommandIndex)
			}
			sc.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			sc.mu.Lock()
			DPrintf("[Server][%v] try to apply snapshot up to #%v", sc.me, applyMsg.SnapshotIndex)
			buffer := bytes.NewBuffer(applyMsg.Snapshot)
			decoder := labgob.NewDecoder(buffer)
			snapshot := Snapshot{}
			decoder.Decode(&snapshot)

			sc.configs = snapshot.Configs
			sc.ckSessions = snapshot.CkSessions
			sc.lastApplied = applyMsg.SnapshotIndex

			DPrintf("[Server][%v] apply snapshot up to #%v successfully, maker [%v]", sc.me, applyMsg.SnapshotIndex, snapshot.Maker)
			sc.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = 1024
	sc.persister = persister

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.confirmMap = make(map[int]bool, 0)
	sc.ckSessions = make(map[int64]Session)
	sc.logRecord = make(map[int]Op)

	sc.snapshotIndex = 0
	sc.lastApplied = 0

	return sc
}
