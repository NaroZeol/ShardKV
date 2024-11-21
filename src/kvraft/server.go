package kvraft

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

type Session struct {
	LastOp      Op
	LastOpVaild bool
	LastOpIndex int
}

type Op struct {
	Type   string
	Number int64
	Key    string
	Value  string // vaild if OpType is OT_Put or OT_Append
	ReqNum int64
	CkId   int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	mp          map[string]string
	ckSessions  map[int64]Session
	logRecord   map[int]Op
	confirmMap  map[int]bool
	lastApplied int

	snapShotIndex int
}

type Snapshot struct {
	Mp         map[string]string
	CkSessions map[int64]Session
	Maker      int // for debug
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.handleNormalRPC(args, reply, OT_GET)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.handleNormalRPC(args, reply, OT_PUT)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.handleNormalRPC(args, reply, OT_APPEND)
}

func (kv *KVServer) handleNormalRPC(args GenericArgs, reply GenericReply, opType string) {
	if opType != OT_GET {
		DPrintf("[Server][%v] receive request from [%v] $%v: %v(%v, %v)", kv.me, args.getId(), args.getReqNum(), opType, args.(*PutAppendArgs).Key, args.(*PutAppendArgs).Value)
	} else if opType == OT_GET {
		DPrintf("[Server][%v] receive request from [%v] $%v: Get(%v)", kv.me, args.getId(), args.getReqNum(), args.(*GetArgs).Key)
	}

	kv.mu.Lock()

	reply.setServerName(kv.me)
	// same reqNum, check if operation is correctly finished
	if session := kv.ckSessions[args.getId()]; session.LastOpVaild && session.LastOp.ReqNum == args.getReqNum() {
		if op, ok := kv.logRecord[session.LastOpIndex]; ok && op.Number == session.LastOp.Number {
			reply.setErr(ERR_OK)
			if opType != OT_GET {
				DPrintf("[Server][%v] finish op #%v: %v(%v, %v)", kv.me, session.LastOpIndex, opType, args.(*PutAppendArgs).Key, args.(*PutAppendArgs).Value)
			} else {
				reply.(*GetReply).Value = kv.mp[args.(*GetArgs).Key]
				DPrintf("[Server][%v] finish op #%v: Get(%v)", kv.me, session.LastOpIndex, args.(*GetArgs).Key)
			}

			kv.mu.Unlock()
			return
		} // else start a new operation
	}

	DPrintf("[Server][%v] try to start a new operation", kv.me)
	op := Op{}
	if opType != OT_GET {
		op = Op{
			Type:   opType,
			Number: nrand(),
			Key:    args.(*PutAppendArgs).Key,
			Value:  args.(*PutAppendArgs).Value,
			CkId:   args.getId(),
			ReqNum: args.getReqNum(),
		}
	} else if opType == OT_GET {
		op = Op{
			Type:   OT_GET,
			Number: nrand(),
			Key:    args.(*GetArgs).Key,
			Value:  "", // unuse
			CkId:   args.getId(),
			ReqNum: args.getReqNum(),
		}
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.setErr(ERR_NotLeader)
		DPrintf("[Server][%v] failed to Start(), not a leader", kv.me)
		kv.mu.Unlock()
		return
	} else {
		DPrintf("[Server][%v] Start #%v", kv.me, index)
	}
	kv.mu.Unlock()

	kv.waittingForCommit(op, index, args, reply, opType)
}

func (kv *KVServer) waittingForCommit(op Op, index int, args GenericArgs, reply GenericReply, opType string) {
	startTime := time.Now()
	reply.setErr(ERR_CommitTimeout)

	for !kv.killed() {
		kv.mu.Lock()
		if index <= kv.lastApplied {
			finishedOp, ok := kv.logRecord[index]
			if ok && finishedOp.Number == op.Number {
				reply.setErr(ERR_OK)
				if opType != OT_GET {
					DPrintf("[Server][%v] finish op #%v: %v(%v, %v)", kv.me, index, opType, args.(*PutAppendArgs).Key, args.(*PutAppendArgs).Value)
				} else {
					reply.(*GetReply).Value = kv.mp[args.(*GetArgs).Key]
					DPrintf("[Server][%v] finish op #%v: Get(%v)", kv.me, index, args.(*GetArgs).Key)
				}

				kv.mu.Unlock()
				return
			} else if ok && finishedOp.Number != op.Number {
				reply.setErr(ERR_FailedToCommit)
				DPrintf("[Server][%v] failed to commit op #%v", kv.me, index)
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()

		if time.Since(startTime) > 30*time.Millisecond {
			reply.setErr(ERR_CommitTimeout)
			DPrintf("[Server][%v] commit timeout op #%v", kv.me, index)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) handleApplyMsg() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid {
			kv.mu.Lock()
			op := applyMsg.Command.(Op)
			if kv.lastApplied+1 != applyMsg.CommandIndex {
				log.Fatalf("[Server][%v] apply out of order, expect #%v, got #%v", kv.me, kv.lastApplied+1, applyMsg.CommandIndex)
			}

			kv.logRecord[applyMsg.CommandIndex] = op
			kv.lastApplied = applyMsg.CommandIndex

			// stable operation, don't change state machine
			if session := kv.ckSessions[op.CkId]; session.LastOpVaild && session.LastOpIndex < applyMsg.CommandIndex &&
				op.ReqNum <= session.LastOp.ReqNum {
				DPrintf("[Server][%v] stable operation #%v for [%v] ($%v <= $%v), do not change state machine", kv.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, session.LastOp.ReqNum)

				session.LastOp = op
				session.LastOpIndex = applyMsg.CommandIndex
				session.LastOpVaild = true
				kv.ckSessions[op.CkId] = session
				kv.mu.Unlock()
				continue
			}

			s := kv.ckSessions[op.CkId]
			s.LastOp = op
			s.LastOpIndex = applyMsg.CommandIndex
			s.LastOpVaild = true
			kv.ckSessions[op.CkId] = s

			switch op.Type {
			case OT_GET:
				DPrintf("[Server][%v] apply op #%v for [%v] with $%v: Get(%v)", kv.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, op.Key)
			case OT_PUT:
				kv.mp[op.Key] = op.Value
				DPrintf("[Server][%v] apply op #%v for [%v] with $%v: Put(%v, %v)", kv.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, op.Key, op.Value)
			case OT_APPEND:
				kv.mp[op.Key] = kv.mp[op.Key] + op.Value
				DPrintf("[Server][%v] apply op #%v for [%v] with $%v: Append(%v, %v)", kv.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, op.Key, op.Value)
			default:
				log.Fatalf("[Server][%v] wrong switch value", kv.me)
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				DPrintf("[Server][%v] %v >= %v try to create snapshot up to #%v", kv.persister.RaftStateSize(), kv.maxraftstate, kv.me, applyMsg.CommandIndex)
				for key := range kv.logRecord {
					if kv.confirmMap[key] {
						delete(kv.logRecord, key)
						delete(kv.confirmMap, key)
					}
				}

				newSnapshot := Snapshot{
					Mp:         kv.mp,
					CkSessions: kv.ckSessions,
					Maker:      kv.me,
				}
				buffer := new(bytes.Buffer)
				encoder := labgob.NewEncoder(buffer)
				encoder.Encode(newSnapshot)

				kv.rf.Snapshot(applyMsg.CommandIndex, buffer.Bytes())
				DPrintf("[Server][%v] create snapshot up to #%v successfully", kv.me, applyMsg.CommandIndex)
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			DPrintf("[Server][%v] try to apply snapshot up to #%v", kv.me, applyMsg.SnapshotIndex)
			buffer := bytes.NewBuffer(applyMsg.Snapshot)
			decoder := labgob.NewDecoder(buffer)
			snapshot := Snapshot{}
			decoder.Decode(&snapshot)

			kv.mp = snapshot.Mp
			kv.ckSessions = snapshot.CkSessions
			kv.lastApplied = applyMsg.SnapshotIndex

			DPrintf("[Server][%v] apply snapshot up to #%v successfully, maker [%v]", kv.me, applyMsg.SnapshotIndex, snapshot.Maker)
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()

	// close(kv.applyCh) // Emm...Let GC to close maybe better
	DPrintf("[Server][%v] Killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mp = make(map[string]string)
	kv.confirmMap = make(map[int]bool)
	kv.ckSessions = make(map[int64]Session)
	kv.logRecord = make(map[int]Op)

	kv.snapShotIndex = 0
	kv.lastApplied = 0

	// debug
	go func() {
		for !kv.killed() {
			term, isLeader := kv.rf.GetState()
			DPrintf("[Server][%v] State report: {term: %v, isLeader: %v}", kv.me, term, isLeader)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	go kv.handleApplyMsg()

	return kv
}
