package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Session struct {
	LastOp *Op
	CurrOp *Op
}

type Op struct {
	OpType  string
	OpIndex int
	OpNum   int64
	OpKey   string
	OpValue string // vaild if OpType is OT_Put or OT_Append
	ReqNum  int64
	CkId    int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	mp         map[string]string
	ckSessions map[int64]*Session
	opnum      int64
	log        []int64 // only record Opnum, index start with 1
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
		DPrintf("[Server][%v] receive request: %v(%v, %v)", kv.me, opType, args.(*PutAppendArgs).Key, args.(*PutAppendArgs).Value)
	} else if opType == OT_GET {
		DPrintf("[Server][%v] receive request: Get(%v)", kv.me, args.(*GetArgs).Key)
	}
	kv.mu.Lock()
	if kv.ckSessions[args.getId()] != nil && kv.ckSessions[args.getId()].CurrOp != nil { // not fisrt communication
		if kv.ckSessions[args.getId()].CurrOp.ReqNum > args.getReqNum() { // is an old request
			reply.setErr(ERR_OK)
			DPrintf("[Server][%v] completed request, return OK", kv.me)
			kv.mu.Unlock()
			return
		} else if kv.ckSessions[args.getId()].CurrOp.ReqNum == args.getReqNum() { // duplicate request
			DPrintf("[Server][%v] duplicate request, waitting for commit", kv.me)
			kv.mu.Unlock()
			kv.waittingForCommit(args, reply, opType)
			return
		}
	} else if kv.ckSessions[args.getId()] == nil { // first communication
		kv.ckSessions[args.getId()] = &Session{}
	}

	op := Op{}
	if opType != OT_GET {
		op = Op{
			OpType:  opType,
			OpNum:   kv.opnum,
			OpKey:   args.(*PutAppendArgs).Key,
			OpValue: args.(*PutAppendArgs).Value,
			CkId:    args.getId(),
			ReqNum:  args.getReqNum(),
		}
	} else if opType == OT_GET {
		op = Op{
			OpType:  OT_GET,
			OpNum:   kv.opnum,
			OpKey:   args.(*GetArgs).Key,
			OpValue: "", // unuse
			CkId:    args.getId(),
			ReqNum:  args.getReqNum(),
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
	op.OpIndex = index
	kv.opnum += 1
	kv.ckSessions[args.getId()].CurrOp = &op
	kv.mu.Unlock()

	kv.waittingForCommit(args, reply, opType)
}

func (kv *KVServer) waittingForCommit(args GenericArgs, reply GenericReply, opType string) {
	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Lock()
		op := kv.ckSessions[args.getId()].CurrOp
		if op.OpIndex <= len(kv.log)-1 {
			if kv.log[op.OpIndex] == op.OpNum {
				reply.setErr(ERR_OK)
				if opType != OT_GET {
					DPrintf("[Server][%v] finish op #%v: %v(%v, %v)", kv.me, op.OpIndex, opType, args.(*PutAppendArgs).Key, args.(*PutAppendArgs).Value)
				} else {
					reply.(*GetReply).Value = kv.mp[args.(*GetArgs).Key]
					DPrintf("[Server][%v] finish op #%v: Get(%v)", kv.me, op.OpIndex, args.(*GetArgs).Key)
				}
			} else if kv.log[op.OpIndex] != op.OpNum {
				reply.setErr(ERR_FailedToCommit)
				DPrintf("[Server][%v] failed to commit op %v", kv.me, op.OpIndex)
			}

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		if time.Since(startTime) > 200*time.Millisecond {
			reply.setErr(ERR_CommitTimeout)
			DPrintf("[Server][%v] commit timeout op #%v", kv.me, op.OpIndex)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) handleApplyMsg() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				kv.mu.Lock()

				op := applyMsg.Command.(Op)
				if len(kv.log) != applyMsg.CommandIndex {
					log.Fatalf("[Server][%v] apply out of order", kv.me)
				}

				kv.log = append(kv.log, op.OpNum)
				if kv.ckSessions[op.CkId] == nil {
					kv.ckSessions[op.CkId] = &Session{}
				}
				// stable operation, don't change state machine
				if kv.ckSessions[op.CkId].LastOp != nil && kv.ckSessions[op.CkId].LastOp.ReqNum >= op.ReqNum {
					DPrintf("[Server][%v] stable operation #%v, do not change state machine", kv.me, applyMsg.CommandIndex)
					kv.mu.Unlock()
					continue
				}
				kv.ckSessions[op.CkId].LastOp = &op
				switch op.OpType {
				case OT_GET:
					DPrintf("[Server][%v] apply op #%v: Get(%v)", kv.me, applyMsg.CommandIndex, op.OpKey)
				case OT_PUT:
					kv.mp[op.OpKey] = op.OpValue
					DPrintf("[Server][%v] apply op #%v: Put(%v, %v)", kv.me, applyMsg.CommandIndex, op.OpKey, op.OpValue)
				case OT_APPEND:
					kv.mp[op.OpKey] = kv.mp[op.OpKey] + op.OpValue
					DPrintf("[Server][%v] apply op #%v: Append(%v, %v)", kv.me, applyMsg.CommandIndex, op.OpKey, op.OpValue)
				default:
					log.Fatalf("[Server][%v] wrong switch value", kv.me)
				}
				kv.mu.Unlock()
			} else if applyMsg.SnapshotValid {
				// TODO: snapshot
				time.Sleep(0)
			}
		default:
			time.Sleep(10 * time.Millisecond)
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
	// Your code here, if desired.
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mp = make(map[string]string)
	kv.ckSessions = make(map[int64]*Session)
	kv.log = make([]int64, 0)
	kv.log = append(kv.log, 0) // add an initial log to make log start with 1

	go kv.handleApplyMsg()

	return kv
}
