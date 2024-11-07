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

type Op struct {
	OpNum           int64
	OpType          int
	OpGetArgs       GetArgs       // vaild if OpType is "Get"
	OpPutAppendArgs PutAppendArgs // vaild if OpType is "Put" or "Append"
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	mp    map[string]string
	opnum int64
	log   []int64 // only record OpNum, index start with 1
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	DPrintf("[Server][%v] receive request: Get(%v)", kv.me, args.Key)

	kv.mu.Lock()
	op := Op{
		OpNum:     kv.opnum,
		OpType:    OT_GET,
		OpGetArgs: *args,
	}

	// well, should Get() need to reach agreement?
	// Hints say yes, but...
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ERR_NotLeader
		DPrintf("[Server][%v] refuse to request because it is not leader", kv.me)
		kv.mu.Unlock()
		return
	}
	kv.opnum += 1
	kv.mu.Unlock()

	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Lock()
		if index <= len(kv.log)-1 {
			if kv.log[index] == op.OpNum {
				reply.Err = ERR_OK
				reply.Value = kv.mp[args.Key]
				DPrintf("[Server][%v] finish op #%v: Get(%v)", kv.me, op.OpNum, args.Key)
			} else if kv.log[index] != op.OpNum {
				reply.Err = ERR_FailedToCommit
				DPrintf("[Server][%v] failed to commit op %v: Get(%v)", kv.me, op.OpNum, args.Key)
			}

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		if time.Since(startTime) > 100*time.Millisecond {
			reply.Err = ERR_CommitTimeout
			DPrintf("[Server][%v] commit timeout op #%v: Get(%v)", kv.me, op.OpNum, args.Key)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[Server][%v] receive request: Put(%v, %v)", kv.me, args.Key, args.Value)

	kv.mu.Lock()
	op := Op{
		OpNum:           kv.opnum,
		OpType:          OT_PUT,
		OpPutAppendArgs: *args,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ERR_NotLeader
		DPrintf("[Server][%v] refuse to request because it is not leader", kv.me)
		kv.mu.Unlock()
		return
	}
	kv.opnum += 1
	kv.mu.Unlock()

	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Lock()
		if index <= len(kv.log)-1 {
			if kv.log[index] == op.OpNum {
				reply.Err = ERR_OK
				DPrintf("[Server][%v] finish op #%v: Put(%v, %v)", kv.me, op.OpNum, args.Key, args.Value)
			} else if kv.log[index] != op.OpNum {
				reply.Err = ERR_FailedToCommit
				DPrintf("[Server][%v] failed to commit op #%v: Put(%v, %v)", kv.me, op.OpNum, args.Key, args.Value)
			}

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		if time.Since(startTime) > 100*time.Millisecond {
			reply.Err = ERR_CommitTimeout
			DPrintf("[Server][%v] commit timeout op #%v: Append(%v, %v)", kv.me, op.OpNum, args.Key, args.Value)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {

	DPrintf("[Server][%v] receive request: Appent(%v, %v)", kv.me, args.Key, args.Value)

	kv.mu.Lock()
	op := Op{
		OpNum:           kv.opnum,
		OpType:          OT_APPEND,
		OpPutAppendArgs: *args,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ERR_NotLeader
		DPrintf("[Server][%v] refuse request because it is not leader", kv.me)
		kv.mu.Unlock()
		return
	}
	kv.opnum += 1
	kv.mu.Unlock()

	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Lock()
		if index <= len(kv.log)-1 {
			if kv.log[index] == op.OpNum {
				reply.Err = ERR_OK
				DPrintf("[Server][%v] finish op #%v: Append(%v, %v)", kv.me, op.OpNum, args.Key, args.Value)
			} else if kv.log[index] != op.OpNum {
				reply.Err = ERR_FailedToCommit
				DPrintf("[Server][%v] failed to commit op #%v: Append(%v, %v)", kv.me, op.OpNum, args.Key, args.Value)
			}

			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		if time.Since(startTime) > 100*time.Millisecond {
			reply.Err = ERR_CommitTimeout
			DPrintf("[Server][%v] commit timeout op #%v: Append(%v, %v)", kv.me, op.OpNum, args.Key, args.Value)
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
					kv.mu.Unlock()
					continue
				}

				kv.log = append(kv.log, op.OpNum)
				switch op.OpType {
				case OT_GET:
					// nop
				case OT_PUT:
					kv.mp[op.OpPutAppendArgs.Key] = op.OpPutAppendArgs.Value
				case OT_APPEND:
					kv.mp[op.OpPutAppendArgs.Key] = kv.mp[op.OpPutAppendArgs.Key] + op.OpPutAppendArgs.Value
				default:
					log.Fatalf("[Server][%v] wrong switch value", kv.me)
				}
				DPrintf("[Server][%v] apply op #%v", kv.me, op.OpNum)
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
	kv.log = make([]int64, 0)
	kv.log = append(kv.log, 0) // add an initial log to make log start with 1

	go kv.handleApplyMsg()

	return kv
}
