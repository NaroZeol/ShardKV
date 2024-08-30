package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu     sync.Mutex
	cMu    sync.Mutex
	dMu    sync.Mutex
	mp     map[string]string
	cliMap map[int64]bool
	dupMap map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Get doesn't need any scheme to avoild duplicate operation
	// Just retutn current value
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.mp[args.Key]

	DPrintf("[Server] Get(%v) call is finished", args)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.mp[args.Key] = args.Value

	// Put operation doesn't need return old value
	DPrintf("[Server] Put(%v) call is finished", args)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	kv.cMu.Lock()
	kv.dMu.Lock()
	defer kv.mu.Unlock()
	defer kv.cMu.Unlock()
	defer kv.dMu.Unlock()
	num := kv.cliMap[args.Id]

	if num != args.Opstate { // client call for an operation which has been done
		reply.Value = kv.dupMap[args.Id]
		return
	}

	kv.cliMap[args.Id] = !kv.cliMap[args.Id]

	reply.Value = kv.mp[args.Key]
	kv.mp[args.Key] = kv.mp[args.Key] + args.Value

	kv.dupMap[args.Id] = reply.Value

	DPrintf("[Server] Append(%v) call is finished", args)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.cliMap = make(map[int64]bool)
	kv.dupMap = make(map[int64]string)
	return kv
}
