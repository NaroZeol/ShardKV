package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.leader = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		Id:  ck.id,
	}
	reply := GetReply{}

	serverNum := ck.leader
	for ; ; serverNum = (serverNum + 1) % len(ck.servers) { // emm, is serial request ok ?
		reply = GetReply{}
		ok := ck.servers[serverNum].Call("KVServer."+"Get", &args, &reply)
		if ok && reply.Err == "" {
			ck.leader = serverNum
			DPrintf("[Client][%v] Get(%v) sucessfully, Value: %v", ck.id, key, reply.Value)
			break
		}

		if !ok {
			DPrintf("[Client][%v] failed to connect to server %v", ck.id, serverNum)
			continue
		}
		DPrintf("[Client][%v] server [%v] reply with err: %v", ck.id, serverNum, reply.Err)
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Id:    ck.id,
	}
	reply := PutAppendReply{}

	serverNum := ck.leader
	for ; ; serverNum = (serverNum + 1) % len(ck.servers) { // emm, is serial request ok ?
		reply = PutAppendReply{}
		ok := ck.servers[serverNum].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == "" {
			ck.leader = serverNum
			DPrintf("[Client][%v] PutAppend(%v, %v) sucessfully", ck.id, key, value)
			break
		}

		if !ok {
			DPrintf("[Client][%v] failed to connect to server %v", ck.id, serverNum)
			continue
		}
		DPrintf("[Client][%v] server [%v] reply with err: %v", ck.id, serverNum, reply.Err)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
