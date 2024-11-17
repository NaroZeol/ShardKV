package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	id      int64
	leader  int
	reqNum  int64
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
	ck.reqNum = 0

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
		Key:    key,
		Id:     ck.id,
		ReqNum: ck.reqNum,
	}
	reply := GetReply{}
	ck.reqNum += 1

	serverNum := ck.leader
	retryCount := 0
	for ; ; serverNum = (serverNum + 1) % len(ck.servers) { // emm, is serial request ok ?
		if retryCount == len(ck.servers) {
			retryCount = 0
			time.Sleep(20 * time.Millisecond)
		}
		retryCount += 1

		reply = GetReply{}
		ok := ck.servers[serverNum].Call("KVServer."+"Get", &args, &reply)
		if ok && reply.Err == "" {
			ck.leader = serverNum
			DPrintf("[Client][%v] Get(%v) from Server [%v] sucessfully, Value: %v", ck.id, key, serverNum, reply.Value)
			break
		}

		if !ok {
			DPrintf("[Client][%v] failed to connect to server %v", ck.id, serverNum)
			continue
		}
		if reply.Err != ERR_NotLeader { // ignore error: not leader
			DPrintf("[Client][%v] server [%v] reply with err: %v", ck.id, serverNum, reply.Err)
		}
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
		Key:    key,
		Value:  value,
		Id:     ck.id,
		ReqNum: ck.reqNum,
	}
	reply := PutAppendReply{}
	ck.reqNum += 1

	serverNum := ck.leader
	retryCount := 0
	for ; ; serverNum = (serverNum + 1) % len(ck.servers) { // emm, is serial request ok ?
		if retryCount == len(ck.servers) {
			retryCount = 0
			time.Sleep(20 * time.Millisecond)
		}
		retryCount += 1

		reply = PutAppendReply{}
		ok := ck.servers[serverNum].Call("KVServer."+op, &args, &reply)
		if ok && reply.Err == ERR_OK {
			ck.leader = serverNum
			DPrintf("[Client][%v] PutAppend(%v, %v) to Server [%v] sucessfully", ck.id, key, value, serverNum)
			break
		}

		if !ok {
			DPrintf("[Client][%v] failed to connect to server %v", ck.id, serverNum)
			continue
		}
		if reply.Err != ERR_NotLeader { // ignore error: not leader
			DPrintf("[Client][%v] server [%v] reply with err: %v", ck.id, serverNum, reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
