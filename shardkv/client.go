package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) (*labrpc.ClientEnd, error)

	id     int64
	reqNum int64

	groupLeader map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) (*labrpc.ClientEnd, error)) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.id = nrand()
	ck.reqNum = 1

	ck.groupLeader = make(map[int]int)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:    key,
		Id:     ck.id,
		ReqNum: ck.reqNum,
	}
	ck.reqNum += 1

	DPrintf("[SKV-C][%v] Get(%v), shard %v", ck.id, key, key2shard(key))
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			startIndex := ck.groupLeader[gid] // start from the last leader
			for i := range servers {
				si := (startIndex + i) % len(servers)

				srv, err := ck.make_end(servers[si])
				if err != nil {
					DPrintf("[SKV-C][%v] Failed to connect to Server [%v][%v]", ck.id, gid, si)
					continue
				}

				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ERR_NoKey) {
					DPrintf("[SKV-C][%v] $%v Get(%v) from Group %v sucessfully, shard %v, value: %v", ck.id, args.ReqNum, key, gid, key2shard(key), reply.Value)

					ck.groupLeader[gid] = si // update leader
					return reply.Value
				}
				if ok && (reply.Err == ERR_WrongGroup) {
					DPrintf("[SKV-C][%v] Server [%v][%v] reply with error: %v", ck.id, gid, si, reply.Err)
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && (reply.Err != OK) {
					DPrintf("[SKV-C][%v] Server [%v][%v] reply with error: %v", ck.id, gid, si, reply.Err)
					continue
				}
				if !ok {
					DPrintf("[SKV-C][%v] Connect to Server [%v][%v] timeout", ck.id, gid, si)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	// Unreachable
	// return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Id:     ck.id,
		ReqNum: ck.reqNum,
	}
	ck.reqNum += 1
	DPrintf("[SKV-C][%v] $%v %v(%v, %v), shard %v", ck.id, args.ReqNum, op, key, value, key2shard(key))

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			startIndex := ck.groupLeader[gid] // start from the last leader
			for i := range servers {
				si := (startIndex + i) % len(servers)

				srv, err := ck.make_end(servers[si])
				if err != nil {
					DPrintf("[SKV-C][%v] Failed to connect to Server [%v][%v]", ck.id, gid, si)
					continue
				}
				var reply PutAppendReply
				ok := srv.Call("ShardKV."+op, &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("[SKV-C][%v] $%v %v(%v, %v) to group %v sucessfully, shard %v", ck.id, args.ReqNum, op, key, value, gid, key2shard(key))

					ck.groupLeader[gid] = si // update leader
					return
				}
				if ok && reply.Err == ERR_WrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
				if ok && (reply.Err != OK) {
					DPrintf("[SKV-C][%v] Server [%v][%v] reply with error: %v", ck.id, gid, si, reply.Err)
					continue
				}
				if !ok {
					DPrintf("[SKV-C][%v] Connect to Server [%v][%v] timeout", ck.id, gid, si)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
