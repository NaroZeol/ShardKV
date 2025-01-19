package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	id      int64
	reqNum  int64

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
	ck.reqNum = 1
	ck.leader = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	args := &QueryArgs{
		Num:    num,
		ReqNum: ck.reqNum,
		Id:     ck.id,
	}
	ck.reqNum += 1
	ck.mu.Unlock()

	for {
		// try each known server.
		startIndex := ck.leader
		for i := range ck.servers {
			num := (startIndex + i) % len(ck.servers)
			srv := ck.servers[num]

			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == ERR_OK {
				ck.leader = num
				return reply.Config
			} else if reply.Err != ERR_OK {
				DPrintf("[SC-C][%v] Server [%v] reply with error: %v", ck.id, num, reply.Err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	args := &JoinArgs{
		Servers: servers,
		Id:      ck.id,
		ReqNum:  ck.reqNum,
	}
	ck.reqNum += 1
	ck.mu.Unlock()

	for {
		// try each known server.
		startIndex := ck.leader
		for i := range ck.servers {
			num := (startIndex + i) % len(ck.servers)
			srv := ck.servers[num]

			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == ERR_OK {
				ck.leader = num
				return
			} else if reply.Err != ERR_OK {
				DPrintf("[SC-C][%v] Server [%v] reply with error: %v", ck.id, num, reply.Err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	args := &LeaveArgs{
		GIDs:   gids,
		Id:     ck.id,
		ReqNum: ck.reqNum,
	}
	ck.reqNum += 1
	ck.mu.Unlock()

	for {
		// try each known server.
		startIndex := ck.leader
		for i := range ck.servers {
			num := (startIndex + i) % len(ck.servers)
			srv := ck.servers[num]

			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == ERR_OK {
				ck.leader = num
				return
			} else if reply.Err != ERR_OK {
				DPrintf("[SC-C][%v] Server [%v] reply with error: %v", ck.id, num, reply.Err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	args := &MoveArgs{
		Shard:  shard,
		GID:    gid,
		Id:     ck.id,
		ReqNum: ck.reqNum,
	}
	ck.reqNum += 1
	ck.mu.Unlock()

	for {
		// try each known server.
		startIndex := ck.leader
		for i := range ck.servers {
			num := (startIndex + i) % len(ck.servers)
			srv := ck.servers[num]

			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == ERR_OK {
				ck.leader = num
				return
			} else if reply.Err != ERR_OK {
				DPrintf("[SC-C][%v] Server [%v] reply with error: %v", ck.id, num, reply.Err)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
