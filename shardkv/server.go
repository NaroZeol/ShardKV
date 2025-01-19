package shardkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Session struct {
	LastOpVaild bool
	LastOpIndex int

	LastOpNumber   int64
	LastOpReqNum   int64
	LastOpShardNum int
}

type Op struct {
	Type     string
	Number   int64
	ReqNum   int64
	CkId     int64
	ShardNum int

	Args interface{}
}

type ShardKV struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	make_end func(string) (*labrpc.ClientEnd, error)
	gid      int
	ctrlers  []*labrpc.ClientEnd
	mck      *shardctrler.Clerk
	config   shardctrler.Config

	dead int32

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	mp          map[string]string
	ckSessions  map[string]Session // {string(ckId+shardNum)} -> session
	logRecord   map[int]Op
	confirmMap  map[int]bool
	lastApplied int

	uid           int64
	localReqNum   int64
	shardLastNums [shardctrler.NShards]int
	shardVec      [shardctrler.NShards]bool

	snapShotIndex int
}

type Snapshot struct {
	Mp            map[string]string
	CkSessions    map[string]Session
	Config        shardctrler.Config
	ShardLastNums [shardctrler.NShards]int
	ShardVec      [shardctrler.NShards]bool
	Maker         int // for debug
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.handleNormalRPC(args, reply, OT_GET)
	return nil
}

func (kv *ShardKV) Put(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.handleNormalRPC(args, reply, OT_PUT)
	return nil
}

func (kv *ShardKV) Append(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.handleNormalRPC(args, reply, OT_APPEND)
	return nil
}

func (kv *ShardKV) RequestMapAndSession(args *RequestMapAndSessionArgs, reply *RequestMapAndSessionReply) error {
	DPrintf("[SKV-S][%v][%v] receive RPC RequestMapAndSession from [%v][%v]", kv.gid, kv.me, args.Gid, args.Me)
	replyMp := make(map[string]string)
	replySession := make(map[string]Session, 0)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ERR_WrongLeader
		return nil
	}

	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = ERR_LowerConfigNum
		kv.mu.Unlock()
		return nil
	}

	for key, value := range kv.mp {
		if args.Shards[key2shard(key)] {
			replyMp[key] = value
		}
	}
	for key, value := range kv.ckSessions {
		// Client's Id is a positive number
		if key[0] != '-' && args.Shards[value.LastOpShardNum] {
			replySession[key] = value
		}
	}
	kv.mu.Unlock()

	reply.Err = OK
	reply.Mp = replyMp
	reply.Sessions = replySession

	return nil
}

func (kv *ShardKV) DeleteShards(args *DeleteShardsArgs, reply *DeleteShardsReply) error {
	kv.handleNormalRPC(args, reply, OT_DeleteShards)
	return nil
}

func (kv *ShardKV) ApplyMovement(args *ApplyMovementArgs, reply *ApplyMovementReply) error {
	DPrintf("[SKV-S][%v][%v] Start RPC $%v ApplyMovement(%+v) ", kv.gid, kv.me, args.ReqNum, *args)
	for !kv.killed() {
		kv.handleNormalRPC(args, reply, OT_ApplyMovement)
		if reply.Err == OK {
			break
		} else if reply.Err == ERR_WrongLeader {
			DPrintf("[SKV-S][%v][%v] Local RPC ApplyMovement $%v reply with error: %v", kv.gid, kv.me, args.ReqNum, reply.Err)
			break
		} else {
			DPrintf("[SKV-S][%v][%v] Local RPC ApplyMovement $%v reply with error: %v", kv.gid, kv.me, args.ReqNum, reply.Err)
			continue
		}
	}
	return nil
}

func (kv *ShardKV) handleNormalRPC(args GenericArgs, reply GenericReply, opType string) {
	DPrintf("[SKV-S][%v][%v] receive RPC %v: %+v", kv.gid, kv.me, opType, args)

	kv.mu.Lock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.setErr(ERR_WrongLeader)
		kv.mu.Unlock()
		return
	}

	if 0 <= args.getId() && !kv.shardVec[key2shard(args.getKey())] {
		reply.setErr(ERR_WrongGroup)
		DPrintf("[SKV-S][%v][%v] reply client[%v] on shard %v with WrongGroup, ShardVec: %v", kv.gid, kv.me, args.getId(), key2shard(args.getKey()), kv.shardVec)
		kv.mu.Unlock()
		return
	}

	// uniKey = string(ckId) + string(shardNum)
	// use to identify each client's operation on different shards
	uniKey := strconv.FormatInt(args.getId(), 10) + strconv.FormatInt(int64(key2shard(args.getKey())), 10)

	if session := kv.ckSessions[uniKey]; session.LastOpVaild && session.LastOpReqNum == args.getReqNum() {
		if op, ok := kv.logRecord[session.LastOpIndex]; ok && op.Number == session.LastOpNumber {
			kv.successCommit(args, reply, opType)
			DPrintf("[SKV-S][%v][%v] reply success because [%v]$%v has completed", kv.gid, kv.me, args.getId(), args.getReqNum())
			kv.mu.Unlock()
			return
		} // else start a new operation
	}

	op := Op{
		Type:     opType,
		Number:   nrand(),
		ReqNum:   args.getReqNum(),
		CkId:     args.getId(),
		ShardNum: key2shard(args.getKey()),
	}

	switch opType {
	case OT_GET:
		op.Args = *args.(*GetArgs)
	case OT_PUT:
		op.Args = *args.(*PutAppendArgs)
	case OT_APPEND:
		op.Args = *args.(*PutAppendArgs)
	case OT_ChangeConfig:
		op.Args = *args.(*ChangeConfigArgs)
	case OT_ApplyMovement:
		op.Args = *args.(*ApplyMovementArgs)
	case OT_DeleteShards:
		op.Args = *args.(*DeleteShardsArgs)
	case OT_UpdateShardVec:
		op.Args = *args.(*UpdateShardVecArgs)
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.setErr(ERR_WrongLeader)
		DPrintf("[SKV-S][%v][%v] failed to Start(), not a leader", kv.gid, kv.me)
		kv.mu.Unlock()
		return
	} else {
		DPrintf("[SKV-S][%v][%v] Start #%v", kv.gid, kv.me, index)
	}
	kv.mu.Unlock()

	kv.waittingForCommit(op, index, args, reply, opType)
}

func (kv *ShardKV) waittingForCommit(op Op, index int, args GenericArgs, reply GenericReply, opType string) {
	startTime := time.Now()
	for !kv.killed() {
		kv.mu.Lock()
		if index <= kv.lastApplied {
			finishedOp, ok := kv.logRecord[index]
			if ok && finishedOp.Number == op.Number {
				kv.successCommit(args, reply, opType)
				kv.mu.Unlock()
				return
			} else if ok && finishedOp.Number != op.Number {
				DPrintf("[SKV-S][%v][%v] Failed to commit op #%v, wrong Op.number", kv.gid, kv.me, index)
				reply.setErr(ERR_FailedToCommit)
				kv.mu.Unlock()
				return
			}
		}
		kv.mu.Unlock()

		if time.Since(startTime) > 30*time.Millisecond {
			DPrintf("[SKV-S][%v][%v] Failed to commit op #%v, timeout", kv.gid, kv.me, index)
			reply.setErr(ERR_CommitTimeout)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (kv *ShardKV) successCommit(args GenericArgs, reply GenericReply, opType string) {
	// should hold kv.mu

	switch opType {
	case OT_GET:
		getArgs := args.(*GetArgs)
		getReply := reply.(*GetReply)

		getReply.Err = OK
		getReply.Value = kv.mp[getArgs.Key]
	case OT_PUT:
		fallthrough
	case OT_APPEND:
		putAppendReply := reply.(*PutAppendReply)
		putAppendReply.Err = OK
	case OT_ChangeConfig:
		changeConfigReply := reply.(*ChangeConfigReply)
		changeConfigReply.Err = OK
		changeConfigReply.Num = kv.config.Num
	case OT_ApplyMovement:
		applyMovementReply := reply.(*ApplyMovementReply)
		applyMovementReply.Err = OK
	case OT_DeleteShards:
		deleteShardsReply := reply.(*DeleteShardsReply)
		deleteShardsReply.Err = OK
	case OT_UpdateShardVec:
		updateShardVecReply := reply.(*UpdateShardVecReply)
		updateShardVecReply.Err = OK
	default:
		log.Fatal("Wrong switch in successCommit()")
	}
}

func (kv *ShardKV) applyOp(op Op) bool {
	// applyMsg() -> applyOp()
	// should hold kv.mu

	switch op.Type {
	case OT_GET:
		getArgs := op.Args.(GetArgs)
		if kv.shardVec[key2shard(getArgs.Key)] {
			DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: Get(%v)", kv.gid, kv.me, getArgs.Id, getArgs.ReqNum, getArgs.Key)
			return true
		} else {
			DPrintf("[SKV-S][%v][%v] Failed to apply Op: Get(%v)", kv.gid, kv.me, getArgs.Key)
			return false
		}
	case OT_PUT:
		putArgs := op.Args.(PutAppendArgs)
		if kv.shardVec[key2shard(putArgs.Key)] {
			kv.mp[putArgs.Key] = putArgs.Value
			DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: Put(%v, %v)", kv.gid, kv.me, putArgs.Id, putArgs.ReqNum, putArgs.Key, putArgs.Value)
			return true
		} else {
			DPrintf("[SKV-S][%v][%v] Failed to apply Op: Put(%v, %v)", kv.gid, kv.me, putArgs.Key, putArgs.Value)
			return false
		}
	case OT_APPEND:
		appendArgs := op.Args.(PutAppendArgs)
		if kv.shardVec[key2shard(appendArgs.Key)] {
			kv.mp[appendArgs.Key] = kv.mp[appendArgs.Key] + appendArgs.Value
			DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: Append(%v, %v)", kv.gid, kv.me, appendArgs.Id, appendArgs.ReqNum, appendArgs.Key, appendArgs.Value)
			return true
		} else {
			DPrintf("[SKV-S][%v][%v] Failed to apply Op: Append(%v, %v)", kv.gid, kv.me, appendArgs.Key, appendArgs.Value)
			return false
		}
	case OT_ChangeConfig:
		changeConfigArgs := op.Args.(ChangeConfigArgs)

		DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: ChangeConfig(%v)", kv.gid, kv.me, changeConfigArgs.Id, changeConfigArgs.ReqNum, changeConfigArgs.NewNum)
		if kv.config.Num < changeConfigArgs.Config.Num {
			kv.config = changeConfigArgs.Config
			for shard, gid := range changeConfigArgs.Config.Shards {
				if gid == kv.gid {
					kv.shardVec[shard] = true
					kv.shardLastNums[shard] = changeConfigArgs.Config.Num
				} else {
					kv.shardVec[shard] = false // do not allow this shard
				}
			}
			DPrintf("[SKV-S][%v][%v] Change config to %v successfuly", kv.gid, kv.me, changeConfigArgs.Config.Num)
			DPrintf("[SKV-S][%v][%v] ShardVec: %+v", kv.gid, kv.me, kv.shardVec)
		} else {
			DPrintf("[SKV-S][%v][%v] ChangeConfig(%v): kv.config is already %v", kv.gid, kv.me, changeConfigArgs.NewNum, kv.config.Num)
		}
		return true
	case OT_ApplyMovement:
		applyMovementArgs := op.Args.(ApplyMovementArgs)

		DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: applyMovement(%+v)", kv.gid, kv.me, applyMovementArgs.Id, applyMovementArgs.ReqNum, applyMovementArgs)
		for key, value := range applyMovementArgs.Mp {
			if kv.shardLastNums[key2shard(key)] >= applyMovementArgs.Config.Num {
				DPrintf("[SKV-S][%v][%v] Refuse to set Key: %v, Value: %v, Shard: %v because same up-to-date num", kv.gid, kv.me, key, value, key2shard(key))
				continue
			}
			kv.mp[key] = value
			DPrintf("[SKV-S][%v][%v] Set Key: %v, Value: %v, Shard: %v", kv.gid, kv.me, key, value, key2shard(key))
		}
		for uniKey, Session := range applyMovementArgs.Sessions {
			if uniKey[0] != '-' && kv.shardLastNums[Session.LastOpShardNum] >= applyMovementArgs.Config.Num {
				DPrintf("[SKV-S][%v][%v] Refuse to update ckSessions[%v] = %v", kv.gid, kv.me, uniKey, Session)
				continue
			}
			Session.LastOpIndex = -1 // temporary solution for fix, TODO, a better way
			kv.ckSessions[uniKey] = Session
			DPrintf("[SKV-S][%v][%v] update ckSessions[%v] = %v", kv.gid, kv.me, uniKey, Session)
		}

		for shard := range applyMovementArgs.Shards {
			kv.shardVec[shard] = true // allow to handle this shard
			kv.shardLastNums[shard] = applyMovementArgs.Config.Num
		}
		DPrintf("[SKV-S][%v][%v] ShardVec: %+v", kv.gid, kv.me, kv.shardVec)
		return true
	case OT_DeleteShards:
		deleteShardsArgs := op.Args.(DeleteShardsArgs)
		DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: DeleteShards(%+v)", kv.gid, kv.me, deleteShardsArgs.Id, deleteShardsArgs.ReqNum, deleteShardsArgs)

		// ensure this shard is indeed unneeded
		deletedShards := deleteShardsArgs.Shards
		for shard := range deletedShards {
			if deleteShardsArgs.ConfigNum < kv.shardLastNums[shard] {
				DPrintf("[SKV-S][%v][%v] refuse to delete shard %v because of higher shardLastNum", kv.gid, kv.me, shard)
				delete(deletedShards, shard)
			}
		}

		DPrintf("[SKV-S][%v][%v] configNum: %v, deleteConfigNum: %v, deletedShards: %+v, shardLastNums: %+v", kv.gid, kv.me, kv.config.Num, deleteShardsArgs.ConfigNum, deletedShards, kv.shardLastNums)

		for key, value := range kv.mp {
			if deletedShards[key2shard(key)] {
				DPrintf("[SKV-S][%v][%v] delete Shard: %v, Key: %v, Value: %v", kv.gid, kv.me, key2shard(key), key, value)
				delete(kv.mp, key)
			}
		}
		for uniKey, session := range kv.ckSessions {
			if uniKey[0] != '-' && deletedShards[session.LastOpShardNum] {
				DPrintf("[SKV-S][%v][%v] delete uniKey: %v, session: %+v", kv.gid, kv.me, uniKey, session)
				delete(kv.ckSessions, uniKey)
			}
		}

		DPrintf("[SKV-S][%v][%v] Map: %+v", kv.gid, kv.me, kv.mp)
		DPrintf("[SKV-S][%v][%v] Session: %+v", kv.gid, kv.me, kv.ckSessions)
		return true
	case OT_UpdateShardVec:
		updateShardVecArgs := op.Args.(UpdateShardVecArgs)
		DPrintf("[SKV-S][%v][%v] Apply Op [%v]$%v: UpdateShardVec(%+v)", kv.gid, kv.me, updateShardVecArgs.Id, updateShardVecArgs.ReqNum, updateShardVecArgs)

		if kv.config.Num < updateShardVecArgs.ConfigNum {
			kv.shardVec = updateShardVecArgs.NewVec
		}
		return true
	default:
		log.Fatal("wrong switch in applyOp")
	}

	// unreachable
	return false
}

func (kv *ShardKV) MoveShards(oldConfig shardctrler.Config, newConfig shardctrler.Config) bool {
	// should hold kv.mu

	// do nothing if it's this first config
	if oldConfig.Num == 0 {
		return true
	}

	if oldConfig.Num == newConfig.Num {
		return true
	}

	// which shard should receive from. (gid -> {set of needed shards})
	receiveFrom := make(map[int]map[int]bool, 0)

	for i := 0; i < shardctrler.NShards; i++ {
		if oldConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
			srcGid := oldConfig.Shards[i]
			// receiveFrom: gid -> set of needed shards
			if receiveFrom[srcGid] == nil {
				receiveFrom[srcGid] = make(map[int]bool)
			}
			receiveFrom[srcGid][i] = true
		}
	}
	DPrintf("[SKV-S][%v][%v] oldConfig: %+v", kv.gid, kv.me, oldConfig)
	DPrintf("[SKV-S][%v][%v] newConfig: %+v", kv.gid, kv.me, newConfig)
	DPrintf("[SKV-S][%v][%v] receiveFrom: %+v", kv.gid, kv.me, receiveFrom)

	// receive From
	wg := sync.WaitGroup{}
	sendApplyMovementLock := sync.Mutex{} // only allow single applyMovement at a moment to avoid ReqNum conflict
	applyMovementFail := int32(0)
	for gid, shards := range receiveFrom {
		wg.Add(1)
		go func(gid int, shards map[int]bool) {
			defer wg.Done()

			for !kv.killed() {
				if servers, ok := oldConfig.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv, err := kv.make_end(servers[si])
						if err != nil {
							DPrintf("[SKV-S][%v][%v] Failed to make_end(%v)", kv.gid, kv.me, servers[si])
							continue
						}

						args := RequestMapAndSessionArgs{
							Gid:       kv.gid,
							Me:        kv.me,
							Shards:    shards,
							ConfigNum: newConfig.Num,
						}
						reply := RequestMapAndSessionReply{}
						ok := srv.Call("ShardKV.RequestMapAndSession", &args, &reply)

						if ok && reply.Err == OK {
							DPrintf("[SKV-S][%v][%v] RequestMapAndSession(%+v) from Server[%v][%v] sucessfully", kv.gid, kv.me, args, gid, si)
							sendApplyMovementLock.Lock()
							kv.mu.Lock()
							moveArgs := ApplyMovementArgs{
								Mp:       reply.Mp,
								Sessions: reply.Sessions,
								Config:   newConfig,
								Id:       kv.uid,
								ReqNum:   kv.localReqNum,
								Shards:   shards,
							}
							moveReply := ApplyMovementReply{}
							kv.localReqNum += 1
							kv.mu.Unlock()

							kv.ApplyMovement(&moveArgs, &moveReply)
							if moveReply.Err != OK {
								atomic.StoreInt32(&applyMovementFail, 1)
								DPrintf("[SKV-S][%v][%v] Set applyMovementFail to 1", kv.gid, kv.me)
							}
							sendApplyMovementLock.Unlock()
							DPrintf("[SKV-S][%v][%v] ApplyMovement(%v) successfully", kv.gid, kv.me, moveArgs)
							return
						}
						// ... not ok, or ErrWrongLeader
						if ok && (reply.Err != OK) {
							DPrintf("[SKV-S][%v][%v] Server [%v][%v] reply with error: %v", kv.gid, kv.me, gid, si, reply.Err)
							continue
						}
						if !ok {
							DPrintf("[SKV-S][%v][%v] RequestMapAndSession to [%v][%v] timeout", kv.gid, kv.me, gid, si)
							continue
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(gid, shards)
	}
	kv.mu.Unlock()
	wg.Wait()

	if _, isLeader := kv.rf.GetState(); !isLeader || kv.killed() || applyMovementFail == 1 {
		kv.mu.Lock()
		DPrintf("[SKV-S][%v][%v] stop sending ApplyMovement because not a leader or killed", kv.gid, kv.me)
		return false // return to avoid deletion
	}

	// Send DeleteShards only after ApplyMovement succeed
	// **suggest** to delete, not force delete
	for gid, shards := range receiveFrom {
		wg.Add(1)
		go func(gid int, shards map[int]bool) {
			defer wg.Done()
			DPrintf("[SKV-S][%v][%v] Sending DeleteShards RPC to Group %v", kv.gid, kv.me, gid)

			for !kv.killed() {
				if servers, ok := oldConfig.Groups[gid]; ok {
					kv.mu.Lock()
					args := DeleteShardsArgs{
						Id:        kv.uid,
						ReqNum:    kv.localReqNum,
						Shards:    shards,
						ConfigNum: newConfig.Num,
					}
					kv.localReqNum += 1
					kv.mu.Unlock()

					for si := 0; si < len(servers); si++ {
						srv, err := kv.make_end(servers[si])
						if err != nil {
							DPrintf("[SKV-S][%v][%v] Failed to make_end(%v)", kv.gid, kv.me, servers[si])
							continue
						}

						reply := DeleteShardsReply{}
						ok := srv.Call("ShardKV.DeleteShards", &args, &reply)

						if ok && reply.Err == OK {
							DPrintf("[SKV-S][%v][%v] DeleteShards to Server[%v][%v] sucessfully", kv.gid, kv.me, gid, si)
							return
						}
						// ... not ok, or ErrWrongLeader
						if ok && (reply.Err != OK) {
							DPrintf("[SKV-S][%v][%v] Server [%v][%v] reply with error: %v", kv.gid, kv.me, gid, si, reply.Err)
							continue
						}
						if !ok {
							DPrintf("[SKV-S][%v][%v] DeleteShards to [%v][%v] timeout", kv.gid, kv.me, gid, si)
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(gid, shards)
	}
	wg.Wait()
	kv.mu.Lock()

	return true
}

func (kv *ShardKV) handleApplyMsg() {
	for applyMsg := range kv.applyCh {
		if applyMsg.CommandValid {
			kv.mu.Lock()
			op := applyMsg.Command.(Op)
			if kv.lastApplied+1 != applyMsg.CommandIndex {
				DPrintf("[SKV-S][%v][%v] apply out of order", kv.gid, kv.me)
			}

			kv.logRecord[applyMsg.CommandIndex] = op
			kv.lastApplied = applyMsg.CommandIndex

			// uniKey = string(ckId) + string(shardNum)
			// use to identify each client's operation on different shards
			uniKey := strconv.FormatInt(op.CkId, 10) + strconv.FormatInt(int64(op.ShardNum), 10)

			// stable operation, don't change state machine
			if session := kv.ckSessions[uniKey]; session.LastOpVaild && session.LastOpIndex < applyMsg.CommandIndex &&
				op.ReqNum <= session.LastOpReqNum {
				DPrintf("[SKV-S][%v][%v] stable operation #%v for [%v] ($%v <= $%v), do not change state machine", kv.gid, kv.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, session.LastOpReqNum)

				session.LastOpNumber = op.Number
				session.LastOpReqNum = op.ReqNum
				session.LastOpShardNum = op.ShardNum
				session.LastOpIndex = applyMsg.CommandIndex
				session.LastOpVaild = true
				kv.ckSessions[uniKey] = session
				kv.mu.Unlock()
				continue
			}

			if !kv.applyOp(op) { // failed to apply due to shards move
				// let waittingForCommit() failed.
				// emm...is this OK?
				op.Number = -1
				kv.logRecord[applyMsg.CommandIndex] = op
			} else { // update session only after applying successfully
				s := kv.ckSessions[uniKey]
				s.LastOpNumber = op.Number
				s.LastOpReqNum = op.ReqNum
				s.LastOpShardNum = op.ShardNum
				s.LastOpIndex = applyMsg.CommandIndex
				s.LastOpVaild = true
				kv.ckSessions[uniKey] = s
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				DPrintf("[SKV-S][%v][%v] %v >= %v try to create snapshot up to #%v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, applyMsg.CommandIndex)
				for key := range kv.logRecord {
					if kv.confirmMap[key] {
						delete(kv.logRecord, key)
						delete(kv.confirmMap, key)
					}
				}

				newSnapshot := Snapshot{
					Mp:            kv.mp,
					CkSessions:    kv.ckSessions,
					Config:        kv.config,
					ShardLastNums: kv.shardLastNums,
					ShardVec:      kv.shardVec,
					Maker:         kv.me,
				}
				buffer := new(bytes.Buffer)
				encoder := gob.NewEncoder(buffer)
				encoder.Encode(newSnapshot)

				kv.rf.Snapshot(applyMsg.CommandIndex, buffer.Bytes())
				DPrintf("[SKV-S][%v][%v] create snapshot up to #%v successfully", kv.gid, kv.me, applyMsg.CommandIndex)
			}
			kv.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			kv.mu.Lock()
			DPrintf("[SKV-S][%v][%v] try to apply snapshot up to #%v", kv.gid, kv.me, applyMsg.SnapshotIndex)
			buffer := bytes.NewBuffer(applyMsg.Snapshot)
			decoder := gob.NewDecoder(buffer)
			snapshot := Snapshot{}
			decoder.Decode(&snapshot)

			kv.mp = snapshot.Mp
			kv.ckSessions = snapshot.CkSessions
			kv.config = snapshot.Config
			kv.shardVec = snapshot.ShardVec
			kv.shardLastNums = snapshot.ShardLastNums
			kv.lastApplied = applyMsg.SnapshotIndex

			DPrintf("[SKV-S][%v][%v] apply snapshot up to #%v successfully, maker [%v]", kv.gid, kv.me, applyMsg.SnapshotIndex, snapshot.Maker)
			DPrintf("[SKV-S][%v][%v] Config: %+v", kv.gid, kv.me, kv.config)
			DPrintf("[SKV-S][%v][%v] ShardVec: %+v", kv.gid, kv.me, kv.shardVec)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) pollConfig() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		latestConfig := kv.mck.Query(-1)

		kv.mu.Lock()
		for kv.config.Num < latestConfig.Num {
			if _, isLeader := kv.rf.GetState(); !isLeader || kv.killed() {
				DPrintf("[SKV-S][%v][%v] stop changing config because it's not a leader or dead", kv.gid, kv.me)
				break
			}

			nextConfig := kv.mck.Query(kv.config.Num + 1)
			DPrintf("[SKV-S][%v][%v] Start changing config from %v to %v", kv.gid, kv.me, kv.config.Num, nextConfig.Num)

			// phase 1: update ShardVec
			newShardVec := [shardctrler.NShards]bool{}
			for i := 0; i < shardctrler.NShards; i++ {
				if (kv.config.Shards[i] == kv.gid || kv.config.Shards[i] == 0) &&
					nextConfig.Shards[i] == kv.gid {
					newShardVec[i] = true
				} else {
					newShardVec[i] = false
				}
			}

			updateShardVecArgs := UpdateShardVecArgs{
				Id:        kv.uid,
				ReqNum:    kv.localReqNum,
				ConfigNum: nextConfig.Num,
				NewVec:    newShardVec,
			}
			kv.localReqNum += 1
			kv.mu.Unlock()

			DPrintf("[SKV-S][%v][%v] Start local RPC UpdateShardVec(%+v)", kv.gid, kv.me, updateShardVecArgs)
			updateOK := false
			for !kv.killed() {
				updateShardVecReply := UpdateShardVecReply{}
				kv.handleNormalRPC(&updateShardVecArgs, &updateShardVecReply, OT_UpdateShardVec)
				if updateShardVecReply.Err == OK {
					updateOK = true
					break
				} else if updateShardVecReply.Err == ERR_WrongLeader {
					updateOK = false
					DPrintf("[SKV-S][%v][%v] Local RPC UpdateShardVec reply with error: %v", kv.gid, kv.me, updateShardVecReply.Err)
					break
				} else {
					DPrintf("[SKV-S][%v][%v] Local RPC UpdateShardVec reply with error: %v", kv.gid, kv.me, updateShardVecReply.Err)
					continue
				}
			}
			kv.mu.Lock()
			if !updateOK || kv.config.Num >= nextConfig.Num || kv.killed() {
				continue
			}

			// phase 2: move shards
			DPrintf("[SKV-S][%v][%v] Start MoveShards", kv.gid, kv.me)
			moveOK := kv.MoveShards(kv.config, nextConfig)
			if !moveOK {
				continue
			}

			// phase 3: change config
			args := ChangeConfigArgs{
				Id:     kv.uid,
				ReqNum: kv.localReqNum,
				Config: nextConfig,
				OldNum: kv.config.Num, // for debug
				NewNum: nextConfig.Num,
			}
			kv.localReqNum += 1
			kv.mu.Unlock()

			DPrintf("[SKV-S][%v][%v] Start Local RPC ChangeConfig(%+v)", kv.gid, kv.me, args)
			for !kv.killed() {
				reply := ChangeConfigReply{}
				kv.handleNormalRPC(&args, &reply, OT_ChangeConfig)
				if reply.Err == OK {
					break
				} else if reply.Err == ERR_WrongLeader {
					DPrintf("[SKV-S][%v][%v] Local RPC ChangeConfig reply with error: %v", kv.gid, kv.me, reply.Err)
					break
				} else {
					DPrintf("[SKV-S][%v][%v] Local RPC ChangeConfig reply with error: %v", kv.gid, kv.me, reply.Err)
					continue
				}
			}
			kv.mu.Lock()
		}
		kv.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("[SKV-S][%v][%v] Killed", kv.gid, kv.me)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) (*labrpc.ClientEnd, error)) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(ChangeConfigArgs{})
	gob.Register(ApplyMovementArgs{})
	gob.Register(DeleteShardsArgs{})
	gob.Register(UpdateShardVecArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.config = shardctrler.Config{}

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	rpc.Register(kv.rf)

	kv.mp = make(map[string]string)
	kv.confirmMap = make(map[int]bool)
	kv.ckSessions = make(map[string]Session)
	kv.logRecord = make(map[int]Op)

	kv.snapShotIndex = 0
	kv.lastApplied = 0
	kv.localReqNum = 1
	kv.uid = -nrand() // use negtive value
	DPrintf("[SKV-S][%v][%v] Start With UID %v", kv.gid, kv.me, kv.uid)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	go kv.handleApplyMsg()
	go kv.pollConfig()

	return kv
}
