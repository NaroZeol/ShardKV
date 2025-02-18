package shardctrler

import (
	"bytes"
	"encoding/gob"
	"log"
	"net/rpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/raft"
	"6.5840/rpcwrapper"
)

var Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ConfigChangeTrace = false
const ConfigReport = false

func ConfigChangeTracePrintf(format string, a ...interface{}) (n int, err error) {
	if ConfigChangeTrace {
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
	logRecord     map[int64]Op
	lastApplied   int64
	snapshotIndex int64
}

type Session struct {
	LastOp      Op
	LastOpVaild bool
	LastOpIndex int64
}

type Op struct {
	Type   string
	Number int64
	// content ?
	ReqNum int64
	CkId   int64

	Args interface{}
}

func Op2Bytes(op Op) []byte {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	encoder.Encode(op)
	return buffer.Bytes()
}

func Bytes2Op(data []byte) Op {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	op := Op{}
	decoder.Decode(&op)
	return op
}

type Snapshot struct {
	Configs    []Config
	CkSessions map[int64]Session
	Maker      int // for debug
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) error {
	sc.handleNomalRPC(args, reply, OT_Join)
	return nil
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sc.handleNomalRPC(args, reply, OT_Leave)
	return nil
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) error {
	sc.handleNomalRPC(args, reply, OT_Move)
	return nil
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) error {
	sc.handleNomalRPC(args, reply, OT_Query)
	return nil
}

func (sc *ShardCtrler) handleNomalRPC(args GenericArgs, reply GenericReply, opType string) {
	sc.mu.Lock()
	DPrintf("[SC-S][%v] receive RPC %v: %+v", sc.me, opType, args)

	if session := sc.ckSessions[args.getId()]; session.LastOpVaild && session.LastOp.ReqNum == args.getReqNum() {
		if op, ok := sc.logRecord[session.LastOpIndex]; ok && op.Number == session.LastOp.Number {
			sc.successCommit(args, reply, opType)
			sc.mu.Unlock()
			return
		} // else start a new operation
	}

	op := Op{
		Type:   opType,
		Number: nrand(),
		ReqNum: args.getReqNum(),
		CkId:   args.getId(),
	}

	switch opType {
	case OT_Join:
		op.Args = *args.(*JoinArgs)
	case OT_Leave:
		op.Args = *args.(*LeaveArgs)
	case OT_Move:
		op.Args = *args.(*MoveArgs)
	case OT_Query:
		op.Args = *args.(*QueryArgs)
	}

	index, _, isLeader := sc.rf.Start(Op2Bytes(op))
	if !isLeader {
		reply.setWrongLeader(true)
		DPrintf("[SC-S][%v] failed to Start(), not a leader", sc.me)
		sc.mu.Unlock()
		return
	} else {
		DPrintf("[SC-S][%v] Start #%v", sc.me, index)
	}
	sc.mu.Unlock()

	sc.waittingForCommit(op, index, args, reply, opType)
}

func (sc *ShardCtrler) waittingForCommit(op Op, index int64, args GenericArgs, reply GenericReply, opType string) {
	startTime := time.Now()
	for !sc.killed() {
		sc.mu.Lock()
		if index <= sc.lastApplied {
			finishedOp, ok := sc.logRecord[index]
			if ok && finishedOp.Number == op.Number {
				sc.successCommit(args, reply, opType)
				sc.mu.Unlock()
				return
			} else if ok && finishedOp.Number != op.Number {
				DPrintf("[SC-S][%v] Failed to commit op #%v, wrong Op.number", sc.me, index)
				reply.setErr(ERR_FailedToCommit)
				sc.mu.Unlock()
				return
			}
		}
		sc.mu.Unlock()

		if time.Since(startTime) > 100*time.Millisecond {
			DPrintf("[SC-S][%v] Failed to commit op #%v, timeout", sc.me, index)
			reply.setErr(ERR_CommitTimeout)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (sc *ShardCtrler) successCommit(args GenericArgs, reply GenericReply, opType string) {
	switch opType { // OK, actually only type OT_Query needs special handle, but I keep this structure for the future...
	case OT_Join:
		joinReply := reply.(*JoinReply)

		joinReply.Err = ERR_OK
		joinReply.WrongLeader = false
	case OT_Leave:
		leaveReply := reply.(*LeaveReply)

		leaveReply.Err = ERR_OK
		leaveReply.WrongLeader = false
	case OT_Move:
		moveReply := reply.(*MoveReply)

		moveReply.Err = ERR_OK
		moveReply.WrongLeader = false
	case OT_Query:
		queryArgs := args.(*QueryArgs)
		queryReply := reply.(*QueryReply)

		num := queryArgs.Num
		if queryArgs.Num == -1 || queryArgs.Num >= len(sc.configs) {
			num = len(sc.configs) - 1
		}

		queryReply.Err = ERR_OK
		queryReply.WrongLeader = false
		queryReply.Config = sc.configs[num]
	default:
		log.Fatal("Wrong switch in successCommit()")
	}

	if ConfigChangeTrace && opType != OT_Query {
		ConfigChangeTracePrintf("Config Changed:")
		ConfigChangeTracePrintf("Old Config: %+v", sc.configs[len(sc.configs)-2])
		ConfigChangeTracePrintf("New Config: %+v", sc.configs[len(sc.configs)-1])
	}
}

func (sc *ShardCtrler) rebalance(oldConfig *Config, newConfig *Config) {
	// should hold sc.mu

	orphanShards := make([]int, 0) // shards that do not belong to any gid
	type GS struct {               // Gid and Shards
		Gid    int
		Shards []int
	}
	GSs := make([]GS, 0)
	gid2Index := make(map[int]int) // gid to index of GSs

	for gid := range newConfig.Groups {
		GSs = append(GSs, GS{
			Gid:    gid,
			Shards: make([]int, 0),
		})
		gid2Index[gid] = len(GSs) - 1
	}

	if len(GSs) == 0 {
		// reset to zero if no gid is vaild
		newConfig.Shards = [NShards]int{0}
		return
	}

	// find out orphanShards(do not belong to any gid) in new configuration
	// or add shardNum to GSs if gid is still vaild in new configruration
	for shardNum, gid := range oldConfig.Shards {
		if gid == 0 { // invaild gid
			orphanShards = append(orphanShards, shardNum)
		} else if _, ok := gid2Index[gid]; !ok { // old gid does not exist in new configuration
			orphanShards = append(orphanShards, shardNum)
		} else {
			GSs[gid2Index[gid]].Shards = append(GSs[gid2Index[gid]].Shards, shardNum)
		}
	}

	// sort by gid to ensure GSs is in same order on each server
	sort.Slice(GSs, func(i, j int) bool {
		return GSs[i].Gid < GSs[j].Gid
	})
	for i, gs := range GSs { // sort by ShardNum to ensure gs.Shards is in same order on each server
		gs.Shards = sort.IntSlice(gs.Shards)
		gid2Index[gs.Gid] = i
	}

	for {
		minShardsNum := int(1e10)
		minGid := 0
		maxShardsNum := 0
		maxGid := 0

		// walk around stupidly
		// a smarter way is to use heapm but we only have NShards(10)
		for _, gs := range GSs {
			if len(gs.Shards) <= minShardsNum { // find out which gid has the minimum shards
				minShardsNum = len(gs.Shards)
				minGid = gs.Gid
			}
			if maxShardsNum <= len(gs.Shards) { // find out which gid has the maximum shards
				maxShardsNum = len(gs.Shards)
				maxGid = gs.Gid
			}
		}

		// stop condition
		if maxShardsNum-minShardsNum <= 1 && len(orphanShards) == 0 {
			break
		}

		minShards := GSs[gid2Index[minGid]].Shards
		maxShards := GSs[gid2Index[maxGid]].Shards

		if len(orphanShards) != 0 { // add an orphanShard to minShards
			minShards = append(minShards, orphanShards[len(orphanShards)-1])
			orphanShards = orphanShards[0 : len(orphanShards)-1]

			GSs[gid2Index[minGid]].Shards = minShards
		} else if maxShardsNum-minShardsNum > 1 { // move a shard from maxShards to minShards
			movedShard := maxShards[len(maxShards)-1]
			maxShards = maxShards[0 : len(maxShards)-1]
			minShards = append(minShards, movedShard)

			GSs[gid2Index[minGid]].Shards = minShards
			GSs[gid2Index[maxGid]].Shards = maxShards
		}

	}

	newShards := [NShards]int{0}
	for _, gs := range GSs {
		for _, shard := range gs.Shards {
			newShards[shard] = gs.Gid
		}
	}

	newConfig.Shards = newShards
}

func (sc *ShardCtrler) applyOp(op Op) {
	// should hold sc.mu

	switch op.Type {
	case OT_Join:
		joinArgs := op.Args.(JoinArgs)
		DPrintf("[SC-S][%v] Apply Op: Join (%+v)", sc.me, joinArgs.Servers)

		oldConfig := sc.configs[len(sc.configs)-1]

		newConfig := Config{
			Groups: make(map[int][]string),
			Shards: oldConfig.Shards,
			Num:    oldConfig.Num + 1,
		}
		for key, value := range oldConfig.Groups {
			newConfig.Groups[key] = value
		}

		// apply op
		for key, value := range joinArgs.Servers {
			newConfig.Groups[key] = value
		}
		sc.rebalance(&sc.configs[len(sc.configs)-1], &newConfig)

		sc.configs = append(sc.configs, newConfig)
		// DPrintf("[SC-S][%v] Old Config: %+v", sc.me, oldConfig)
		// DPrintf("[SC-S][%v] New Config: %+v", sc.me, newConfig)
	case OT_Leave:
		leaveArgs := op.Args.(LeaveArgs)
		DPrintf("[SC-S][%v] Apply Op: Leave (%+v)", sc.me, leaveArgs.GIDs)

		oldConfig := sc.configs[len(sc.configs)-1]

		newConfig := Config{
			Groups: make(map[int][]string),
			Shards: oldConfig.Shards,
			Num:    oldConfig.Num + 1,
		}
		for key, value := range oldConfig.Groups {
			newConfig.Groups[key] = value
		}

		// apply op
		for _, gid := range leaveArgs.GIDs {
			delete(newConfig.Groups, gid)
		}
		sc.rebalance(&sc.configs[len(sc.configs)-1], &newConfig)

		sc.configs = append(sc.configs, newConfig)
		// DPrintf("[SC-S][%v] Old Config: %+v", sc.me, oldConfig)
		// DPrintf("[SC-S][%v] New Config: %+v", sc.me, newConfig)
	case OT_Move:
		moveArgs := op.Args.(MoveArgs)
		DPrintf("[SC-S][%v] Apply Op: Move %+v", sc.me, moveArgs)

		oldConfig := sc.configs[len(sc.configs)-1]

		newConfig := Config{
			Groups: make(map[int][]string),
			Shards: oldConfig.Shards,
			Num:    oldConfig.Num + 1,
		}
		for key, value := range oldConfig.Groups {
			newConfig.Groups[key] = value
		}

		// apply op
		newConfig.Shards[moveArgs.Shard] = moveArgs.GID

		sc.configs = append(sc.configs, newConfig)
		// DPrintf("[SC-S][%v] Old Config: %+v", sc.me, oldConfig)
		// DPrintf("[SC-S][%v] New Config: %+v", sc.me, newConfig)
	case OT_Query:
		queryArgs := op.Args.(QueryArgs)
		// do nothing for Query
		DPrintf("[SC-S][%v] Apply Op: Query (%+v)", sc.me, queryArgs)
	default:
		log.Fatal("wrong switch in applyOp")
	}
}

func (sc *ShardCtrler) handleApplyMsg() {
	for applyMsg := range sc.applyCh {
		if applyMsg.CommandValid {
			sc.mu.Lock()

			op := Bytes2Op(applyMsg.Command)

			if sc.lastApplied+1 != applyMsg.CommandIndex {
				DPrintf("[SC-S][%v] apply out of order", sc.me)
			}

			sc.logRecord[applyMsg.CommandIndex] = op
			sc.lastApplied = applyMsg.CommandIndex

			// stable operation, don't change state machine
			if session := sc.ckSessions[op.CkId]; session.LastOpVaild && session.LastOpIndex < applyMsg.CommandIndex &&
				op.ReqNum <= session.LastOp.ReqNum {
				DPrintf("[SC-S][%v] stable operation #%v for [%v] ($%v <= $%v), do not change state machine", sc.me, applyMsg.CommandIndex, op.CkId, op.ReqNum, session.LastOp.ReqNum)

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

			sc.applyOp(op)

			if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate {
				DPrintf("[SC-S][%v] %v >= %v try to create snapshot up to #%v", sc.me, sc.persister.RaftStateSize(), sc.maxraftstate, applyMsg.CommandIndex)
				newSnapshot := Snapshot{
					Configs:    sc.configs,
					CkSessions: sc.ckSessions,
					Maker:      sc.me,
				}
				buffer := new(bytes.Buffer)
				encoder := gob.NewEncoder(buffer)
				encoder.Encode(newSnapshot)

				sc.rf.Snapshot(applyMsg.CommandIndex, buffer.Bytes())

				for index := range sc.logRecord {
					if index < applyMsg.CommandIndex {
						delete(sc.logRecord, index)
					}
				}
				DPrintf("[SC-S][%v] create snapshot up to #%v successfully", sc.me, applyMsg.CommandIndex)
			}
			sc.mu.Unlock()
		} else if applyMsg.SnapshotValid {
			sc.mu.Lock()
			DPrintf("[SC-S][%v] try to apply snapshot up to #%v", sc.me, applyMsg.SnapshotIndex)
			buffer := bytes.NewBuffer(applyMsg.Snapshot)
			decoder := gob.NewDecoder(buffer)
			snapshot := Snapshot{}
			decoder.Decode(&snapshot)

			sc.configs = snapshot.Configs
			sc.ckSessions = snapshot.CkSessions
			sc.lastApplied = applyMsg.SnapshotIndex

			DPrintf("[SC-S][%v] apply snapshot up to #%v successfully, maker [%v]", sc.me, applyMsg.SnapshotIndex, snapshot.Maker)
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
func StartServer(servers []*rpcwrapper.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *ShardCtrler {
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = maxraftstate
	sc.persister = persister

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, (int64)(me), persister, sc.applyCh)
	rpc.Register(sc.rf)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Shards = [NShards]int{0}

	sc.ckSessions = make(map[int64]Session)
	sc.logRecord = make(map[int64]Op)

	sc.snapshotIndex = 0
	sc.lastApplied = 0

	go sc.handleApplyMsg()
	if ConfigReport {
		go func() {
			for !sc.killed() {
				if _, isLeader := sc.rf.GetState(); !isLeader {
					time.Sleep(1 * time.Second)
					continue
				}

				sc.mu.Lock()
				log.Printf("[SC-S][%v] Report Config: ", sc.me)
				for i, cfg := range sc.configs {
					log.Printf("[SC-S][%v] Config %v: %+v", sc.me, i, cfg)
				}
				sc.mu.Unlock()
				time.Sleep(1 * time.Second)
			}
		}()
	}

	return sc
}
