package shardctrler

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// Errors
const (
	ERR_OK             = "" // no error
	ERR_FailedToCommit = "Failed to commit"
	ERR_CommitTimeout  = "Commit timeout"
)

// Op types
const (
	OT_Join  = "Join"
	OT_Leave = "Leave"
	OT_Move  = "Move"
	OT_Query = "Query"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Id      int64
	ReqNum  int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs   []int
	Id     int64
	ReqNum int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard  int
	GID    int
	Id     int64
	ReqNum int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num    int // desired config number
	Id     int64
	ReqNum int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type GenericArgs interface {
	getId() int64
	getReqNum() int64
}

type GenericReply interface {
	setErr(str Err)
	setWrongLeader(wrongLeader bool)
}

//
// following functions are generated by Copilot
//

func (args *JoinArgs) getId() int64 {
	return args.Id
}

func (args *JoinArgs) getReqNum() int64 {
	return args.ReqNum
}

func (args *LeaveArgs) getId() int64 {
	return args.Id
}

func (args *LeaveArgs) getReqNum() int64 {
	return args.ReqNum
}

func (args *MoveArgs) getId() int64 {
	return args.Id
}

func (args *MoveArgs) getReqNum() int64 {
	return args.ReqNum
}

func (args *QueryArgs) getId() int64 {
	return args.Id
}

func (args *QueryArgs) getReqNum() int64 {
	return args.ReqNum
}

func (reply *JoinReply) setErr(err Err) {
	reply.Err = err
}

func (reply *JoinReply) setWrongLeader(wrongLeader bool) {
	reply.WrongLeader = wrongLeader
}

func (reply *LeaveReply) setErr(err Err) {
	reply.Err = err
}

func (reply *LeaveReply) setWrongLeader(wrongLeader bool) {
	reply.WrongLeader = wrongLeader
}

func (reply *MoveReply) setErr(err Err) {
	reply.Err = err
}

func (reply *MoveReply) setWrongLeader(wrongLeader bool) {
	reply.WrongLeader = wrongLeader
}

func (reply *QueryReply) setErr(err Err) {
	reply.Err = err
}

func (reply *QueryReply) setWrongLeader(wrongLeader bool) {
	reply.WrongLeader = wrongLeader
}
