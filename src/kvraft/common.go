package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Id     int64
	ReqNum int64
}

type PutAppendReply struct {
	Err        Err
	ServerName int // use for debug
}

type GetArgs struct {
	Key    string
	Id     int64
	ReqNum int64
}

type GetReply struct {
	Err        Err
	Value      string
	ServerName int // use for debug
}

type GenericArgs interface {
	getId() int64
	getReqNum() int64
}

type GenericReply interface {
	setErr(str Err)
	getErr() Err
	setServerName(name int)
}

// func (args GetArgs) getKey() string {
// 	return args.Key
// }

// func (args PutAppendArgs) getKey() string {
// 	return args.Key
// }

func (args GetArgs) getId() int64 {
	return args.Id
}

func (args PutAppendArgs) getId() int64 {
	return args.Id
}

func (args GetArgs) getReqNum() int64 {
	return args.ReqNum
}

func (args PutAppendArgs) getReqNum() int64 {
	return args.ReqNum
}

func (reply *GetReply) setErr(err Err) {
	reply.Err = err
}

func (reply *PutAppendReply) setErr(err Err) {
	reply.Err = err
}

func (reply GetReply) getErr() Err {
	return reply.Err
}

func (reply PutAppendReply) getErr() Err {
	return reply.Err
}

func (reply *GetReply) setServerName(name int) {
	reply.ServerName = name
}

func (reply *PutAppendReply) setServerName(name int) {
	reply.ServerName = name
}
