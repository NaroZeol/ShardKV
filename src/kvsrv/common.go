package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Id      int64
	Opstate bool
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	Id  int64
}

type GetReply struct {
	Value string
}
