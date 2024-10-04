package raft

type raftState int
type logType int

// raftState
const (
	RS_Follower raftState = 0
	RS_Candiate raftState = 1
	RS_Leader   raftState = 2
)

// logType
const (
	LT_Normal logType = 0
	LT_Noop   logType = 1
)
