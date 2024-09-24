package raft

type raftState int

const (
	RS_Follower raftState = 0
	RS_Candiate raftState = 1
	RS_Leader   raftState = 2
)
