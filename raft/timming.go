package raft

import "time"

const (
	TM_HeartBeatInterval time.Duration = 100 * time.Millisecond // heart beat interval
	TM_ElectionTimeout   time.Duration = 200 * time.Millisecond // election timeout
	TM_RandomWaitingTime time.Duration = 300 * time.Millisecond // randomization, wait for 0~TM_RandomWaitingTime
	TM_RPCTimeout        time.Duration = 100 * time.Millisecond // rpc timeout
)
