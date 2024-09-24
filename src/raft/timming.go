package raft

import "time"

const (
	TM_HeartBeatInterval time.Duration = 200 * time.Millisecond // heart beat interval
	TM_HeartBeatTimeout  time.Duration = 400 * time.Millisecond // heart beat timeout
	TM_ElectionTimeout   time.Duration = 400 * time.Millisecond // election timeout
	TM_RandomWaitingTime time.Duration = 400 * time.Millisecond // randomization, wait for 0~TM_RandomWaitingTime
)
