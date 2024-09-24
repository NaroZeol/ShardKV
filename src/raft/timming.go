package raft

import "time"

const (
	TM_HeartBeatInterval time.Duration = 200 * time.Millisecond  // heart beat interval
	TM_ElectionTimeout   time.Duration = 500 * time.Millisecond  // election timeout
	TM_HeartBeatTimeout  time.Duration = 600 * time.Millisecond  // heart beat timeout
	TM_RandomWaitingTime time.Duration = 1500 * time.Millisecond // randomization, wait for 0~TM_RandomWaitingTime
)
