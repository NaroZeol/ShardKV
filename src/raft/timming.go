package raft

import "time"

const (
	TM_HeartBeatInterval time.Duration = 100 * time.Millisecond // heart beat interval
	TM_ElectionTimeout   time.Duration = 300 * time.Millisecond // election timeout
	TM_RandomWaitingTime time.Duration = 800 * time.Millisecond // randomization, wait for 0~TM_RandomWaitingTime
	TM_CommitCheck       time.Duration = 10 * time.Millisecond  // check interval of commit
	TM_ApplyCheck        time.Duration = 10 * time.Millisecond  //check interval of apply
	MAX_RETRY_TIMES      int           = 3                      // Max times a server can try during sending a RPC
)
