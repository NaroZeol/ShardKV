package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type ApplyQueue struct {
	queue []ApplyMsg
	mu    sync.Mutex
}

func (aq *ApplyQueue) Enqueue(val ApplyMsg) {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	aq.queue = append(([]ApplyMsg)(aq.queue), val)
}

func (aq *ApplyQueue) Dequeue() (ApplyMsg, bool) {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	if len(aq.queue) == 0 {
		return ApplyMsg{}, false
	}
	ret := aq.queue[0]
	aq.queue = ([]ApplyMsg)(aq.queue)[1:]
	return ret, true
}

func (aq *ApplyQueue) Front() ApplyMsg {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	return aq.queue[0]
}

func (aq *ApplyQueue) Size() int {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	return len(aq.queue)
}

func (aq *ApplyQueue) Clean() {
	aq.mu.Lock()
	defer aq.mu.Unlock()

	aq.queue = make([]ApplyMsg, 0)
}
