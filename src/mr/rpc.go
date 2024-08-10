package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RegisterWorkerArgs struct {
	CallerId int
}

type RegisterWorkerReply struct {
	IsErr  bool
	ErrStr string
}

type GetWorkerInfoArgs struct {
	CallerId int
}

type GetWorkerInfoReply struct {
	State workerState
	Work  string
}

type WorkFinishArgs struct {
	CallerId int
	WorkId   int
	WorkType string
}

type WorkFinishReply struct {
	IsErr  bool
	ErrStr string
}

type GetNReduceArgs struct {
	CallerId int
}

type GetNReduceReply struct {
	NReduce int
}

type WorkerDeathArgs struct {
	CallId int
}

type WorkerDeathReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
