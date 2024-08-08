package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type worker struct {
	state workerState
	job   string
}

type Coordinator struct {
	workerMap     map[int]worker
	workerMapLock sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// register a worker
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.workerMapLock.Lock() // Writer lock
	c.workerMap[args.CallerId] = worker{
		state: Free,
		job:   "No job",
	}
	c.workerMapLock.Unlock()

	log.Printf("Worker %v register suceessfully\n", args.CallerId)

	return nil
}

// get worker info
func (c *Coordinator) GetWorkerInfo(args *GetWorkerInfoArgs, reply *GetWorkerInfoReply) error {
	c.workerMapLock.RLock() // Reader lock
	info := c.workerMap[args.CallerId]
	c.workerMapLock.RUnlock()

	reply.Job = info.job
	reply.State = info.state

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workerMap:     map[int]worker{},
		workerMapLock: sync.RWMutex{},
	}

	// Your code here.

	c.server()
	return &c
}
