package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type worker struct {
	state workerState
	work  string
}

type Coordinator struct {
	workerMap     map[int]worker
	workerMapLock sync.RWMutex
	workInfo      []int // currently: -1->done 0->ready or id of worker
	workInfoLock  sync.RWMutex
	nReduce       int
}

// register a worker
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.workerMapLock.Lock() // Writer lock
	c.workerMap[args.CallerId] = worker{
		state: Free,
		work:  "No job",
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

	reply.Work = info.work
	reply.State = info.state

	return nil
}

// get nReduce
func (c *Coordinator) GetNReduce(args *GetNReduceArgs, reply *GetNReduceReply) error {
	reply.NReduce = c.nReduce

	return nil
}

// report a work has been finished
func (c *Coordinator) WorkFinish(args *WorkFinishArgs, reply *WorkFinishReply) error {
	if args.WorkType == "Map" || args.WorkType == "Reduce" {
		c.workerMapLock.Lock()
		c.workerMap[args.CallerId] = worker{
			state: Free,
			work:  "",
		}
		c.workerMapLock.Unlock()

		c.workInfoLock.Lock()
		c.workInfo[args.WorkId] = -1 // -1 as completed
		c.workInfoLock.Unlock()

		log.Print("WorkerFinish is called with ", *args)
	} else if args.WorkType == "Reduce" {
		log.Print("WorkerFinish is called with ", *args)
	}

	return nil
}

func (c *Coordinator) run(files []string, nReduce int) {
	c.workInfoLock = sync.RWMutex{}
	nMap := len(files)

	// Map phase
	log.Println("Map phase start")
	c.workInfo = make([]int, len(files))
	c.AllocWork(files, nMap, "Map")
	log.Println("Map phase done")

	log.Println("Reduce phase start")
	c.workInfo = make([]int, nReduce)
	c.AllocWork(files, nReduce, "Reduce")
	log.Println("Reduce phase done")
}

func (c *Coordinator) AllocWork(files []string, size int, workType string) {
	getFreeWorker := func() int {
		c.workerMapLock.RLock()
		defer c.workerMapLock.RUnlock()
		for id, worker := range c.workerMap {
			if worker.state == Free {
				return id
			}
		}
		return -1
	}

	for {
		readyWork := -1
		isWorking := false
		c.workInfoLock.Lock()
		for i := 0; i < size; i++ {
			// find a ready work
			if c.workInfo[i] == 0 {
				readyWork = i
				break
			} else if c.workInfo[i] != -1 {
				isWorking = true
			}
		}
		c.workInfoLock.Unlock()

		if readyWork == -1 {
			if isWorking {
				log.Println("Waiting all works to be finished")
				time.Sleep(1 * time.Second)
				continue
			} else {
				break
			}
		}

		workerId := getFreeWorker()
		if workerId == -1 {
			time.Sleep(1 * time.Second) // wait for next loop
			continue
		}

		c.workInfoLock.Lock()
		c.workInfo[readyWork] = workerId
		c.workInfoLock.Unlock()

		c.workerMapLock.Lock()
		var newJob string
		if workType == "Map" {
			newJob = "Map" + " " + strconv.Itoa(readyWork) + " " + files[readyWork]
		} else if workType == "Reduce" {
			newJob = "Reduce" + " " + strconv.Itoa(readyWork) + " " + strconv.Itoa(readyWork)
		}
		c.workerMap[workerId] = worker{
			state: Ready,
			work:  newJob,
		}
		c.workerMapLock.Unlock()

		log.Printf("Worker %v get work: %v\n", workerId, newJob)

		// TODO: Timer for timeout
	}
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
		nReduce:       nReduce,
	}

	// Your code here.
	// run coordinator service in a new thread
	go c.run(files, nReduce)

	c.server()
	return &c
}
