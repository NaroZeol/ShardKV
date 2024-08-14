package mr

import (
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

type workInfo struct {
	state     int       // currently: -1->done 0->ready or id of worker
	timestamp time.Time // Only avild when state is not 0 or -1
}

type Coordinator struct {
	workerMap     map[int]worker
	workerMapLock sync.RWMutex
	workInfo      []workInfo
	workInfoLock  sync.RWMutex
	state         string // Working, Starting, Exiting, Death
	stateLock     sync.Mutex
	logger        Logger
	nReduce       int
}

// register a worker
func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	if c.state != "Working" {
		reply.IsErr = true
		reply.ErrStr = "RPC server is not working"
		return nil // I hate rpc to write to log... Bad hack...
	}

	c.workerMapLock.Lock() // Writer lock
	c.workerMap[args.CallerId] = worker{
		state: WS_Free,
		work:  "No job",
	}
	c.workerMapLock.Unlock()

	c.logger.Printf("Worker %v register suceessfully\n", args.CallerId)

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
	c.workInfoLock.Lock()
	c.workerMapLock.Lock()
	defer c.workerMapLock.Unlock()
	defer c.workInfoLock.Unlock()

	if c.workerMap[args.CallerId].state == WS_Death {
		reply.IsErr = true
		reply.ErrStr = "this worker has marked Death"
		return nil // This bad hack is to avoid rpc to write to log
	}

	if args.WorkType == "Map" || args.WorkType == "Reduce" {
		c.workerMap[args.CallerId] = worker{
			state: WS_Free,
			work:  "",
		}
		c.workInfo[args.WorkId].state = -1 // -1 as completed

		c.logger.Println("WorkerFinish is called with ", *args)
	} else if args.WorkType == "Reduce" {
		c.logger.Println("WorkerFinish is called with ", *args)
	}

	return nil
}

// Worker Death
func (c *Coordinator) WorkerDeath(args *WorkerDeathArgs, reply *WorkerDeathReply) error {
	c.workerMapLock.Lock()
	defer c.workerMapLock.Unlock()

	c.workerMap[args.CallId] = worker{
		state: WS_Death,
		work:  "None",
	}

	return nil
}

func (c *Coordinator) run(files []string, nReduce int) {
	c.workInfoLock = sync.RWMutex{}
	c.stateLock = sync.Mutex{}
	nMap := len(files)

	c.stateLock.Lock()
	c.state = "Working"
	c.stateLock.Unlock()

	// Map phase
	c.logger.Println("Map phase start")
	c.workInfo = make([]workInfo, len(files))
	c.AllocWork(files, nMap, "Map")
	c.logger.Println("Map phase done")

	// Reduce phase
	c.logger.Println("Reduce phase start")
	c.workInfo = make([]workInfo, nReduce)
	c.AllocWork(files, nReduce, "Reduce")
	c.logger.Println("Reduce phase done")

	c.stateLock.Lock()
	c.state = "Exiting"
	c.stateLock.Unlock()

	// Done
	c.logger.Println("All works done, exiting")
	c.Exit()
	c.logger.Println("Exit successfully!")
}

func (c *Coordinator) AllocWork(files []string, size int, workType string) {
	getFreeWorker := func() int {
		c.workerMapLock.RLock()
		defer c.workerMapLock.RUnlock()
		for id, worker := range c.workerMap {
			if worker.state == WS_Free {
				return id
			}
		}
		return -1
	}

	for {
		// Crash Recover
		// Currently, a low efficiency solution
		c.workInfoLock.Lock()
		for i, work := range c.workInfo {
			if work.state != 0 && work.state != -1 &&
				time.Since(work.timestamp).Seconds() > 10 { // working by a work but not finished in 10s
				c.workerMapLock.Lock()
				c.workerMap[work.state] = worker{
					state: WS_Death,
				}
				c.workerMapLock.Unlock()
				c.logger.Printf("worker %v didn't finish work %v in 10s", work.state, i)

				work.state = 0 // No alloc
				c.workInfo[i] = work
			}
		}

		c.workInfoLock.Unlock()
		readyWork := -1
		isWorking := false
		c.workInfoLock.Lock()
		for i := 0; i < size; i++ {
			// find a ready work
			if c.workInfo[i].state == 0 {
				readyWork = i
				break
			} else if c.workInfo[i].state != -1 {
				isWorking = true
			}
		}
		c.workInfoLock.Unlock()

		if readyWork == -1 {
			if isWorking {
				c.logger.Println("Waiting all works to be finished")
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
		c.workInfo[readyWork].state = workerId
		c.workInfo[readyWork].timestamp = time.Now()
		c.workInfoLock.Unlock()

		c.workerMapLock.Lock()
		var newJob string
		if workType == "Map" {
			newJob = "Map" + " " + strconv.Itoa(readyWork) + " " + files[readyWork]
		} else if workType == "Reduce" {
			newJob = "Reduce" + " " + strconv.Itoa(readyWork) + " " + strconv.Itoa(readyWork)
		}
		c.workerMap[workerId] = worker{
			state: WS_Ready,
			work:  newJob,
		}
		c.workerMapLock.Unlock()

		c.logger.Printf("Worker %v get work: %v\n", workerId, newJob)
	}
}

func (c *Coordinator) Exit() {
	c.logger.Println("Send end signal to worker")
	c.workerMapLock.Lock()
	for key, worker := range c.workerMap {
		if worker.state == WS_Death {
			continue
		}
		worker.state = WS_Exiting
		worker.work = "Exit"
		c.workerMap[key] = worker
	}
	c.workerMapLock.Unlock()

	for {
		isAllDeath := true

		c.workerMapLock.RLock()
		for _, worker := range c.workerMap {
			if worker.state != WS_Death {
				isAllDeath = false
				break
			}
		}
		c.workerMapLock.RUnlock()

		if !isAllDeath {
			time.Sleep(1 * time.Second)
			continue
		} else {
			break
		}
	}

	c.logger.Println("All workers have exited")
	c.logger.Println("Set state to Death")

	c.stateLock.Lock()
	c.state = "Death"
	c.stateLock.Unlock()
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
		c.logger.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.stateLock.Lock()
	if c.state == "Death" {
		ret = true
	} else {
		ret = false
	}
	c.stateLock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		workerMap:     map[int]worker{},
		workerMapLock: sync.RWMutex{},
		state:         "Starting",
		stateLock:     sync.Mutex{},
		logger:        *NewLogger("\033[36m[Coordinator]\033[0m"),
		nReduce:       nReduce,
	}

	// Your code here.
	// run coordinator service in a new thread
	go c.run(files, nReduce)

	c.server()
	return &c
}
