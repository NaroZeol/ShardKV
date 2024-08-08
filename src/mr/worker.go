package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	CallRegisterWorker()

	for {
		time.Sleep(3 * time.Second)
		workerInfo := CallGetWorkerInfo()
		if workerInfo == nil {
			log.Fatal("Something wrong when getting worker info")
		}
		if workerInfo.State == Free {
			log.Println("Free now")
			continue
		} else if workerInfo.State == Ready {
			err := do_it(workerInfo.Job)
			if err != nil {
				log.Println("Error when doing job")
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func do_it(job string) error {
	log.Println("Get job: ", job)
	return nil
}

func CallRegisterWorker() {
	args := RegisterWorkerArgs{}

	args.CallerId = os.Getpid()

	reply := RegisterWorkerReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		log.Printf("Worker %v register successfully\n", os.Getpid())
	} else {
		log.Fatalf("Worker %v register failed\n", os.Getpid())
	}
}

func CallGetWorkerInfo() *GetWorkerInfoReply {
	args := GetWorkerInfoArgs{}

	args.CallerId = os.Getpid()

	reply := GetWorkerInfoReply{}

	ok := call("Coordinator.GetWorkerInfo", &args, &reply)
	if ok {
		log.Println("Get worker info: ", reply)
		return &reply
	} else {
		log.Println("Unable to get worker info")
		return nil
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
