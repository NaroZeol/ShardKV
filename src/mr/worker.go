package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerMeta struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	nReduce int
	id      int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workMetaMsg WorkerMeta

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

	workMetaMsg = WorkerMeta{
		mapf:    mapf,
		reducef: reducef,
		nReduce: CallGetNReduce(),
		id:      os.Getpid(),
	}
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
			err := do_it(workerInfo.Work)
			if err != nil {
				log.Println("Error when doing job: ", err.Error())
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func do_it(work string) error {

	workArgs := strings.Split(work, " ")

	workType := workArgs[0]                // "Map" or "Reduce"
	workId, _ := strconv.Atoi(workArgs[1]) // Id of map or reduce
	workArg := workArgs[2]                 // "Map": key of mapf. "Reduce": nReduce of reducef.

	log.Println("Get job: ", workType, workId, workArg)

	if workType == "Map" {
		err := do_mapf(workId, workArg)
		if err != nil {
			return err
		}
	} else {
		err := do_reducef(workId)
		if err != nil {
			return err
		}
	}

	// Report finish
	CallWorkFinish(workId, workType)

	return nil
}

func do_mapf(workId int, workArg string) error {
	path := workArg
	content, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	log.Printf("Reading from %v successful\n", path)

	mrKVs := workMetaMsg.mapf(path, string(content))
	sort.Sort(ByKey(mrKVs))
	log.Printf("Map %v done!\n", workId)

	nReduce := workMetaMsg.nReduce
	for i := 0; i < nReduce; i++ {
		// mr-MapNumber-ReduceNumber
		intermediatePath := "mr-" + strconv.Itoa(workId) + "-" + strconv.Itoa(i)

		// TODO: atomic write
		file, err := os.OpenFile(intermediatePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR,
			os.ModeAppend|os.ModePerm)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(file)
		for _, kv := range mrKVs {
			if ihash(kv.Key)%nReduce == i {
				err := enc.Encode(&kv)
				if err != nil {
					return nil
				}
			}
		}
	}

	return nil
}

func do_reducef(workId int) error {
	dir, err := os.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	files, err := dir.Readdir(-1)
	if err != nil {
		return nil
	}

	regexPattern := fmt.Sprintf(`^mr-(\d+)-%v$`, workId)
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil
	}

	kva := make([]KeyValue, 0)

	for _, file := range files {
		if regex.MatchString(file.Name()) {
			intermediateFile, err := os.Open(file.Name())
			if err != nil {
				return nil
			}
			defer intermediateFile.Close()
			log.Println("Reading from: ", file.Name())

			dec := json.NewDecoder(intermediateFile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}

	sort.Sort(ByKey(kva))

	oPath := "mr-" + strconv.Itoa(workId)
	ofile, _ := os.Create(oPath)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := workMetaMsg.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	return nil
}

func CallRegisterWorker() {
	args := RegisterWorkerArgs{}

	args.CallerId = workMetaMsg.id

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

	args.CallerId = workMetaMsg.id

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

func CallWorkFinish(workId int, workType string) {
	args := WorkFinishArgs{}

	args.CallerId = workMetaMsg.id
	args.WorkId = workId
	args.WorkType = workType

	reply := WorkFinishReply{}

	ok := call("Coordinator.WorkFinish", &args, &reply)
	if ok {
		log.Println("Send finish message to coordinator: ", args)
	} else {
		log.Println("Failed to send finish message to coordinator: ", args)
	}
}

func CallGetNReduce() int {
	// TODO: A more gentle way to get coordinator meta message
	args := GetNReduceArgs{}

	reply := GetNReduceReply{}

	call("Coordinator.GetNReduce", &args, &reply)

	return reply.NReduce
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
