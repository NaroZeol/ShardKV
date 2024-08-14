package mr

import (
	"encoding/json"
	"errors"
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

type WorkerType struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	logger  Logger
	nReduce int
	id      int
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	w := WorkerType{
		mapf:    mapf,
		reducef: reducef,
		id:      os.Getpid(),
	}
	w.nReduce = w.CallGetNReduce()
	w.logger = *NewLogger("")
	w.logger.prefix = fmt.Sprintf("\033[35m[Worker %v]\033[0m", w.id)
	w.CallRegisterWorker()

	for {
		time.Sleep(1 * time.Second)
		workerInfo := w.CallGetWorkerInfo()
		if workerInfo == nil {
			w.logger.Println("Something wrong when getting worker info")
			continue
		}
		if workerInfo.State == WS_Free {
			w.logger.Println("Free now")
			continue
		} else if workerInfo.State == WS_Ready {
			err := w.do_it(workerInfo.Work)
			if err != nil {
				w.logger.Println("Error when doing job: ", err.Error())
			}
		} else if workerInfo.State == WS_Exiting {
			w.logger.Println("Receive coordinator exiting")
			w.CallWorkerDeath()
			w.logger.Println("Report death")
			os.Exit(0)
		}
	}
}

func (w *WorkerType) do_it(work string) error {

	workArgs := strings.Split(work, " ")

	workType := workArgs[0]                // "Map" or "Reduce"
	workId, _ := strconv.Atoi(workArgs[1]) // Id of map or reduce
	workArg := workArgs[2]                 // "Map": key of mapf. "Reduce": nReduce of reducef.

	w.logger.Println("Get work: ", work)

	if workType == "Map" {
		err := w.do_mapf(workId, workArg)
		if err != nil {
			return err
		}
	} else {
		err := w.do_reducef(workId)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *WorkerType) do_mapf(workId int, workArg string) error {
	path := workArg
	content, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	w.logger.Printf("Reading from %v successful\n", path)

	mrKVs := w.mapf(path, string(content))
	sort.Sort(ByKey(mrKVs))
	w.logger.Printf("Map %v done!\n", workId)
	w.logger.Println("Write to temp files")

	nReduce := w.nReduce
	for i := 0; i < nReduce; i++ {
		// mr-tmp-workerID-MapNumber-ReduceNumber
		// use tmp files to make it automic
		tmpPAth := "mr-tmp-" + strconv.Itoa(w.id) + "-" + strconv.Itoa(workId) + "-" + strconv.Itoa(i)

		file, err := os.OpenFile(tmpPAth, os.O_CREATE|os.O_TRUNC|os.O_RDWR,
			os.ModeAppend|os.ModePerm)
		if err != nil {
			return err
		}

		enc := json.NewEncoder(file)
		for _, kv := range mrKVs {
			if ihash(kv.Key)%nReduce == i {
				err := enc.Encode(&kv)
				if err != nil {
					return err
				}
			}
		}
	}

	// Report finish
	reply := w.CallWorkFinish(workId, "Map")

	// Only rename when coordinator says OK
	if !reply.IsErr {
		w.logger.Println("Rename temp files")
		dir, err := os.Open(".")
		if err != nil {
			return err
		}
		defer dir.Close()
		files, err := dir.Readdir(-1)
		if err != nil {
			return err
		}

		regexPattern := fmt.Sprintf(`^mr-tmp-%v-(\d+)-(\d+)$`, w.id)
		regex, err := regexp.Compile(regexPattern)
		if err != nil {
			return err
		}

		for _, file := range files {
			if regex.MatchString(file.Name()) {
				strA := strings.Split(file.Name(), "-")
				if len(strA) != 5 {
					return errors.New(fmt.Sprint("Wrong when handle filename: ", file.Name()))
				}
				newPath := "mr-" + strA[3] + "-" + strA[4]
				os.Rename(file.Name(), newPath)
			}
		}
	}

	return nil
}

func (w *WorkerType) do_reducef(workId int) error {
	dir, err := os.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	files, err := dir.Readdir(-1)
	if err != nil {
		return err
	}

	regexPattern := fmt.Sprintf(`^mr-(\d+)-%v$`, workId)
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return err
	}

	kva := make([]KeyValue, 0)

	w.logger.Println("Reading from intermediate files")
	for _, file := range files {
		if regex.MatchString(file.Name()) {
			intermediateFile, err := os.Open(file.Name())
			if err != nil {
				return err
			}
			defer intermediateFile.Close()

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

	// TODO: Atomic write
	oPath := "mr-out-" + strconv.Itoa(workId)
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
		output := w.reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	// Report finish
	reply := w.CallWorkFinish(workId, "Reduce")

	if reply.IsErr {
		return errors.New(reply.ErrStr)
	}

	return nil
}

func (w *WorkerType) CallRegisterWorker() {
	args := RegisterWorkerArgs{}

	args.CallerId = w.id

	reply := RegisterWorkerReply{
		IsErr: false,
	}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok && !reply.IsErr {
		w.logger.Println("register successfully")
	} else {
		w.logger.Println("register failed: ", reply.ErrStr)
		os.Exit(1)
	}
}

func (w *WorkerType) CallGetWorkerInfo() *GetWorkerInfoReply {
	args := GetWorkerInfoArgs{}

	args.CallerId = w.id

	reply := GetWorkerInfoReply{}

	ok := call("Coordinator.GetWorkerInfo", &args, &reply)
	if ok {
		w.logger.Println("Get worker info: ", reply)
		return &reply
	} else {
		w.logger.Println("Unable to get worker info")
		return nil
	}
}

func (w *WorkerType) CallWorkFinish(workId int, workType string) WorkFinishReply {
	args := WorkFinishArgs{}

	args.CallerId = w.id
	args.WorkId = workId
	args.WorkType = workType

	reply := WorkFinishReply{
		IsErr:  false,
		ErrStr: "",
	}

	ok := call("Coordinator.WorkFinish", &args, &reply)
	if ok && !reply.IsErr {
		w.logger.Println("Send finish message to coordinator: ", args)
	} else {
		w.logger.Println("Error to send finish message to coordinator: ", reply.ErrStr)
	}

	return reply
}

func (w *WorkerType) CallWorkerDeath() {
	args := WorkerDeathArgs{
		CallId: w.id,
	}

	reply := WorkerDeathReply{}

	ok := call("Coordinator.WorkerDeath", &args, &reply)
	if ok {
		w.logger.Println("Send death signal to coordinator")
	} else {
		w.logger.Println("Error to send death signal to coordinator")
	}
}

func (w *WorkerType) CallGetNReduce() int {
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
