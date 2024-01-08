package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type WorkerHelper struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	pid     int
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

	w := WorkerHelper{}

	w.mapf = mapf
	w.reducef = reducef
	w.pid = os.Getpid()

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	w.CallForReqTaskAndExecite()

}

func (w *WorkerHelper) ProcessMapper(mapperJob *MapperJob) {
	//create nReduce files

	arr1 := make([]*os.File, mapperJob.ReduceN)
	arr_names := make([]string, mapperJob.ReduceN)
	encoders := make([]json.Encoder, mapperJob.ReduceN)
	for reducer_idx := 0; reducer_idx < mapperJob.ReduceN; reducer_idx++ {
		tempFile, err := ioutil.TempFile("", "temp-"+strconv.Itoa(mapperJob.Current_mapper_index)+"-"+strconv.Itoa(reducer_idx))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error when creation: %s", err)
			os.Stderr.Sync()
		}
		encoders[reducer_idx] = *json.NewEncoder(tempFile)
		arr_names[reducer_idx] = tempFile.Name()
		arr1[reducer_idx] = tempFile
		if err != nil {
			fmt.Println("Error creating temp file:", err)

		}

		//defer os.Remove(tempFile.Name())

		fmt.Println("Temporary file created with name:", tempFile.Name())
		os.Stderr.Sync()
	}

	filename := mapperJob.Consuming_file_name

	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapf(filename, string(content))

	for _, kv := range kva {
		file_index := ihash(kv.Key) % mapperJob.ReduceN
		err := encoders[file_index].Encode(&kv)
		if err != nil {
			panic(err)
		}

	}
	ags := MapperFinishArgs{w.pid, mapperJob.Current_mapper_index, arr_names}
	reply := MapperFinishReply{}
	sendback_task_ok := call("Coordinator.FinishMapper", &ags, &reply)
	if !sendback_task_ok {
		panic(sendback_task_ok)
	}

}

func (w *WorkerHelper) ProcessReducer(reducerJob *ReducerJob) {
	all_temp_files := reducerJob.Consuming_file_names
	intermediate := []KeyValue{}
	for _, filename := range all_temp_files {
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("shit happened while opening file in reducer")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}

	sort.Sort(ByKey(intermediate))

	tempFile, err := ioutil.TempFile("", "temp-mr-out-"+strconv.Itoa(reducerJob.Current_reducer_index))
	//ADD TMR PLZ WILL BE FAST

	/*
		i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
	*/
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when creation: %s", err)
		os.Stderr.Sync()
	}
	fmt.Println("(For reducer!!!)Temporary file created with name:", tempFile.Name())
	os.Stderr.Sync()

	ags := ReducerFinishArgs{w.pid, reducerJob.Current_reducer_index, tempFile.Name()}
	reply := ReducerFinishReply{}
	sendback_task_ok := call("Coordinator.FinishReducer", &ags, &reply)
	if !sendback_task_ok {
		panic(sendback_task_ok)
	}
}

func (w *WorkerHelper) CallForReqTaskAndExecite() {
	for {
		fmt.Fprintf(os.Stderr, "Trying to get a job!!!")
		os.Stderr.Sync()
		ags := AskForTaskArgs{os.Getpid()}
		reply := AskForTaskReply{}

		request_for_task_ok := call("Coordinator.RequestForTask", &ags, &reply)

		if request_for_task_ok {
			if reply.IsMapper {
				w.ProcessMapper(reply.MapperJob)
			} else {
				w.ProcessReducer(reply.ReducerJob)
			}

		} else {
			fmt.Printf("quitting due to unable to connect to host")
			return
		}
		time.Sleep(time.Second)
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
