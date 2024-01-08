package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	splitted_file_names     []string
	mapper_created_files    map[int][]string
	current_mapper_job_list []*MapperJob

	reducerN                 int
	reducer_file_names       []string
	current_reducer_job_list []*ReducerJob

	//increment mapper id for each worker assigned as a mapper
	//current_mapper_id     int
	//splitted_file_index   int

	//treat task number as the index in the array-- easier to see whats happening

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FinishMapper(args *MapperFinishArgs, reply *MapperFinishReply) error {
	c.mutex.Lock()
	modified_obj := c.current_mapper_job_list[args.MapperIndex]
	if modified_obj.ProcessId == args.ProcessId {
		modified_obj.Finished = true

		if entries, ok := c.mapper_created_files[args.MapperIndex]; ok {
			//this shouldn't happen? wtf
			c.mapper_created_files[args.MapperIndex] = append(entries, args.Created_temp_files...)

		} else {
			c.mapper_created_files[args.MapperIndex] = args.Created_temp_files
		}
		c.current_reducer_job_list[args.MapperIndex].Consuming_file_names = args.Created_temp_files

	}
	c.mutex.Unlock()
	return nil

}

func (c *Coordinator) FinishReducer(args *ReducerFinishArgs, reply *ReducerFinishReply) error {
	c.mutex.Lock()
	modified_obj := c.current_reducer_job_list[args.ReducerIndex]
	if modified_obj.ProcessId == args.ProcessId {
		modified_obj.Finished = true
		c.reducer_file_names[args.ReducerIndex] = args.Created_reducer_file
		err := os.Rename(args.Created_reducer_file, "mr-out-"+strconv.Itoa(args.ReducerIndex))
		if err != nil {
			fmt.Printf("things gone wrong when making reducer temp files persistent %s", err)
		}

	}

	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) RequestForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.mutex.Lock()
	mapper_job_finished := true
	idx_of_available_mapper := 0
	/*if len(c.current_mapper_job_list) == len(c.splitted_file_names){
		for _,job := range c.current_mapper_job_list {
			if !job.finished {
				mapper_job_finished = false
			}
		}
	} else {
		mapper_job_finished = false
	}*/

	for idx, job := range c.current_mapper_job_list {
		if !job.In_progress && !job.Finished {
			mapper_job_finished = false
			idx_of_available_mapper = idx
			break
		}
	}

	if mapper_job_finished {
		for _, job := range c.current_reducer_job_list {
			if !job.In_progress && !job.Finished {
				reply.IsMapper = false
				job.In_progress = true
				job.ProcessId = args.ProcessId
				job.StartTime = time.Now()
				reply.ReducerJob = job
				break
			}
		}
	} else {
		reply.IsMapper = true
		c.current_mapper_job_list[idx_of_available_mapper].In_progress = true
		c.current_mapper_job_list[idx_of_available_mapper].ProcessId = args.ProcessId
		c.current_mapper_job_list[idx_of_available_mapper].StartTime = time.Now()
		reply.MapperJob = c.current_mapper_job_list[idx_of_available_mapper]

		//reply.MapperJob = MapperJob{args.ProcessId, c.splitted_file_names[idx_of_available_mapper], c.reducerN, idx_of_available_mapper, false}
	}

	c.mutex.Unlock()
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
	/*ret := false
	if len(c.current_mapper_job_list) == 0 && len(c.current_reducer_job_list) == 0 {
		println("Yeah the mapper jobs are produced without errors")
		ret = true
	}*/
	c.mutex.Lock()

	ret := true
	for _, job := range c.current_mapper_job_list {
		if !job.Finished {
			ret = false
		}
	}
	for _, job := range c.current_reducer_job_list {
		if !job.Finished {
			ret = false
		}
	}
	// Your code here.
	c.mutex.Unlock()
	if ret {
		fmt.Printf("I returned for some reason?????")
	}

	return ret
}

func (c *Coordinator) CheckDeadWorker() {
	for _, job := range c.current_mapper_job_list {
		if job.In_progress {
			after_10_seconds := job.StartTime.Add(10 * time.Second)

			if time.Now().After(after_10_seconds) {
				job.In_progress = false
			}
		}

	}
	for _, job := range c.current_reducer_job_list {
		if job.In_progress {
			after_10_seconds := job.StartTime.Add(10 * time.Second)

			if time.Now().After(after_10_seconds) {
				job.In_progress = false
			}
		}
	}
}

//periodically checks for timeout
func (c *Coordinator) Timeout() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					// Stop the ticker when done
					ticker.Stop()
					return
				}
				c.CheckDeadWorker()
			}
		}
	}()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.reducerN = nReduce
	c.mapper_created_files = make(map[int][]string)
	c.reducer_file_names = make([]string, nReduce)
	// Your code here.

	//intermediate := []KeyValue{}

	for index, filename := range os.Args[1:] {

		c.splitted_file_names = append(c.splitted_file_names, filename)
		c.current_mapper_job_list = append(c.current_mapper_job_list, &MapperJob{-1, filename, c.reducerN, index, time.Now(), false, false})
	}
	fmt.Printf("how many pg files" + strconv.Itoa(len(c.splitted_file_names)))

	for i := 0; i < c.reducerN; i++ {

		c.current_reducer_job_list = append(c.current_reducer_job_list, &ReducerJob{
			ProcessId:             -1,
			Current_reducer_index: i,
			In_progress:           false,
			Finished:              false,
		})
	}

	//c.splitted_file_names = append(c.splitted_file_names, os.Args[2:])

	c.server()
	return &c
}
