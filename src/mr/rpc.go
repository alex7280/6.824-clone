package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

// (first) RPC: worker ask for a task from coordinator
type MapperJob struct {
	ProcessId            int
	Consuming_file_name  string
	ReduceN              int
	Current_mapper_index int
	StartTime            time.Time
	In_progress          bool
	Finished             bool
}

type ReducerJob struct {
	ProcessId             int
	Consuming_file_names  []string
	Current_reducer_index int
	StartTime             time.Time
	In_progress           bool
	Finished              bool
}

type AskForTaskArgs struct {
	ProcessId int
}

type AskForTaskReply struct {
	IsMapper   bool
	MapperJob  *MapperJob
	ReducerJob *ReducerJob
}

type MapperFinishArgs struct {
	ProcessId          int
	MapperIndex        int
	Created_temp_files []string
}

type MapperFinishReply struct {
}

type ReducerFinishArgs struct {
	ProcessId            int
	ReducerIndex         int
	Created_reducer_file string
}

type ReducerFinishReply struct {
}

//type RPC

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
