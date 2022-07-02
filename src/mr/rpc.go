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

// Add your RPC definitions here.

type WorkerIdAndReduceNumberRequest struct {
	WorkerId int
}
type WorkerIdAndReduceNumberReply struct {
	WorkerId     int
	ReduceNumber int
}

type GetTaskRequest struct {
	WorkerId int
}

type GetTaskReply struct {
	TaskId   int
	FileName []string
}

type ReportCompleteRequest struct {
	TaskId               int
	IntermediateFileName []string
}

type ReportCompleteReply struct {
	Done bool
}

type GetReduceTaskRequest struct {
	WorkerId int
}

type GetReduceTaskReply struct {
	TaskId   int
	WorkerId int
	FileName []string
}

type ReportReduceDoneRequest struct {
	TaskId   int
	WorkerId int
}

type ReportReduceDoneReply struct {
	CompleteDone bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
