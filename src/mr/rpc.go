package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//


// Add your RPC definitions here.

type InputFileNameRequest struct{
	WorkerId int
}

type InputFileNameReply struct {

	WorkId int
	FileName []string
}


type PushIntermediateFileRequest struct{

	WorkId int
	IntermediateFileName []string
}

type PushIntermediateFileReply struct{
	WorkerId int
	Ok bool
}

type ReduceInputFileNameRequest struct {
	WorkerId int
}

type ReduceInputFileNameReply struct{

	WorkerId int
	FileName []string
}


type MapDone struct {
	WorkerId int
}

type MapDoneReply struct {
	WorkerId int
	Ok bool
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
