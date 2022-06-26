package mr

import (
	// "fmt"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	// "strconv"
	"sync"
	// "golang.org/x/tools/go/analysis/passes/nilfunc"
	// "golang.org/x/tools/go/analysis/passes/nilfunc"
	// "golang.org/x/tools/go/analysis/passes/nilfunc"
	// "golang.org/x/tools/go/analysis/passes/nilfunc"
)


type Coordinator struct {
	// Your definitions here.

	Mx sync.Mutex
	Index int
	WorkerNumber int
	State []string
	InputFileName []string
	Tasks [][]string
	CompleteMapTask int
	ReduceInputFileName [][]string

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Coordinator) GetInputFileName(request *InputFileNameRequest, reply *InputFileNameReply) error {

	if request.WorkerId!=-1{
		return nil
	}
	m.Mx.Lock()
	reply.WorkId=m.Index
	m.Index++
	m.Mx.Unlock()
	reply.FileName = m.Tasks[reply.WorkId]
	return nil
}


func (m* Coordinator) PushIntermediateFile(request *PushIntermediateFileRequest,reply *PushIntermediateFileReply) error{
	if request.WorkId==-1{
		fmt.Println("PushIntermediateFile request.WorkId==-1")
		return nil
	}
	
	m.ReduceInputFileName = append(m.ReduceInputFileName, request.IntermediateFileName)
	reply.WorkerId=request.WorkId
	reply.Ok=true
	return nil
}



func (m*Coordinator) RecuceInputFileName(request *ReduceInputFileNameRequest,reply *ReduceInputFileNameReply) error{
	
	if request.WorkerId==-1{
		fmt.Println("RecuceInputFileName request.WorkId==-1")
		return nil
	}

	reply.WorkerId=request.WorkerId


	// log.Print("--------------%-------------")

	// fmt.Println(len(m.ReduceInputFileName))
	for i:=0;i<len(m.ReduceInputFileName);i++{
		// fmt.Println("!!!!!!!!!!!!",reply.WorkerId,m.ReduceInputFileName[i][reply.WorkerId])
		reply.FileName = append(reply.FileName, m.ReduceInputFileName[i][reply.WorkerId])
	}
	fmt.Println("!!!!!!!!!!!!",reply.FileName)
	return nil

}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

// 参数分别是输入文件名，以及reduce数量
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Tasks=make([][]string, 10)
	c.State=make([]string, 10)
	c.ReduceInputFileName=make([][]string, 0)

	// for i:=0;i<len(c.ReduceInputFileName);i++{
	// 	c.ReduceInputFileName[i]=make([]string,0)
	// }
	// Your code here.

	taskFileNumber := len(files)/nReduce
	if taskFileNumber == 0{
		taskFileNumber = 1
	}

	// var tasks [][]string
	var begin int =0
	var end int = 0

	// 将files切割分配为tasks
	for i:=0;i<nReduce;i++{

		if(i==nReduce-1){
			c.Tasks[i]=files[i*taskFileNumber:]
			break
		}

		begin=i*taskFileNumber
		end=begin+taskFileNumber
		if(end>len(files)){
			end=len(files)
		}

		c.Tasks[i]=files[begin:end]
		if(end>=len(files)){
			break
		}
	}

	for i:=0;i<nReduce;i++{
		log.Printf("task i %v %v", i,c.Tasks[i])
	}
	c.server()
	return &c
}
