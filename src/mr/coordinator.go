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

	Mx                  sync.Mutex
	ReduceNumber        int
	Index               int
	WorkerNumber        int
	State               []string
	InputFileName       []string
	Tasks               [][]string
	TasksStatus         []int
	TasksNumber         int
	CompleteMapTask     int
	left                int
	ReduceInputFileName [][]string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Coordinator) DoGetWorkerIdAndReduceNumber(request *WorkerIdAndReduceNumberRequest, reply *WorkerIdAndReduceNumberReply) error {
	if request.WorkerId != -1 {
		return nil
	}
	reply.ReduceNumber = m.ReduceNumber
	reply.WorkerId = m.Index
	m.Mx.Lock()
	m.Index++
	m.Mx.Unlock()
	return nil
}

func (m *Coordinator) DoGetTask(request *GetTaskRequest, reply *GetTaskReply) error {

	// TODO(qiaoshuo): 增加逻辑
	if request.WorkerId == -1 {
		return nil
	}
	m.Mx.Lock()
	// -1 表示正在处理 1表示处理完成 0表示没有安排
	// TODO(qiaoshuo.qs): go的final实现 减小锁的粒度
	for i := 0; i < m.TasksNumber; i++ {
		if m.TasksStatus[i] == 0 {
			m.TasksStatus[i] = -1
			m.Mx.Unlock()
			reply.TaskId = i
			reply.FileName = m.Tasks[i]
			return nil
		}
	}
	m.Mx.Unlock()
	reply.TaskId = -1
	return nil
}

func (m *Coordinator) DoReportComplete(request *ReportCompleteRequest, reply *ReportCompleteReply) error {
	if request.TaskId == -1 {
		fmt.Println("DoReportComplete request.TaskId==-1")
		return nil
	}
	m.ReduceInputFileName = append(m.ReduceInputFileName, request.IntermediateFileName)
	m.left--
	m.TasksStatus[request.TaskId] = 1
	if m.left != 0 {
		reply.Done = true
	} else {
		reply.Done = false
	}
	return nil
}

func (m*Coordinator) DoGetReduceTask(request *GetReduceTaskRequest, reply *GetReduceTaskReply){
	if request.WorkerId == -1{
		return nil
	}
}

func (m*Coordinator) DoReportReduceDone(request *ReportReduceDoneRequest, reply *ReportReduceDoneReply)
{

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
	c.Index = 0
	c.ReduceNumber = nReduce
	c.Tasks = make([][]string, nReduce)
	c.State = make([]string, nReduce)
	c.ReduceInputFileName = make([][]string, 0)

	// Your code here.

	taskFileNumber := len(files) / nReduce
	if taskFileNumber == 0 {
		taskFileNumber = 1
	}

	var begin int = 0
	var end int = 0

	// 将files切割分配为tasks
	for i := 0; i < nReduce; i++ {

		if i == nReduce-1 {
			c.Tasks[i] = files[i*taskFileNumber:]
			break
		}

		begin = i * taskFileNumber
		end = begin + taskFileNumber
		if end > len(files) {
			end = len(files)
		}

		c.Tasks[i] = files[begin:end]
		if end >= len(files) {
			break
		}
	}

	c.server()
	return &c
}
