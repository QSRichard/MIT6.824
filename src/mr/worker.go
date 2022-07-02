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
	// "time"
	// "time"
	// "golang.org/x/tools/go/analysis/passes/nilfunc"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

/*
*	将全部任务切分
*		每个map处理部分任务，并根据key将处理结果哈希到多个不同的文件中
*		每个reduce读取自己对应的由map产生的全部哈希文件并处理
 */
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := WorkerIdAndReduceNumberReply{}
	CallWorkerIdAndReduceNumber(&reply)
	nreduce := reply.ReduceNumber
	workerId := reply.WorkerId
	MapStageFunc(mapf, nreduce, workerId)

	ReduceStageFunc(reducef)

	time.Sleep(10 * time.Second)
}

func MapStageFunc(mapf func(string, string) []KeyValue, nreduce int, workerId int) {

	for {
		reply := GetTaskReply{}
		CallDoGetTask(&reply, workerId)
		taskId := reply.TaskId
		if taskId == -1 {
			return
		}
		intermediates := []KeyValue{}

		for _, name := range reply.FileName {
			file, err := os.Open(name)
			if err != nil {
				log.Fatalf("Can not open %v", name)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Can not read %v", name)
			}
			file.Close()
			kva := mapf(name, string(content))
			intermediates = append(intermediates, kva...)
		}

		// 应该创建临时文件
		basereducefilename := "mr-" + strconv.Itoa(taskId)
		var reduceinputfile = make([]*os.File, nreduce)
		for i := 0; i < nreduce; i++ {
			reduceinputfile[i], _ = os.Create(basereducefilename + "-" + strconv.Itoa(i))
		}

		// 根据key的哈希值，写文件
		var allEnc = make([]*json.Encoder, nreduce)
		for i := 0; i < nreduce; i++ {
			allEnc[i] = json.NewEncoder(reduceinputfile[i])
		}
		for _, kv := range intermediates {

			err := allEnc[ihash(kv.Key)%nreduce].Encode(&kv)
			if err != nil {
				log.Print(err)
				log.Fatalf("Json file write error")
			}
		}

		// 向server报告任务完成
		reportRequest := ReportCompleteRequest{}
		reportRequest.TaskId = taskId
		var intermediatefilename []string
		for i := 0; i < nreduce; i++ {
			intermediatefilename = append(intermediatefilename, basereducefilename+"-"+strconv.Itoa(i))
		}
		reportRequest.IntermediateFileName = append(reportRequest.IntermediateFileName, intermediatefilename...)
		reportReply := ReportCompleteReply{}
		CallDoReportComplete(&reportRequest, &reportReply)
		if reportReply.Done {
			return
		}
	}
}

func ReduceStageFunc(reducef func(string, []string) string, id int, ch chan bool) {

	reply := ReduceGetTaskReply{}
	reply.FileName = make([]string, 0)
	request := ReduceGetTaskRequest{}
	request.WorkerId = id

	CallGetReduceInputFileName(&request, &reply)

	var filename = reply.FileName

	var decs []*json.Decoder

	for i := 0; i < len(filename); i++ {
		file, _ := os.Open(filename[i])
		decs = append(decs, json.NewDecoder(file))
	}

	var kva ByKey

	for _, dec := range decs {
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(kva)
	workId := reply.WorkerId

	outfile, _ := os.Create("mr-out" + strconv.Itoa(workId))
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
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outfile.Close()
}

func CallWorkerIdAndReduceNumber(reply *WorkerIdAndReduceNumberReply) {
	request := WorkerIdAndReduceNumberRequest{}
	request.WorkerId = -1
	ok := call("Coordinator.DoGetWorkerIdAndReduceNumber", &request, reply)
	if !ok {
		fmt.Println("call failed! DoGetWorkerIdAndReduceNumber")
	}
}

func CallDoGetTask(reply *GetTaskReply, WorkerId int) {

	// declare an argument structure.
	args := GetTaskRequest{}
	args.WorkerId = WorkerId
	ok := call("Coordinator.DoGetTask", &args, reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.WorkId %v reply.FileName %v\n", reply.WorkId, reply.FileName)
	} else {
		fmt.Printf("call failed! CallDoGetTask\n")
	}
}

func CallDoReportComplete(request *ReportCompleteRequest, reply *ReportCompleteReply) {

	ok := call("Coordinator.DoReportComplete", request, reply)
	if !ok {
		fmt.Printf("call failed DoReportComplete\n")
	}
}

func CallDoGetReduceTask(request *GetReduceTaskRequest, reply *GetReduceTaskReply) {
	ok := call("Coordinator.DoGetReduceTask", request, reply)
	if !ok {
		fmt.Printf("call failed DoGetReduceTask\n")
	}
}

func CallDoReportReduceDone(request *ReportReduceDoneRequest, reply *ReportReduceDoneReply) {
	ok := call("Coordinator.CallDoReportReduceDone", request, reply)
	if !ok {
		fmt.Printf("call failed CallDoReportReduceDone\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
