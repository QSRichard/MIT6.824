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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// var nreduce int = 10
	// 通过RPC得到workId以及Map stage InputFileName
	ch := make(chan int,100)
	for i := 0; i < 10; i++ {
		go MapStageFunc(mapf, ch)
	}
	// 向coordinator请求intermediate File
	// for i := 0; i < 7; i++ {
	// 	<-ch
	// }



	time.Sleep(5*time.Second)


	ch2 := make (chan bool,100)
	for i := 0; i < 10; i++ {
		// id:=<-ch
		go ReduceStageFunc(reducef, i,ch2)
	}
	// for i:=0;;i++{
	// 	reply:=MapDoneReply{}
	// 	CallMapDone(&reply)
	// 	if(reply.Ok){
	// 		break;
	// 	}
	// 	time.Sleep(1*time.Second)
	// }

	// reducefilename:=ReduceInputFileNameReply{}
	// CallGetReduceInputFileName(&reducefilename)

	time.Sleep(10*time.Second)
}

func MapStageFunc(mapf func(string, string) []KeyValue, ch chan int) {

	var nreduce int = 10
	reply := InputFileNameReply{}
	CallGetInputFileName(&reply)
	workerId := reply.WorkId

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

		// 得到Intermediates KV Pairs
		kva := mapf(name, string(content))
		intermediates = append(intermediates, kva...)

	}

	// 应该创建临时文件
	basereducefilename := "mr-" + strconv.Itoa(workerId)

	var reduceinputfile [10]*os.File
	for i := 0; i < 10; i++ {
		log.Println("Create File")
		reduceinputfile[i], _ = os.Create(basereducefilename + "-" + strconv.Itoa(i))
	}

	// 创建reduce的输入文件

	// var allEnc []*json.Encoder
	var allEnc = make([]*json.Encoder, nreduce)

	for i := 0; i < 10; i++ {
		allEnc[i] = json.NewEncoder(reduceinputfile[i])
	}
	//   enc := json.NewEncoder(reduceinputfile[0])

	for _, kv := range intermediates {

		err := allEnc[ihash(kv.Key)%nreduce].Encode(&kv)
		if err != nil {
			log.Print(err)
			log.Fatalf("Json file write error")
		}
	}

	pushrequest :=PushIntermediateFileRequest{}
	pushrequest.WorkId=workerId
	var intermediatefilename []string
	for i:=0;i<10;i++{
		intermediatefilename = append(intermediatefilename, basereducefilename+"-"+strconv.Itoa(i))
		// fmt.Println(basereducefilename+"-"+strconv.Itoa(i))
	}
	pushrequest.IntermediateFileName = append(pushrequest.IntermediateFileName, intermediatefilename...)
	fmt.Println(pushrequest.IntermediateFileName)
	pushreply :=PushIntermediateFileReply{}
	CallPushIntermediateFile(&pushrequest,&pushreply)

	ch <- workerId
}

func ReduceStageFunc(reducef func(string, []string) string, id int, ch chan bool) {

	reply := ReduceInputFileNameReply{}
	reply.FileName=make([]string, 0)
	request :=ReduceInputFileNameRequest{}
	request.WorkerId=id

	CallGetReduceInputFileName(&request,&reply)

	var filename=reply.FileName

	fmt.Println("reply Reduce filename:",filename)

	// for i:=0;i<len(filename);i++{
	// 	fmt.Println("---------"+reply.FileName[i]+"-----------")
	// }


	var decs []*json.Decoder

	for i:=0;i<len(filename);i++{
		file,_:=os.Open(filename[i])
		decs=append(decs, json.NewDecoder(file))
	}


	var kva ByKey

	for _,dec :=range decs{
		for{
			var kv KeyValue
			if err:=dec.Decode(&kv);err!=nil{
				break;
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(kva)
	workId:=reply.WorkerId

	fmt.Println("CREATE")
	outfile,_:= os.Create("mr-out"+strconv.Itoa(workId))
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

func CallGetInputFileName(reply *InputFileNameReply) {

	// declare an argument structure.
	args := InputFileNameRequest{}
	args.WorkerId = -1
	ok := call("Coordinator.GetInputFileName", &args, reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.WorkId %v reply.FileName %v\n", reply.WorkId, reply.FileName)
	} else {
		fmt.Printf("call failed! CallGetInputFileName\n")
	}
}


func CallPushIntermediateFile(request *PushIntermediateFileRequest,reply*PushIntermediateFileReply){


	ok:=call("Coordinator.PushIntermediateFile",request,reply)
	if!ok{
		fmt.Printf("call failed CallPushIntermediateFile\n")
	}
}


func CallGetReduceInputFileName(request *ReduceInputFileNameRequest,reply *ReduceInputFileNameReply) {


	ok := call("Coordinator.RecuceInputFileName", request,reply)
	if !ok {
		fmt.Printf("call failed! CallGetReduceInputFileName\n")
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
