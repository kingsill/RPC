package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	Map = iota
	Reduce
	Over
)
const (
	Idle = iota
	Busy
	Finish
)

// Map functions return a slice of KeyValue.

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	var TaskInfo = &TaskInfo{}
	var Res = &Args{State: Idle} //初始化为idle状态
	Ch := make(chan bool)
	for {
		GetTask(Res, TaskInfo)

		//主任务结束后不再请求
		if TaskInfo.TaskType == Over {
			break
		}

		go AssignAnother(Ch)
		go DoTask(TaskInfo, mapf, reducef, Ch)

		select {
		/*		case Ch <- true:
					Done(Res)
				case Ch <- false:
					call("Coordinator.Err", Res, &TaskInfo)*/

		case <-Ch:
			Done(Res)
		default:
			call("Coordinator.Err", Res, &TaskInfo)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func GetTask(args *Args, TaskInfo *TaskInfo) {
	// 调用coordinator获取任务
	for {
		call("Coordinator.AssignTask", args, TaskInfo)
		if TaskInfo.TaskType != Idle {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("Type:%v,Id:%v\n", TaskInfo.TaskType, TaskInfo.TaskId)
}

func writeKVs(KVs []KeyValue, info *TaskInfo, fConts []*os.File) {
	//fConts := make([]io.Writer, info.NReduce)
	KVset := make([][]KeyValue, info.NReduce)

	//for j := 1; j <= info.NReduce; j++ {
	//
	//	fileName := fmt.Sprintf("mr-%v-%v", info.TaskId, j)
	//	os.Create(fileName)
	//
	//	f, _ := os.Open(fileName)
	//	fConts[j-1] = f
	//
	//	defer f.Close()
	//}
	var Order int
	for _, v := range KVs {
		Order = ihash(v.Key) % info.NReduce
		KVset[Order] = append(KVset[Order], v)
	}
	for i, v := range KVset {
		data, _ := json.Marshal(v)
		fmt.Println(data)
		fConts[i].Write(data)
	}
}

func read(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

// DoTask 执行mapf或者reducef任务

func DoTask(info *TaskInfo, mapf func(string, string) []KeyValue, reducef func(string, []string) string, Ch chan bool) {
	//fConts := make([]io.Writer, info.NReduce)

	switch info.TaskType {
	case Map:
		info.FileContent = string(read(info.FileName))
		KVs := mapf(info.FileName, info.FileContent.(string))

		//将其排序
		sort.Sort(ByKey(KVs))

		var fConts []*os.File // 修改为 *os.File 类型

		for j := 0; j < info.NReduce; j++ {

			fileName := fmt.Sprintf("mr-%v-%v", info.TaskId, j)
			//_, err := os.Create(fileName)
			//if err != nil {
			//	fmt.Println(err)
			//	return
			//}
			//
			//f, _ := os.Open(fileName)
			//fConts[j-1] = f
			//
			//defer f.Close()
			f, err := os.Create(fileName) // 直接使用 Create 函数
			if err != nil {
				fmt.Println(err)
				return
			}
			//fConts[j] = f
			fConts = append(fConts, f)
			defer f.Close()
		}

		writeKVs(KVs, info, fConts)

	case Reduce:

		fileOS, err := os.Create(fmt.Sprintf("mr-out-%v", info.TaskId))
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer fileOS.Close()
		//读取文件
		info.FileContent = read(info.FileName)

		var KVs []KeyValue
		var KVsRes []KeyValue

		//解码为KVs
		err = json.Unmarshal(info.FileContent.([]byte), &KVs)
		if err != nil {
			return
		}

		//整理并传输内容给reduce
		i := 0
		for i < len(KVs) {
			j := i + 1
			for j < len(KVs) && KVs[j].Key == KVs[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, KVs[k].Value)
			}
			// this is the correct format for each line of Reduce output.

			//每个key对应的计数
			value := reducef(KVs[i].Key, values)
			KVsRes = append(KVsRes, KeyValue{KVs[i].Key, value})

			fmt.Fprintf(fileOS, "%v %v\n", KVs[i].Key, KVsRes)

			i = j
		}

	}
	Ch <- true

}

func Done(args *Args) {
	args.State = Finish
	call("Coordinator.WorkerDone", args, &TaskInfo{})
}

func AssignAnother(Ch chan bool) {
	time.After(10 * time.Second)

	Ch <- false
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
	//fmt.Println("Worker is dialing", sockname)
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
