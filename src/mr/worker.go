package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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

	Ch := make(chan bool)
	for {
		var Res = &Args{State: Idle} //初始化为idle状态
		var TaskInformation = &TaskInfo{}
		GetTask(Res, TaskInformation)

		//主任务结束后不再请求
		if TaskInformation.TaskType == Over {
			break
		}
		//fmt.Println("do it!")
		go DoTask(TaskInformation, mapf, reducef, Ch)

		sign := <-Ch
		//fmt.Println("sign:", sign)

		if sign == true {
			//fmt.Println("Finish one,ID:", TaskInformation.TaskId)
			Done(Res, TaskInformation)

		} else {
			//TaskInformation.Status = Idle
			//fmt.Println("err one,ID:", TaskInformation.TaskId)
			call("Coordinator.Err", Res, TaskInformation)
			Res = &Args{State: Idle}
		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func GetTask(Args *Args, TaskInformation *TaskInfo) {
	// 调用coordinator获取任务
	for {
		call("Coordinator.AssignTask", Args, TaskInformation)
		//fmt.Println(TaskInformation)

		if TaskInformation.Status != Idle {
			Args.State = Busy
			Args.Tasktype = TaskInformation.TaskType
			Args.TaskId = TaskInformation.TaskId
			//fmt.Println("TaskInfo:", TaskInformation)
			//fmt.Println("Args：", Args)
			call("Coordinator.Verify", Args, TaskInformation)
			break
		}

		time.Sleep(time.Second)
	}
	//fmt.Printf("Type:%v,Id:%v\n", TaskInformation.TaskType, TaskInformation.TaskId)
}

func writeKVs(KVs []KeyValue, info *TaskInfo, fConts []*os.File) {
	//fConts := make([]io.Writer, info.NReduce)
	KVset := make([][]KeyValue, info.NReduce)

	//fmt.Println("start write")

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
	//fmt.Println("kvset:", KVset)

	for i, v := range KVset {

		for _, value := range v {
			data, _ := json.Marshal(value)
			_, err := fConts[i].Write(data)

			//fmt.Println("data: ", data)
			//fmt.Println("numbers:", write)
			if err != nil {
				return
			}

		}
	}
	//fmt.Println("finish write")
}

func read(filename string) []byte {
	//fmt.Println("read", filename)
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		fmt.Println(err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	return content
}

// DoTask 执行mapf或者reducef任务

func DoTask(info *TaskInfo, mapf func(string, string) []KeyValue, reducef func(string, []string) string, Ch chan bool) {
	//fConts := make([]io.Writer, info.NReduce)

	//fmt.Println("start", info.TaskId)
	//go AssignAnother(Ch)
	switch info.TaskType {
	case Map:
		info.FileContent = string(read(info.FileName))
		//fmt.Println(info.FileContent)
		KVs := mapf(info.FileName, info.FileContent.(string))
		//fmt.Println("map:", KVs)
		//将其排序
		sort.Sort(ByKey(KVs))

		var fConts []*os.File // 修改为 *os.File 类型

		//0-9
		for j := 0; j < info.NReduce; j++ {

			//暂时名，完成后重命名
			fileName := fmt.Sprintf("mr-%v-%v-test", info.TaskId, j)
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

			//fmt.Println("creatfile:  ", fileName)

			//fConts[j] = f
			fConts = append(fConts, f)
			defer os.Rename(fileName, strings.TrimSuffix(fileName, "-test"))
			defer f.Close()
		}

		writeKVs(KVs, info, fConts)

	case Reduce:
		fileName := fmt.Sprintf("testmr-out-%v", info.TaskId)
		fileOS, err := os.Create(fileName)
		//fmt.Println("create success")
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer os.Rename(fileName, strings.TrimPrefix(fileName, "test"))
		defer fileOS.Close()
		var KVs []KeyValue
		//读取文件
		for i := 0; i < info.Nmap; i++ {
			fileName := fmt.Sprintf("mr-%v-%v", i, info.TaskId)
			//fmt.Println(fileName)

			file, err := os.Open(fileName)
			defer file.Close()
			if err != nil {
				fmt.Println(err)
			}

			dec := json.NewDecoder(file)

			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				//fmt.Println(kv)
				KVs = append(KVs, kv)
			}
		}
		//var KVsRes []KeyValue
		sort.Sort(ByKey(KVs))
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
			output := reducef(KVs[i].Key, values)
			//每个key对应的计数
			//KVsRes = append(KVsRes, KeyValue{KVs[i].Key, output})

			fmt.Fprintf(fileOS, "%v %v\n", KVs[i].Key, output)

			i = j
		}

	}
	Ch <- true

}

func Done(Arg *Args, Info *TaskInfo) {

	//Info.Status = Idle
	call("Coordinator.WorkerDone", Arg, Info)

	//arg重新清空
	Arg = &Args{State: Idle}
}

func AssignAnother(Ch chan bool) {
	time.Sleep(2 * time.Second)

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
		//log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		//fmt.Println(err)
		return false
	}

	return true
}
