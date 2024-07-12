package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
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
	Map    = iota
	Reduce = iota
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
	var TaskInfo TaskInfo
	TaskInfo = GetTask()

	DoTask(TaskInfo, mapf, reducef)

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func GetTask() TaskInfo {
	call("Coordinator.AssignTask", &Args{}, &TaskInfo{})
	return TaskInfo{}
}

func writeKVs(KVs []KeyValue, info TaskInfo, fConts []io.Writer) {
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

	for _, v := range KVs {
		var Order int
		Order = ihash(v.Key)
		KVset[Order] = append(KVset[Order], v)
	}
	for i, v := range KVset {
		data, _ := json.Marshal(v)
		fConts[i+1].Write(data)
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

func DoTask(info TaskInfo, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	fConts := make([]io.Writer, info.NReduce)

	switch info.TaskType {
	case Map:
		info.FileContent = string(read(info.FileName))
		KVs := mapf(info.FileName, info.FileContent.(string))

		//将其排序
		sort.Sort(ByKey(KVs))

		for j := 1; j <= info.NReduce; j++ {

			fileName := fmt.Sprintf("mr-%v-%v", info.TaskId, j)
			os.Create(fileName)

			f, _ := os.Open(fileName)
			fConts[j-1] = f

			defer f.Close()
		}

		writeKVs(KVs, info, fConts)

	case Reduce:

		os.Create(fmt.Sprintf("mr-out-%v", info.TaskId))

		//读取文件
		info.FileContent = read(info.FileName)

		var KVs []KeyValue
		var KVsRes []KeyValue

		//解码为KVs
		err := json.Unmarshal(info.FileContent.([]byte), &KVs)
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
			i = j
		}

		for j := 1; j <= info.FileNum; j++ {
			fileName := fmt.Sprintf("mr-out-%v", info.TaskId)

			f, _ := os.Open(fileName)
			fConts[j-1] = f
			defer f.Close()

			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				KVs = append(KVs, kv)
			}
		}

	}

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
