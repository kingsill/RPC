package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files     []string
	nReduce   int
	TaskMap   map[int]*Task
	ReduceMap []int
	OK        bool
	Lock      sync.Mutex
}

type Task struct {
	fileName string
	state    int
}

var TaskMapR map[int]*Task

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *Args, reply *TaskInfo) error {

	c.Lock.Lock()
	defer c.Lock.Unlock()

	args.State = Busy
	//首先分配Map
	for i, task := range c.TaskMap {
		if task.state == Idle {
			args.Tasktype = Map
			args.TaskId = i + 1
			reply.TaskType = Map
			reply.FileName = task.fileName
			fmt.Println(task.fileName)
			reply.TaskId = i + 1      //range从0开始
			reply.NReduce = c.nReduce // 设置 NReduce
			reply.Status = Busy
			task.state = Busy
			fmt.Println("map")
			return nil
		}
	}

	//Map完成后再Reduce
	for _, task := range c.TaskMap {
		if task.state != Finish {
			fmt.Println("等待Map完成")
			return nil
		}
	}

	fmt.Println("MapDone")

	//分配Reduce
	for i, v := range c.ReduceMap {
		if v == Idle {
			args.Tasktype = Reduce
			args.TaskId = i + 1
			reply.TaskType = Reduce
			reply.TaskId = i + 1
			reply.NReduce = c.nReduce // 设置 NReduce
			reply.Status = Busy
			v = Busy
			fmt.Println("reduce")
			return nil
		}
	}

	//Reduce都结束则成功
	for _, v := range c.ReduceMap {
		if v == Finish {
		} else {
			return nil
		}
	}
	reply.TaskType = Over
	c.OK = true

	return nil
}

func (c *Coordinator) WorkerDone(args *Args, reply *TaskInfo) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	id := args.TaskId
	switch args.Tasktype {
	case Map:
		c.TaskMap[id-1].state = Finish
	case Reduce:
		c.ReduceMap[id-1] = Finish
	}
	return nil
}

func (c *Coordinator) Err(args *Args, reply *TaskInfo) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	id := args.TaskId
	switch args.Tasktype {
	case Map:
		c.TaskMap[id-1].state = Idle
	case Reduce:
		c.ReduceMap[id-1] = Idle
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("Coordinator is listening on", sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	ret := false
	if c.OK == true {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println(files)
	TaskMapR = make(map[int]*Task, len(files))

	for i, file := range files {
		TaskMapR[i] = &Task{
			fileName: file,
			state:    Idle,
		}
	}

	ReduceMap := make([]int, nReduce)

	c := Coordinator{
		files:     files,
		nReduce:   nReduce,
		TaskMap:   TaskMapR,
		ReduceMap: ReduceMap,
		OK:        false,
	}

	// Your code here.

	c.server()

	return &c
}
