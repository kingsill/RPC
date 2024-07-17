package mr

import (
	"log"
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
}

type Task struct {
	fileName string
	state    int
}

var TaskMapR map[int]*Task

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *Args, reply *TaskInfo) {
	args.state = Busy
	//首先分配Map
	for i, task := range c.TaskMap {
		if task.state == Idle {
			args.Tasktype = Map
			args.TaskId = i + 1
			reply.TaskType = Map
			reply.FileName = task.fileName
			reply.TaskId = i + 1 //range从0开始
		}
		return
	}

	//Map完成后再Reduce
	for _, task := range c.TaskMap {
		if task.state != Finish {
			return
		}
	}

	//分配Reduce
	for i, v := range c.ReduceMap {
		if v == Idle {
			args.Tasktype = Reduce
			args.TaskId = i + 1
			reply.TaskType = Reduce
			reply.TaskId = i + 1
		}
		return
	}

	//Reduce都结束则成功
	for _, v := range c.ReduceMap {
		if v == Finish {
		}
	}
	reply.TaskType = Over
	return
}

func (c *Coordinator) WorkerDone(args *Args, reply *TaskInfo) {
	id := args.TaskId
	switch args.Tasktype {
	case Map:
		c.TaskMap[id-1].state = Finish
	case Reduce:
		c.ReduceMap[id-1] = Finish
	}
}

func (c *Coordinator) Err() {

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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

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
	}

	// Your code here.

	c.server()
	c.AssignTask()
	return &c
}
