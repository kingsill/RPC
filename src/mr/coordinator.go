package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files      []string
	nReduce    int
	MapTask    map[int]*Task
	ReduceTask []int
	OK         bool
	Lock       sync.Mutex
}

type Task struct {
	fileName string
	state    int
}

var TaskMapR map[int]*Task

func (c *Coordinator) Verify(Arg *Args, Reply *TaskInfo) error {

	switch Arg.Tasktype {
	case Map:
		time.Sleep(3 * time.Second)
		if c.MapTask[Arg.TaskId].state != Finish {
			c.MapTask[Arg.TaskId].state = Idle
			Reply = &TaskInfo{}
		}
	case Reduce:
		time.Sleep(3 * time.Second)
		if c.ReduceTask[Arg.TaskId] != Finish {
			c.ReduceTask[Arg.TaskId] = Idle
			Reply = &TaskInfo{}
		}
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(Arg *Args, Reply *TaskInfo) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	//如果请求为空闲
	if Arg.State == Idle {

		//Args.State = Busy
		//首先分配Map
		for i, task := range c.MapTask {
			//fmt.Println(*task, "Id:", i)
			if task.state == Idle {
				//Arg.Tasktype = Map
				//Arg.TaskId = i + 1
				Reply.TaskType = Map
				Reply.FileName = task.fileName

				//fmt.Println(task.fileName)

				Reply.TaskId = i          //range从0开始
				Reply.NReduce = c.nReduce // 设置 NReduce
				Reply.Status = Busy
				task.state = Busy
				//fmt.Println("map,Id:", i)
				return nil
			}
		}

		//Map完成后再Reduce
		for _, task := range c.MapTask {
			if task.state != Finish {
				//fmt.Println("等待Map完成")
				return nil
			}
		}

		//fmt.Println("MapDone")

		//分配Reduce
		for i, v := range c.ReduceTask {
			//fmt.Println(c.ReduceTask)
			if v == Idle {
				Arg.Tasktype = Reduce
				Arg.TaskId = i
				Reply.TaskType = Reduce
				Reply.TaskId = i
				Reply.NReduce = c.nReduce // 设置 NReduce
				Reply.Status = Busy
				Reply.Nmap = len(c.files)
				c.ReduceTask[i] = Busy
				//fmt.Println(c.ReduceTask[i])
				//fmt.Println("reduce", i)
				return nil
			}
		}

		//Reduce都结束则成功
		for _, v := range c.ReduceTask {
			if v == Finish {
			} else {
				return nil
			}
		}
		Reply.TaskType = Over
		c.OK = true
	}

	return nil
}

func (c *Coordinator) WorkerDone(args *Args, reply *TaskInfo) error {
	//c.Lock.Lock()
	//defer c.Lock.Unlock()

	//reply清空
	reply = &TaskInfo{}
	//args.State = Finish

	id := args.TaskId
	//fmt.Println("id", id)
	switch args.Tasktype {
	case Map:
		c.MapTask[id].state = Finish
		//fmt.Println(*c.MapTask[id])
	case Reduce:
		c.ReduceTask[id] = Finish
		//fmt.Println(c.ReduceTask)
	}
	return nil
}

func (c *Coordinator) Err(args *Args, reply *TaskInfo) error {
	//c.Lock.Lock()
	//defer c.Lock.Unlock()

	reply = &TaskInfo{}
	id := args.TaskId
	switch args.Tasktype {
	case Map:
		if c.MapTask[id].state != Finish {
			c.MapTask[id].state = Idle
		}

	case Reduce:
		if c.ReduceTask[id] != Finish {
			c.ReduceTask[id] = Idle
		}
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
	//fmt.Println("Coordinator is listening on", sockname)
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

func (c *Coordinator) Done() bool {
	//c.Lock.Lock()
	//defer c.Lock.Unlock()

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
	//fmt.Println(files)
	TaskMapR = make(map[int]*Task, len(files))

	for i, file := range files {
		TaskMapR[i] = &Task{
			fileName: file,
			state:    Idle,
		}
	}

	ReduceMap := make([]int, nReduce)

	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		MapTask:    TaskMapR,
		ReduceTask: ReduceMap,
		OK:         false,
	}

	// Your code here.

	c.server()

	return &c
}
