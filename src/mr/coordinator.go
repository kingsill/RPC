package mr

import (
	"fmt"
	"io/ioutil"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask() {

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
	c := Coordinator{}

	// Your code here.

	//创建nxm个中间文件   XXXX 应该在Map中进行建立，不涉及并发安全，每个只对应一个Map或Reduce
	Num := len(files)
	for i := 1; i <= Num; i++ {
		for j := 1; j <= nReduce; j++ {
			fileName := fmt.Sprintf("mr-%v-%v", i, j)
			os.Create(fileName)
		}
	}

	for _, fileName := range files {
		file, err := os.Open(fileName)

		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
		}
		file.Close()

		//woker??
		kva := mapf(fileName, string(content))
		intermediate = append(intermediate, kva...)
	}

	c.server()
	return &c
}
