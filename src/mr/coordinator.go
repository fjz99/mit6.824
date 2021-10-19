package mr

import "C"
import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	jobs      chan *Task //模拟队列
	stage     uint8
	workers   map[string]*Task
	mu        sync.Mutex
	inputs    map[string]string
	M, R      int
	doneWorks int
}

// Your code here -- RPC handlers for the worker to call.
func AskForWork(worker string, task *Task) {

}

func FinishWork(worker string, output *Output) {

}

func HeartBeat(worker string) {

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// Your code here.
	c.mu.Lock()
	if c.doneWorks == c.M+c.R {
		return true
	}
	defer c.mu.Unlock()
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.jobs = make(chan *Task)
	c.inputs = make(map[string]string)
	c.workers = make(map[string]*Task)
	c.M = len(files)
	c.R = nReduce
	for i, file := range files {
		id := "mr-" + strconv.Itoa(i)
		c.inputs[id] = file
		c.jobs <- &Task{id, 0, file}
	}
	for i := 0; i < nReduce; i++ {
		c.workers["worker-"+strconv.Itoa(i)] = &Task{}
	}
	c.server()
	return &c
}
