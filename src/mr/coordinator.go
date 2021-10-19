package mr

import "C"
import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//fixme reduce的编号不对
type Coordinator struct {
	mapJobs               chan *Task       //模拟队列
	reduceJobs            chan *Task       //模拟队列
	mapInputs             map[int]string   //map的input是一个string，而reduce是一个数组
	reduceInputs          map[int][]string //map的input是一个string，而reduce是一个数组
	stage                 uint8            //因为map完才可以reduce
	workers               map[string]*Task
	mu                    sync.Mutex
	M, R                  int
	doneMaps, doneReduces int
	workerId              int
}

func (c *Coordinator) Connect(e *Empty, id *string) error {
	c.mu.Lock()
	*id = fmt.Sprintf("worker-%d", c.workerId)
	c.workerId++
	Debug("%s connected", *id)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AskForWork(worker string, task *Task) error {
	//Debug("%s call AskForWork", worker)
	var job *Task
	c.mu.Lock()         //保证可见性,否则可能err
	defer c.mu.Unlock() //最简单的lock方式，而且稳健
	if c.doneMaps+c.doneReduces == c.M+c.R {
		//任务已经结束了
		*task = Task{-1, 100, nil, c.R}
		Debug("%s 发送AskForWork请求，但是任务已经结束", worker)
		return nil
	}
	if c.stage == 0 {
		if len(c.mapJobs) == 0 {
			Debug("%s 在阶段 %d 获取任务失败，因为队列map为空", worker, c.stage)
			//task = nil这样只会修改本地变量，就会导致rpc远程的响应struct不变
			*task = Task{-999, 100, []string{"获取任务失败，因为队列map为空"}, c.R}
			return nil
		}
		job = <-c.mapJobs
	} else {
		if len(c.reduceJobs) == 0 {
			Debug("%s 在阶段 %d 获取任务失败，因为队列reduce为空", worker, c.stage)
			*task = Task{-1000, 100, []string{"获取任务失败，因为队列reduce为空"}, c.R}
			return nil
		}
		//map可能挂了，所以就没有足够的输入数据了,就直接重新进入第1阶段
		job = <-c.reduceJobs
	}
	c.workers[worker] = job //加锁，否则可能破坏map的内部结构
	*task = *job
	if job.TaskId == 0 {
		fmt.Printf("%d\n", task.TaskId)
		task.TaskId = 0
	}
	Debug("%s ask for job %#v", worker, *task)
	return nil
}

func (c *Coordinator) FinishWork(req *FinishWorkReq, empty *Empty) error {
	output := req.Output
	worker := req.WorkerId
	if output.Task.Type == 0 {
		//map
		c.mu.Lock()

		c.doneMaps++
		c.workers[worker] = nil

		//处理输出
		Debug("%s map-%d 任务完成,响应为 %#v", worker, output.Task.TaskId, output)
		sort.Strings(output.OutputLocation) // 必须排序，因为输出根据hash，是无序的！，所以要根据mod分类
		for id, location := range output.OutputLocation {
			c.reduceInputs[id] = append(c.reduceInputs[id], location)
		}

		if c.doneMaps == c.M {
			c.stage = 1
			Debug("Coordinator： map阶段结束，进入reduce阶段")
			Debug("对应的reduceInputs为 %#v", c.reduceInputs)
			//创建task
			//map是哈希的，默认是无序的,需要排序
			for i := 0; i < len(c.reduceInputs); i++ {
				Debug("新增task %#v", Task{i, 1, c.reduceInputs[i], c.R})
				c.reduceJobs <- &Task{i, 1, c.reduceInputs[i], c.R}
			}
		}
		c.mu.Unlock()
	} else {
		//reduce
		c.mu.Lock()
		c.workers[worker] = nil
		c.mu.Unlock()

		//因为任务可能重复分发，所以这个直接++有风险
		if output.Status == 0 {
			Debug("%s reduce-%d 任务完成,响应为 %#v", worker, output.Task.TaskId, output)
			c.doneReduces++
		} else if output.Status == 2 {
			Debug("%s reduce-%d 任务已经完成！", worker, output.Task.TaskId)
		}

	}
	return nil
}

//挂了之后需要清除worker map的id
//map挂了需要转为阶段0
//reduce挂了可以重新分配任务
func (c *Coordinator) HeartBeat(worker string, empty *Empty) error {
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
//自动根据done来停止
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock() //避免死锁
	if c.doneMaps+c.doneReduces == c.M+c.R {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapJobs = make(chan *Task, 100) //队列容量
	c.reduceJobs = make(chan *Task, 100)
	c.mapInputs = make(map[int]string)      //!
	c.reduceInputs = make(map[int][]string) //!
	c.workers = make(map[string]*Task)
	c.M = len(files)
	c.R = nReduce
	for i, file := range files {
		c.mapInputs[i] = file
		c.mapJobs <- &Task{i + 100, 0, []string{file}, c.R}
	}
	for i := 0; i < nReduce; i++ {
		c.workers["worker-"+strconv.Itoa(i)] = nil
	}
	Debug("master启动完成")
	c.server()
	return &c
}
