package mr

import "C"
import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapJobs               chan *Task       //模拟队列
	reduceJobs            chan *Task       //模拟队列
	mapInputs             map[int]string   //map的input是一个string，而reduce是一个数组
	reduceInputs          map[int][]string //map的input是一个string，而reduce是一个数组
	stage                 uint8            //因为map完才可以reduce
	workers               map[string]*Task
	heartbeatMap          map[string]bool
	mu                    sync.Mutex
	M, R                  int
	doneMaps, doneReduces int
	workerId              uint64               //自增处理，如果复用的话，会出现认为挂了但是没挂，导致id重复
	whoDoesMapJob         map[string][]*Output //一个节点可以做过多个map任务
}

func (c *Coordinator) Connect(e *Empty, id *string) error {
	c.mu.Lock()
	*id = fmt.Sprintf("worker-%d", c.workerId)
	c.workers[*id] = nil
	c.workerId++
	c.heartbeatMap[*id] = true //保证通过第一次测试
	Debug("%s connected", *id)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) AskForWork(worker string, task *Task) error {
	//Debug("%s call AskForWork", worker)
	var job *Task
	c.mu.Lock()         //保证可见性,否则可能err
	defer c.mu.Unlock() //最简单的lock方式，而且稳健
	//检查是否worker被认为挂了
	if _, ok := c.workers[worker]; !ok {
		Debug("已经挂了的节点！ %s ", worker)
		Debug("%s 节点被错误的认为下线,重新添加回去", worker)
		c.workers[worker] = nil
		*task = Task{TaskId: 8888, Type: 100}
		return nil
	}
	if c.doneMaps+c.doneReduces == c.M+c.R {
		//任务已经结束了
		*task = Task{-1, 100, nil, c.R}
		Debug("%s 发送AskForWork请求，但是任务已经结束", worker)
		return nil
	}
	//DEBUG
	var tt []string
	for k, v := range c.workers {
		if v != nil {
			tt = append(tt, k)
		}
	}
	if c.stage == 0 {
		if len(c.mapJobs) == 0 {
			Debug("%s 在阶段 %d 获取任务失败，因为队列map为空\n对应的正在进行的任务为 %d 个，有 %#v,doneMaps=%d", worker, c.stage, len(tt), tt, c.doneMaps)
			//task = nil这样只会修改本地变量，就会导致rpc远程的响应struct不变
			*task = Task{-999, 100, []string{"获取任务失败，因为队列map为空"}, c.R}
			return nil
		}
		job = <-c.mapJobs
	} else {
		if len(c.reduceJobs) == 0 {
			Debug("%s 在阶段 %d 获取任务失败，因为队列reduce为空\n对应的正在进行的任务为 %d 个，有 %#v,doneReduces=%d", worker, c.stage, len(tt), tt, c.doneReduces)
			*task = Task{-1000, 100, []string{"获取任务失败，因为队列reduce为空"}, c.R}
			return nil
		}
		//map可能挂了，所以就没有足够的输入数据了,就直接重新进入第1阶段
		job = <-c.reduceJobs
	}
	c.workers[worker] = job //加锁，否则可能破坏map的内部结构
	*task = *job
	Debug("%s ask for job %#v", worker, *task)
	return nil
}

func (c *Coordinator) FinishWork(req *FinishWorkReq, empty *Empty) error {
	output := req.Output
	worker := req.WorkerId
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.workers[worker]; !ok {
		Debug("%s 节点被错误的认为下线,重新添加回去", worker)
		c.workers[worker] = nil
		*empty = Empty{IsDown: true}
		if output.Task.Type == 0 {
			Debug("拒绝一个认为是下线的节点的map输出 %#v", *output)
			return nil //不接收输出
		} else {
			Debug("接收一个认为是下线的节点的reduce输出 %#v", *output)
		}
	} else {
		c.workers[worker] = nil
	}
	if output.Task.Type == 0 {
		//map
		c.doneMaps++
		if _, ok := c.whoDoesMapJob[worker]; !ok {
			c.whoDoesMapJob[worker] = []*Output{}
		}
		c.whoDoesMapJob[worker] = append(c.whoDoesMapJob[worker], output)

		//处理输出
		Debug("%s map-%d 任务完成,响应为 %#v", worker, output.Task.TaskId, *output)
		sort.Strings(output.OutputLocation)
		// 必须排序，因为输出根据hash，是无序的！，所以要根据mod分类，但是mod不一定是满的，比如mod5，可能只有2个值，0和3
		//为了简便直接改这里了，应该改output结构定义为map的
		for _, location := range output.OutputLocation {
			a := strings.LastIndex(location, "-")
			b := strings.LastIndex(location, ".")
			sub := location[a+1 : b]
			id, _ := strconv.Atoi(sub)
			c.reduceInputs[id] = append(c.reduceInputs[id], location)
		}

		if c.doneMaps == c.M {
			c.stage = 1
			Debug("Coordinator： map阶段结束，进入reduce阶段")
			Debug("对应的reduceInputs为 %#v", c.reduceInputs)
			//创建task
			//map是哈希的，默认是无序的,其实不排序也行
			//重新重新进入map阶段的情况下，任务队列中的任务的输入需要完全改动，需要手动读出所有任务，然后重新放进去
			if len(c.reduceJobs) != 0 {
				for i := 0; i < len(c.reduceJobs); i++ {
					_ = <-c.reduceJobs
				}
			}
			Debug("WARN：丢弃所有reduceJobs！")
			for i := 0; i < len(c.reduceInputs); i++ {
				//fixme 直接模拟输入
				c.reduceInputs[i] = []string{}
				for j := 0; j < c.M; j++ {
					name := fmt.Sprintf("mr-%d-%d.txt", j, i)
					c.reduceInputs[i] = append(c.reduceInputs[i], name)
				}

				Debug("新增task %#v", Task{i, 1, c.reduceInputs[i], c.R})
				c.reduceJobs <- &Task{i, 1, c.reduceInputs[i], c.R}
			}
		}
	} else {
		//reduce
		if output.Task.Type == 0 {
			Debug("阶段1收到了阶段0的success\n可能一个节点认为挂了，但是没挂，导致又收到了一个success")
			return nil
		}
		if output.Status == 0 {
			Debug("%s reduce-%d 任务完成,响应为 %#v", worker, output.Task.TaskId, *output)
			c.doneReduces++
		} else if output.Status == 2 {
			Debug("%s reduce-%d 重命名失败", worker, output.Task.TaskId)
		}
	}
	return nil
}

func (c *Coordinator) HeartBeat(worker string, empty *Empty) error {
	c.mu.Lock()
	defer c.mu.Unlock() //简单处理
	if _, ok := c.workers[worker]; !ok {
		Debug("%s 节点被错误的认为下线,重新添加回去", worker)
		c.workers[worker] = nil
	}
	c.heartbeatMap[worker] = true
	return nil
}

func checkHeartBeat(c *Coordinator) {
	Debug("心跳检测线程启动")
	for {
		time.Sleep(time.Duration(5) * time.Second)
		Debug("checkHeartBeat: 开始检查")
		c.mu.Lock()
		if c.R+c.M == c.doneReduces+c.doneMaps {
			Debug("任务完成，心跳检测线程停止")
			break
		}
		var fall []string
		for k, v := range c.workers {
			Debug("check %s %#v", k, v)
			tv, ok := c.heartbeatMap[k]
			if !ok || !tv {
				Debug("checkHeartBeat: 探测到 %s 已经挂了,从workers中移除", k)
				delete(c.workers, k)
				fall = append(fall, k)

				//处理任务重做
				//检查是否曾经做过map
				if v, ok := c.whoDoesMapJob[k]; ok {
					Debug("挂了的节点%s曾经做过map任务%#v", k, v)
					c.stage = 0
					c.doneMaps -= len(v)
					for _, t := range v {
						Debug("重做 map任务 %#v", *t)
						c.mapJobs <- t.Task
						//todo 删除reduce inputs,但是很麻烦，所以就直接模拟输出了
					}
					delete(c.whoDoesMapJob, k)
				}
				//worker可能没有任务，比如刚上线就掉线了
				if v == nil {
					continue
				} else if v.Type == 0 {
					//map
					if c.stage == 1 {
						Debug("在reduce阶段检测到map阶段的任务 %s：%#v", k, *v)
						//reduce阶段可能，在workers中检测到map阶段挂掉的任务（因为心跳检测10s一次），而此时就不要加入队列了
						continue
					}
					//Debug("checkHeartBeat: map %#v 重做，重新转换为状态0", v)
					Debug("checkHeartBeat: map %#v 重做", *v)
					//c.stage = 0 因为
					//c.doneMaps-- //曾经做过map的节点挂了的话，才需要--
					c.mapJobs <- v
					//重做map
				} else if v.Type == 1 {
					//reduce
					Debug("checkHeartBeat: reduce %#v 重做", *v)
					c.reduceJobs <- v
					//因为finishwork的不用重做，所以不用--
				} else {
					Debug("checkHeartBeat: ERROR: %s 未知的任务 %#v", k, *v)
				}
			}
		}
		//清空map
		c.heartbeatMap = make(map[string]bool)
		Debug("checkHeartBeat: 检查结束，挂了的有%#v", fall)
		c.mu.Unlock()
	}
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

// Done
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

// MakeCoordinator
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
	c.heartbeatMap = make(map[string]bool)
	c.whoDoesMapJob = make(map[string][]*Output)
	c.M = len(files)
	c.R = nReduce
	for i, file := range files {
		c.mapInputs[i] = file
		c.mapJobs <- &Task{i, 0, []string{file}, c.R}
	}
	//初始化，避免某个mod的key不存在
	for i := 0; i < nReduce; i++ {
		c.reduceInputs[i] = []string{}
	}
	Debug("master启动完成")
	go checkHeartBeat(&c)
	c.server()
	return &c
}
