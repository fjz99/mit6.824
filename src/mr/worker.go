package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//todo down了的话，master清除文件映射
// map任务在的机器挂了的话，需要重做
//todo 重做map重复问题
//fixme 如果检查时刚好心跳检测到了，但是其实挂了，因为map被清空了，此时根本不知道挂了；但是没有finish，为什么没有死锁？
//fixme 时间区间问题，比如刚上线还没发心跳就检查了，或者刚检查完就挂了；这个没问题关键在于为啥任务没做完就停止了,因为finish了2次reduce6
//todo map完成之后再挂了相关的问题：1.任务队列中的任务的输入需要完全改动，需要手动读出所有任务，然后重新放进去
//2. 维护一个谁做了map的Map，而且在finish时添加
//3.维护每个reduce任务的输入的map也需要先删除输入，在添加新的（数组原地修改就行）
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	mu := &sync.Mutex{}

	workerId := ""
	go sendHearBeat(&workerId, mu)
restart:
	mu.Lock()
	call("Coordinator.Connect", &Empty{}, &workerId)
	mu.Unlock()

	Debug("%s 初始化成功", workerId)
	task := &Task{}
	var output *Output
	for {
		task = &Task{} //!!必须传入空的struct，如果字段不是默认值。可能rpc解析失败
		//Debug("%s call  AskForWork", workerId)
		b := call("Coordinator.AskForWork", workerId, task)
		Debug("%s 获得任务 %#v", workerId, *task)
		if task.TaskId == -999 || task.TaskId == -1000 {
			//暂无任务
			Debug("暂无任务，所以sleep 1秒,%s", workerId, task.InputLocation[0])
			time.Sleep(time.Duration(1) * time.Second)
			continue
		} else if !b || task.TaskId == -1 {
			//已经结束,两种情况，master关闭或者还在运行
			Debug("%s stop", workerId)
			os.Exit(0)
		} else if task.TaskId == 8888 {
			Debug("被认为挂了，重新connect！")
			goto restart
		} else if task.Type == 0 {
			//map
			output = processMap(mapf, task, workerId)
		} else {
			//reduce
			output = processReduce(reducef, task, workerId)
		}
		//Debug("%s 任务 %#v 完成", workerId, *task)
		reply := &Empty{}
		call("Coordinator.FinishWork", &FinishWorkReq{output, workerId}, reply)
		if reply.IsDown {
			Debug("被认为挂了，重新connect！")
			goto restart
		}
	}
}

func processMap(mapf func(string, string) []KeyValue, task *Task, workerId string) *Output {
	Debug("%s 处理Map任务 %#v", workerId, *task)
	filename := task.InputLocation[0]
	file := openFile(filename)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//存储键值对
	//sort.Sort(ByKey(kva)) //?似乎不需要排序,因为已经hash了
	files := make(map[string][]KeyValue)
	output := Output{task, 100, nil}
	for _, kv := range kva {
		mod := ihash(kv.Key) % task.NReduce
		oname := fmt.Sprintf("mr-%d-%d.txt", task.TaskId, mod)
		files[oname] = append(files[oname], kv)
		if b, _ := PathExists(oname); b {
			Debug("已经存在的输出 %s ，删除！", oname)
			os.Remove(oname)
		}
	}
	//检查输出是否已经存在了，如果是，就删除文件，因为可能之前down了，输出不对
	for k, v := range files {
		output.OutputLocation = append(output.OutputLocation, k)
		ofile, _ := os.Create(k)
		enc := json.NewEncoder(ofile)
		for _, kv := range v {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("写json文件失败 %s", err)
			}
		}
		ofile.Close()
	}
	output.Status = 0
	Debug("%s map处理完成，输出在 %s", workerId, output.OutputLocation)
	return &output
}

func processReduce(reducef func(string, []string) string, task *Task, workerId string) *Output {
	Debug("%s 处理Reduce任务 %#v", workerId, *task)
	tfn := fmt.Sprintf("%s-temp-mr-out-%d.txt", workerId, task.TaskId)
	tempFile, err := os.OpenFile(tfn, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	defer os.Remove(tempFile.Name())
	if err != nil {
		Debug("%s 在reduce操作时，打开输出文件失败，原因：", workerId, err)
		Debug("%s 返回 状态2", workerId)
		return &Output{task, 2, nil}
	}

	//读取json
	var kvs []KeyValue
	for _, filename := range task.InputLocation {
		file := openFile(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(ByKey(kvs))

	var list []string
	var current *KeyValue = nil
	kvs = append(kvs, KeyValue{Value: "1", Key: "##"}) //用做最后停止
	for i := 0; i < len(kvs); i++ {
		if current == nil || current.Key == kvs[i].Key {
			list = append(list, kvs[i].Value)
			current = &kvs[i]
		} else {
			//Debug("key %s count %d", current.Key, len(list))
			o := reducef(current.Key, list)
			tempFile.WriteString(fmt.Sprintf("%s %s\n", current.Key, o))
			list = []string{}

			list = append(list, kvs[i].Value)
			current = &kvs[i]
		}
	}
	tempFile.Close()
	//尝试重命名,可能会有问题
	ofilename := fmt.Sprintf("mr-out-%d.txt", task.TaskId)
	if b, _ := PathExists(ofilename); b {
		Debug("%s 输出 %s 已经存在，任务 %#v 重复了！", workerId, ofilename, *task)
		return &Output{task, 2, []string{ofilename}}
	} else {
		os.Rename(tempFile.Name(), ofilename)
		Debug("%s reduce处理完成，输出在 %s", workerId, ofilename)
		return &Output{task, 0, []string{ofilename}}
	}
}

func openFile(name string) *os.File {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalf("cannot open %v", name)
	}
	return file
}

func sendHearBeat(worker *string, mu *sync.Mutex) {
	for {
		time.Sleep(time.Duration(1) * time.Second)
		//指针还未初始化
		mu.Lock()
		if worker == nil || *worker == "" {
			continue
		}
		Debug("%s 发送heartbeat", *worker)
		reply := &Empty{}
		b := call("Coordinator.HeartBeat", *worker, reply)
		mu.Unlock()
		if !b {
			Debug("master下线，heartbeat 线程关闭")
			break
		}
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
