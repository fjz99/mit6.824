package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//fixme hash之后，同一个key应该合并在一起
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
	workerId := ""
	call("Coordinator.Connect", &Empty{}, &workerId)
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
		} else if task.TaskId == -1 || !b {
			//已经结束,两种情况，master关闭或者还在运行
			Debug("%s stop", workerId)
			os.Exit(0)
		} else if task.Type == 0 {
			//map
			output = processMap(mapf, task, workerId)
		} else {
			//reduce
			output = processReduce(reducef, task, workerId)
		}
		//Debug("%s 任务 %#v 完成", workerId, *task)
		call("Coordinator.FinishWork", &FinishWorkReq{output, workerId}, &Empty{})
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
	}
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

	ofilename := fmt.Sprintf("mr-out-%d.txt", task.TaskId)
	file, err := os.OpenFile(ofilename, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
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
			file.WriteString(fmt.Sprintf("%s %s\n", current.Key, o))
			list = []string{}

			list = append(list, kvs[i].Value)
			current = &kvs[i]
		}
	}
	file.Close()

	Debug("%s reduce处理完成，输出在 %s", workerId, ofilename)
	return &Output{task, 0, []string{ofilename}}
}

func openFile(name string) *os.File {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalf("cannot open %v", name)
	}
	return file
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
