package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"sync"
)
import "strconv"

const debug = false

var p = false
var mu = sync.Mutex{}

type Empty struct {
	Worker string
	IsDown bool
}

func Debug(format string, a ...interface{}) {
	mu.Lock()
	if !p {
		p = true
		inits()
	}
	mu.Unlock()
	if debug {
		fmt.Printf(format, a)
		fmt.Println()
	}
	log.Printf(format, a...)
}

func inits() {
	file := "master.log"
	index := 1
	if b, _ := PathExists(file); b {
		file = "worker-0.log"
	}
	//注意，for循环的第一个是初始值
	for b, _ := PathExists(file); b; b, _ = PathExists(file) {
		file = fmt.Sprintf("worker-%d.log", index)
		index++
	}
	fmt.Printf("日志文件: %s\n", file)
	logFile, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0766)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile) // 将文件设置为log输出的文件
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type Task struct {
	TaskId        int      //TaskId
	Type          uint8    //0 map ,1 reduce
	InputLocation []string //location
	NReduce       int      //为了hash取模
}

type Output struct {
	Task           *Task
	Status         uint8    //0 ok,1 failed,2 rename failed
	OutputLocation []string //map操作的输出文件位置，数组长度为nReduce；reduce操作的输出为mr-out-x
}

type FinishWorkReq struct {
	Output   *Output
	WorkerId string
}

func PathExists(path string) (bool, error) {

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
