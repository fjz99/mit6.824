package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
)
import "strconv"

const debug = false

type Empty struct {
	Worker string
}

func Debug(format string, a ...interface{}) {
	if debug {
		log.Printf(format, a...)
	}
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
