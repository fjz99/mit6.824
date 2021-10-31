package kvraft

import (
	"6.824/raft"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dMachine logTopic = "MACH" //状态机
	dCommit  logTopic = "CMIT"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dServer  logTopic = "SEVE"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dSnap    logTopic = "SNAP"
	dTest    logTopic = "TEST"
	dTrace   logTopic = "TRCE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var logInitLock = sync.Mutex{}

// InitLog 提前手动调用
func InitLog() {
	logInitLock.Lock()
	defer logInitLock.Unlock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	logInitLock.Lock()
	defer logInitLock.Unlock()
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		//format = prefix + "S%d <%s> " + format
		format = prefix + format
		log.Printf(format, a...)
	}
}

//从环境变量获得日志级别
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func Assert(normal bool, msg interface{}) {
	if !normal {
		panic(msg)
	}
}

//todo
//等到状态机做完到index,返回对应的状态机运行结果
func (kv *KVServer) waitFor(index int) *StateMachineOutput {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for kv.lastApplied < index {
		kv.commitIndexCond.Wait()
	}
	output := kv.output[index]
	delete(kv.output, index)
	Debug(dServer, "waitFor 返回 %+v", *output)
	return output
}

func (kv *KVServer) buildCmd(op *Op, id int, seqId int) *Command {
	return &Command{*op, id, seqId, raft.GetNow()}
}

//被状态机调用，用来判断是否已经执行
//此方法还需维护会话
//返回值是是否重复
func (kv *KVServer) checkDuplicate(CommandIndex int, command Command) bool {
	if s, ok := kv.session[command.ClientId]; ok {
		if s.lastSequenceId >= command.SequenceId {
			//不执行,因为只有put，所以也不用返回。。
			Debug(dServer, "S%d 检查会话，会话已存在 cmd=%+v,session=%+v", command, *s)
			kv.output[CommandIndex] = &StateMachineOutput{OK, ""}
			return true
		} else {
			//todo 这里必须保证单增，即线性一致性

			s.lastSequenceId = command.SequenceId
			s.lastOp = command.Op
			return false
		}
	} else {
		//会话不存在
		panic(1)
	}
}
