package raft

import (
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
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var lock = sync.Mutex{}

//提前手动调用
func InitLog() {
	lock.Lock()
	defer lock.Unlock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	lock.Lock()
	defer lock.Unlock()
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		//format = prefix + "S%d <%s> " + format
		format = prefix + "S%d " + format
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

//caller heartbeat; appendEntry; election timeout;vote others
func (rf *Raft) ResetTimer() {
	rf.lastAccessTime = GetNow()
	Debug(dTimer, "重置计时器", rf.me)
}

func GetNow() int64 {
	return time.Now().UnixNano() / 1e6
}

func (rf *Raft) getLastLog() LogEntry {
	lens := len(rf.log)
	if lens == 0 {
		return LogEntry{Term: -1, Index: -1, Command: nil}
	} else {
		return rf.log[lens-1]
	}
}

func (rf *Raft) ChangeState(to State) {
	if from := rf.state; from != to {
		rf.state = to
		rf.stateChanging <- &ChangedState{from, to}
		Debug(dInfo, "状态转换 %d -> %d", rf.me, from, to)
	}
}

func Assert(normal bool, msg string) {
	if !normal {
		panic(msg)
	}
}

func SetArrayValue(arr []int, v int) {
	for i := 0; i < len(arr); i++ {
		arr[i] = v
	}
}

func (rf *Raft) increaseTerm(newTerm int) {
	Assert(rf.term < newTerm, "")
	rf.voteFor = -1 //重置
	rf.leaderId = -1
	rf.term = newTerm
	rf.ChangeState(FOLLOWER)
	Debug(dTerm, "set Term = %d", rf.me, newTerm)
}

func (rf *Raft) TimedWait(timeout time.Duration, timeoutCallback func(rf *Raft),
	waitedFunction func() interface{}, timeWaitSuccessCallback func(rf *Raft, result interface{})) {
	waitedChan := make(chan interface{})
	wrapper := func() {
		waitedChan <- waitedFunction()
	}
	go wrapper()

	select {
	case err := <-waitedChan:
		{
			timeWaitSuccessCallback(rf, err)
			break
		}
	case _ = <-time.After(timeout):
		{
			timeoutCallback(rf)
			break
		}
	}
}
