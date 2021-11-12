package shardkv

import (
	"6.824/shardctrler"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type logTopic string

const DebugEnabled = true

const (
	dClient  logTopic = "CLNT"
	dMachine logTopic = "MACH" //状态机
	dServer  logTopic = "SEVE"
	dTrace   logTopic = "TRCE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int
var logInitLock = sync.Mutex{}

const NShards = shardctrler.NShards

// InitLog 提前手动调用
func InitLog() {
	if !DebugEnabled {
		return
	}
	logInitLock.Lock()
	defer logInitLock.Unlock()

	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if !DebugEnabled {
		return
	}
	logInitLock.Lock()
	defer logInitLock.Unlock()

	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
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

func CopyMap(src map[string]string) map[string]string {
	res := map[string]string{}
	for k, v := range src {
		res[k] = v
	}
	return res
}

func CopySessionMap(src map[int64]int) map[int64]int {
	res := map[int64]int{}
	for k, v := range src {
		res[k] = v
	}
	return res
}

func DeleteArrayIndex(src []int, index int) []int {
	return append(src[:index], src[(index+1):]...)
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func CopyShard(src Shard) Shard {
	s := Shard{Id: src.Id}
	s.State = CopyMap(src.State)
	s.Session = CopySessionMap(src.Session)
	return s
}
