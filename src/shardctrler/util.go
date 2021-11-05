package shardctrler

import (
	"6.824/raft"
	"fmt"
	"hash/crc32"
	"log"
	"math/rand"
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

//等到状态机做完到index,返回对应的状态机运行结果
//外部必须提供锁，否则无法cond
//必须指定超时时间，然后每隔一段时间检查一次，因为可能本来是leader
func (sc *ShardCtrler) waitFor(index int) *StateMachineOutput {
	Debug(dServer, "S%d waitFor index=%d", sc.me, index)

	for sc.lastApplied < index {
		sc.commitIndexCond.Wait() //这里的wait无法释放所有的重入的lock，只会释放一层。。
		//Debug(dServer, "S%d waitFor 被唤醒，此时index=%d，等待的index=%d", sc.me, sc.lastApplied, index)
		id := sc.rf.GetLeaderId()
		if id != sc.me {
			//因为互斥性，此时可能被取代了
			Debug(dServer, "S%d waitFor 发现自己已经被取代，新的leader为S%d", sc.me, id)
			return &StateMachineOutput{ErrWrongLeader, nil}
		}
	}
	output := sc.output[index]
	delete(sc.output, index)
	Debug(dServer, "S%d waitFor 返回index=%d,data=%+v", sc.me, index, *output)
	return output
}

func (sc *ShardCtrler) buildCmd(op *Op, id int, seqId int) Command {
	return Command{*op, id, seqId, raft.GetNow()}
}

func (sc *ShardCtrler) checkDuplicate(CommandIndex int, command Command) bool {
	if s, ok := sc.session[command.ClientId]; ok {
		if s >= command.SequenceId {
			//不执行,因为只有put，所以也不用返回。。
			Debug(dServer, "S%d 检查会话，会话已存在 cmd=%+v,session=%v", command, s)
			sc.output[CommandIndex] = &StateMachineOutput{OK, ""}
			return true
		} else {
			s = command.SequenceId
			return false
		}
	} else {
		//会话不存在
		panic(1)
	}
}

func (sc *ShardCtrler) latestConfig() *Config {
	if len(sc.configs) == 0 {
		panic("1")
	} else {
		return &sc.configs[len(sc.configs)-1]
	}
}

func CopyMap(src map[int][]string) map[int][]string {
	res := map[int][]string{}
	for k, v := range src {
		arr := make([]string, len(v))
		copy(arr, v)
		res[k] = arr
	}
	return res
}

func HashString(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func FindShard(key string) int {
	return int(crc32.ChecksumIEEE([]byte(key)) % NShards) //先取模，避免成为负数
}

// Returns an int >= min, < max
func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

// Generate a random string of A-Z chars with len = l
func randomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(randomInt(65, 90))
	}
	return string(bytes)
}

func ChangeArray2FixedArray(src []int) [NShards]int {
	Assert(len(src) == NShards, "")
	arr := [NShards]int{}
	for i := 0; i < NShards; i++ {
		arr[i] = src[i]
	}
	return arr
}

func DeleteArrayIndex(src []int, index int) []int {
	return append(src[:index], src[(index+1):]...)
}

func DeleteArrayValue(src []int, target int) []int {
	//删除第一个
	i := 0
	for i = 0; i < len(src); i++ {
		if src[i] == target {
			break
		}
	}
	return DeleteArrayIndex(src, i)
}
