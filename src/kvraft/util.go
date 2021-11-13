package kvraft

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
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
func (kv *KVServer) waitFor(index int) *StateMachineOutput {
	Debug(dServer, "S%d waitFor index=%d", kv.me, index)

	for kv.lastApplied < index {
		kv.commitIndexCond.Wait() //这里的wait无法释放所有的重入的lock，只会释放一层。。
		//Debug(dServer, "S%d waitFor 被唤醒，此时index=%d，等待的index=%d", kv.me, kv.lastApplied, index)
		id := kv.rf.GetLeaderId()
		if id != kv.me {
			//因为互斥性，此时可能被取代了
			Debug(dServer, "S%d waitFor 发现自己已经被取代，新的leader为S%d", kv.me, id)
			return &StateMachineOutput{ErrWrongLeader, nil}
		}
	}
	output := kv.output[index]
	delete(kv.output, index)
	Debug(dServer, "S%d waitFor 返回index=%d,data=%+v", kv.me, index, *output)
	return output
}

func (kv *KVServer) buildCmd(op *Op, id int, seqId int) Command {
	return Command{*op, id, seqId, raft.GetNow()}
}

//被状态机调用，用来判断是否已经执行
//此方法还需维护会话
//返回值是是否重复
func (kv *KVServer) checkDuplicate(CommandIndex int, command Command) bool {
	if s, ok := kv.session[command.ClientId]; ok {
		if s.LastSequenceId >= command.SequenceId {
			//不执行,因为只有put，所以也不用返回。。
			Debug(dServer, "S%d 检查会话，会话已存在 cmd=%+v,session=%+v", command, *s)
			kv.output[CommandIndex] = &StateMachineOutput{OK, ""}
			return true
		} else {
			//todo 这里必须保证单增，即线性一致性

			s.LastSequenceId = command.SequenceId
			s.LastOp = command.Op
			return false
		}
	} else {
		//会话不存在
		panic(1)
	}
}

func (kv *KVServer) constructSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(kv.stateMachine)
	encoder.Encode(kv.session)
	encoder.Encode(kv.sessionSeed)

	Debug(dServer, "S%d 创建快照完成，state=%+v,sessionMap=%+v,sessionSeed=%d",
		kv.me, kv.stateMachine, kv.session, kv.sessionSeed)
	return buf.Bytes()
}

func (kv *KVServer) readSnapshotPersist(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var sm map[string]string
	var session map[int]*Session
	var sseed int

	if d.Decode(&sm) != nil ||
		d.Decode(&session) != nil ||
		d.Decode(&sseed) != nil {
		panic("decode err")
	} else {
		kv.stateMachine = sm
		kv.session = session
		kv.sessionSeed = sseed
	}
	Debug(dServer, "S%d 读取snapshot持久化数据成功 state=%+v,sessionMap=%+v,sessionSeed=%d",
		kv.me, kv.stateMachine, kv.session, kv.sessionSeed)
}
