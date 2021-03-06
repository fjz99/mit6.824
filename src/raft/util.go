package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"runtime"
	"sort"
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
var logInitLock = sync.Mutex{}

const EnableLog = false

// InitLog 提前手动调用
func InitLog() {
	if !EnableLog {
		return
	}
	logInitLock.Lock()
	defer logInitLock.Unlock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if !EnableLog {
		return
	}
	logInitLock.Lock()
	defer logInitLock.Unlock()
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
	level := 0
	v := os.Getenv("VERBOSE")
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level //为了测试kvraft
}

// ResetTimer caller heartbeat; appendEntry; election timeout;vote others
func (rf *Raft) ResetTimer() {
	rf.lastAccessTime = GetNow()
	Debug(dTimer, "重置计时器", rf.me)
}

// IndexBig2Small 获得log数组内的索引
func (rf *Raft) IndexBig2Small(index int) int {
	return index - rf.snapshotIndex - 1
}

// IndexSmall2Big 通过log数组内的索引获得具体的commitId这样的综合索引
func (rf *Raft) IndexSmall2Big(index int) int {
	return index + rf.snapshotIndex + 1
}

// TrimLog 根据snapshot的参数截断日志,节点重启后也必须截断！
//todo 重启的时候，如果想恢复的话？？需要额外的信息
func (rf *Raft) TrimLog(smallIndex int) {
	//之前的都不要了，include边界
	var temp []LogEntry
	temp = Copy(temp, rf.log[smallIndex+1:])
	rf.log = temp
	Debug(dSnap, "日志被截断，smallIndex=%d，结果为%+v", rf.me, smallIndex, rf.log)
}

func GetNow() int64 {
	return time.Now().UnixNano() / 1e6
}

func (rf *Raft) getLastLog() LogEntry {
	lens := len(rf.log)
	if lens == 0 {
		if rf.snapshotIndex != -1 {
			return LogEntry{Term: rf.snapshotTerm, Index: rf.snapshotMachineIndex - 1, Command: nil}
		} else {
			return LogEntry{Term: -1, Index: -1, Command: nil}
		}
	} else {
		return rf.log[lens-1]
	}
}

//返回bigIndex
func (rf *Raft) getLastLastLog() (LogEntry, int) {
	lens := len(rf.log)
	if lens <= 1 {
		if rf.snapshotIndex != -1 {
			return LogEntry{Term: rf.snapshotTerm, Index: rf.snapshotMachineIndex - 1, Command: nil}, rf.snapshotIndex
		} else {
			return LogEntry{Term: -1, Index: -1, Command: nil}, -1
		}
	} else {
		return rf.log[lens-2], rf.IndexSmall2Big(lens - 2)
	}
}

//获得某个索引位置的上一个log，位置必须是有效的
func (rf *Raft) getLastLogOf(smallIndex int) LogEntry {
	Assert(smallIndex >= 0 && smallIndex < len(rf.log), "smallIndex")
	if smallIndex == 0 {
		if rf.snapshotIndex != -1 {
			return LogEntry{Term: rf.snapshotTerm, Index: rf.snapshotMachineIndex - 1, Command: nil}
		} else {
			return LogEntry{Term: -1, Index: -1, Command: nil}
		}
	} else {
		return rf.log[smallIndex-1]
	}
}

// ApplyMsg2B index的作用就是打日志。。
//这里不能持有锁！thisEntry复制了一份，所以，没事
func (rf *Raft) ApplyMsg2B(thisEntry *LogEntry, index int) {
	//nil代表是空entry
	if thisEntry.Command != nil {
		Debug(dCommit, "发送测试数据 command=%+v，内部index=%d 修正index=%d", rf.me, thisEntry.Command, index, thisEntry.Index+1)
		rf.fuckerChan <- &ApplyMsg{CommandValid: true, Command: thisEntry.Command, CommandIndex: thisEntry.Index + 1} //index从一开始，所以返回+1
	} else {
		Debug(dCommit, "因为cmd=nil不发送测试数据", rf.me)
	}
}

func (rf *Raft) FollowerUpdateCommitIndex(args *AppendEntriesArgs) {
	//myMatchIndex := rf.matchIndex[rf.me]
	//Debug(dTrace, "进入FollowerUpdateCommitIndex,matchIndex=%d,commitId=%d,leaderCommit=%d", rf.me,
	//	myMatchIndex, rf.commitIndex, LeaderCommit)

	//c := Min(LeaderCommit, myMatchIndex) //follower就不用唤醒cond了
	if rf.commitIndex < rf.snapshotIndex {
		//快照还没有安装
		Debug(dTrace, "快照index=%d，commitIndex=%d，快照还没有安装，拒绝更新commitId", rf.me, rf.snapshotIndex, rf.commitIndex)
		return
	}
	//这个没有也可以。。
	temp := -1
	if args.Log == nil {
		temp = Min(args.LeaderCommit, args.PrevLogIndex) //对应 commitId
	} else {
		temp = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Log))
	}
	c := Max(rf.commitIndex, temp)
	//注意空entry就不用apply了。。
	//注意，在有了快照的时候，如果直接安装快照的话，就无法发送log entry了
	for i := rf.commitIndex + 1; i <= c; i++ {
		if rf.IndexBig2Small(i) >= 0 {
			localLog := rf.log[rf.IndexBig2Small(i)]
			rf.ApplyMsg2B(&localLog, i) //这里不能持有锁！会死锁
		} else {
			Debug(dTrace, "无法发送测试数据，因为比快照早，具体i=%d，snapIndex=%d", rf.me, i, rf.snapshotIndex)
		}
	}

	//Assert(c >= rf.commitIndex, "") //leader的commitId可能比我（follower）小！，见figure8，和bugfix9
	if c > rf.commitIndex {
		rf.commitIndex = c
		Debug(dCommit, "更新commitId为 %d", rf.me, rf.commitIndex)
	}
}

func (rf *Raft) ChangeState(to State) {
	if from := rf.state; from != to {
		if to == LEADER {
			rf.LeaderId = rf.me
		}
		if to == FOLLOWER {
			rf.matchIndex[rf.me] = rf.commitIndex
			Debug(dInfo, "状态转换为follower，所以初始化matchIndex=commitId=%d", rf.me, rf.commitIndex)
		}
		if rf.state == LEADER {
			rf.logAppendCondition.Broadcast() //否则会出现，leader在callback中阻塞，但是leader变成follower的情况，就死锁了
		}
		rf.state = to
		//这个chan太容易死锁了
		rf.stateChanging <- &ChangedState{from, to}
		Debug(dInfo, "状态转换 %d -> %d", rf.me, from, to)
	}
}

func Assert(normal bool, msg interface{}) {
	if !normal {
		panic(msg)
	}
}

func SetArrayValue(arr []int, v int) {
	for i := 0; i < len(arr); i++ {
		arr[i] = v
	}
}

func (rf *Raft) increaseTerm(newTerm int, leaderId int) {
	Assert(rf.term < newTerm, "")
	rf.voteFor = -1 //重置
	rf.term = newTerm
	rf.persistState()
	rf.LeaderId = leaderId
	rf.ChangeState(FOLLOWER)
	Debug(dTerm, "set Term = %d", rf.me, newTerm)
}

// LeaderUpdateCommitIndex 找超过半数的，办法很简单，直接排序，然后取前半数个,找到最大值，一起更新
//注意判断term！
//fixme
func (rf *Raft) LeaderUpdateCommitIndex() {
	temp := make([]int, len(rf.nextIndex))
	copy(temp, rf.matchIndex)
	temp[rf.me] = rf.IndexSmall2Big(len(rf.log) - 1)
	//逆序排序
	sort.Sort(sort.Reverse(sort.IntSlice(temp))) //.......
	Debug(dCommit, "LeaderUpdateCommitIndex matchIndex逆序排序的结果为 %+v ", rf.me, temp)
	maxIndex := rf.IndexBig2Small(temp[rf.n/2])
	Debug(dCommit, "LeaderUpdateCommitIndex 选择的过半最小matchIndex=%d", rf.me, maxIndex)
	if maxIndex > rf.IndexBig2Small(rf.commitIndex) {
		//过半了,检查当前index的任期
		if rf.log[maxIndex].Term == rf.term {

			//用于测试
			//很坑。。因为可能有fig.8的情况，主节点的commitId不一定是满的！,所以主节点更新commitId的时候，需要多次给chan发送数据！
			for i := rf.IndexBig2Small(rf.commitIndex + 1); i <= maxIndex; i++ { //fixme bug
				if i >= 0 {
					localLog := rf.log[i]
					rf.ApplyMsg2B(&localLog, i)
				} else {
					Debug(dTrace, "无法发送测试数据，因为比快照早，具体i=%d，snapIndex=%d", i, rf.snapshotIndex)
				}
			}

			rf.commitIndex = rf.IndexSmall2Big(maxIndex)
			Debug(dCommit, "commitId 修改为 %d，此时matchIndex=%+v,广播这个消息", rf.me, rf.commitIndex, rf.matchIndex)
		} else {
			Debug(dCommit, "最大的过半matchIndex为%d，但是term为%d，而leader term为%d，所以放弃更新commitId",
				rf.me, maxIndex, rf.log[maxIndex].Term, rf.term)
		}
	}
}

//生成新任务
func (rf *Raft) generateNewTask(peerIndex int) *Task {
	nextIndex := rf.IndexBig2Small(rf.nextIndex[peerIndex])

	if rf.state != LEADER {
		return nil
	}

	Assert(nextIndex <= len(rf.log),
		fmt.Sprintf("S%d peerIndex=%d nextIndex=%d,rf.log=%+v,snap=%d,next origin=%d", rf.me, peerIndex, nextIndex, rf.log, rf.snapshotIndex, rf.nextIndex[peerIndex]))

	if nextIndex == len(rf.log) {
		Debug(dTrace, "对于S%d index=%d，我的lenLog=%d 没有任务可以生成", rf.me, peerIndex, nextIndex, len(rf.log))
		return nil
	}

	if rf.nextIndex[peerIndex] <= rf.snapshotIndex {
		Debug(dCommit, "日志log=%+v不足了(snapshotIndex=%d,nextIndex=%d)！选择发送快照！", rf.me, rf.log,
			rf.snapshotIndex, rf.nextIndex[peerIndex])
		args := &InstallSnapshotArgs{rf.term, rf.me, rf.snapshot, true, 0,
			rf.snapshotIndex, rf.snapshotMachineIndex, rf.snapshotTerm}
		return &Task{snapshotRpcFailureCallback, snapshotRpcSuccessCallback,
			args, &InstallSnapshotReply{}, "Raft.InstallSnapshot"}
	}

	lastLog := rf.getLastLogOf(nextIndex)
	args := &AppendEntriesArgs{}

	var arr []LogEntry
	if len(rf.log)-nextIndex >= BatchSize {
		arr = Copy(arr, rf.log[nextIndex:nextIndex+BatchSize])
	} else {
		arr = Copy(arr, rf.log[nextIndex:])
	}
	args = &AppendEntriesArgs{rf.term, arr, rf.IndexSmall2Big(nextIndex - 1), //因为nextIndex从0开始，所以可以保证-1
		lastLog.Term, rf.commitIndex, rf.me}
	Assert(len(arr) > 0, "")

	//if clearChannel {
	//	rf.cleanupSenderChannelFor(peerIndex)
	//}

	//rf.senderChannel[peerIndex] <- &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
	//	args, &AppendEntriesReply{}, "Raft.AppendEntries"}

	Debug(dTrace, "生成 S%d 新的任务 %+v", rf.me, peerIndex, *args)
	return &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
		args, &AppendEntriesReply{}, "Raft.AppendEntries"}
}

//根据match进行回退,
func (rf *Raft) backward(peerIndex int, reply *AppendEntriesReply) {
	//回退之前的nextIndex应该大于matchIndex+1，因为matchIndex一定匹配。。因为开始时0，-1所以大于好一点
	//Assert(rf.nextIndex[peerIndex] > rf.matchIndex[peerIndex], "")

	//查找是否存在
	// fixme 是否需要考虑到snapshot?
	ci := 0
	for ; ci < len(rf.log); ci++ {
		if rf.log[ci].Term == reply.ConflictTerm {
			break
		}
	}
	if ci == len(rf.log) {
		rf.nextIndex[peerIndex] = reply.ConflictIndex
		Debug(dCommit, "没有找到follower的ConflictTerm=%d,所以设置nextIndex=ConflictIndex=%d",
			rf.me, reply.ConflictTerm, reply.ConflictIndex)
	} else {
		//找最后一个位置的下个位置
		for ; ci < len(rf.log) && rf.log[ci].Term == reply.Term; ci++ {

		}
		rf.nextIndex[peerIndex] = rf.IndexSmall2Big(ci)
		Debug(dCommit, "找到了follower的ConflictTerm=%d,所以设置nextIndex=这个term的下一个term的第一个index=%d",
			rf.me, reply.ConflictTerm, rf.nextIndex[peerIndex])
	}
	//不过这种情况似乎不会发生。。
	if rf.nextIndex[peerIndex] <= rf.matchIndex[peerIndex] {
		Debug(dCommit, "WARN：回退nextIndex %d 的时候，竟然小于等于MatchIndex %d 了！修正为macthIndex+1!",
			rf.me, rf.nextIndex[peerIndex], rf.matchIndex[peerIndex])
		rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
	}
}

func (rf *Raft) cleanupSenderChannelFor(peerIndex int) {
	for len(rf.senderChannel[peerIndex]) > 0 {
		_ = <-rf.senderChannel[peerIndex]
	}
	Debug(dCommit, "生成 S%d 新的任务前，先清除队列中的任务", rf.me, peerIndex)
}

func (rf *Raft) cleanupSenderChannel() {
	Debug(dInfo, " state = %d 清空发送队列，因为状态已经是follower了", rf.me, rf.state)
	Assert(rf.state != LEADER, "") //??
	for _, c := range rf.senderChannel {
		for len(c) > 0 {
			_ = <-c
		}
	}
}

func (rf *Raft) TimedWait(timeout time.Duration, timeoutCallback func(rf *Raft),
	waitedFunction func() interface{}, timeWaitSuccessCallback func(rf *Raft, result interface{})) {
	waitedChan := make(chan interface{}, 1) //防止协程无法停止
	wrapper := func() {
		waitedChan <- waitedFunction()
	}
	go wrapper()
	Debug(dTrace, "TimedWait:当前协程数量为%d", rf.me, runtime.NumGoroutine())

	select {
	case result := <-waitedChan:
		{
			timeWaitSuccessCallback(rf, result)
			break
		}
	case _ = <-time.After(timeout):
		{
			timeoutCallback(rf)
			break
		}
	}
}

func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// Copy 完全覆盖版本的copy
func Copy(dst, src []LogEntry) []LogEntry {
	Assert(len(dst) == 0, "")
	for _, i := range src {
		dst = append(dst, i)
	}
	return dst
}

// DeepCopy 结构体成员变量必须是大写！否则无法拷贝成功！
func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

//输入状态机的index，从1开始，找到smallIndex，如果没找到的话，
//如果index太小了，小于所有的log就为-1，否则如果太大了，大于所有的log，就为-2
func (rf *Raft) findSmallIndex(index int) int {
	smallIndex := 0
	for ; smallIndex < len(rf.log); smallIndex++ {
		if rf.log[smallIndex].Index == index-1 {
			break
		}
	}
	return smallIndex
}

// GetLeaderId 下面是给kv使用的
func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.LeaderId
}

// SetCommitIndex 一种不好的设计，破坏了封装性
func (rf *Raft) SetCommitIndex(mi int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//这个index是状态机从1开始的index。。。
	ci := -1
	if mi == rf.snapshotMachineIndex {
		ci = rf.snapshotIndex
	} else {
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Index+1 == mi { //+1
				ci = rf.IndexSmall2Big(i)
				break
			}
		}
	}
	rf.commitIndex = Max(rf.commitIndex, ci)
	return rf.commitIndex
}
