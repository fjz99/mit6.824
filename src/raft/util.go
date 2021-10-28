package raft

import (
	"fmt"
	"log"
	"os"
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

//提前手动调用
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

func (rf *Raft) getLastLastLog() (LogEntry, int) {
	lens := len(rf.log)
	if lens <= 1 {
		return LogEntry{Term: -1, Index: -1, Command: nil}, -1
	} else {
		return rf.log[lens-2], lens - 2
	}
}

//获得某个索引位置的上一个log，位置必须是有效的
func (rf *Raft) getLastLogOf(index int) LogEntry {
	Assert(index >= 0 && index < len(rf.log), "")
	if index == 0 {
		return LogEntry{Term: -1, Index: -1, Command: nil}
	} else {
		return rf.log[index-1]
	}
}

//index的作用就是打日志。。
func (rf *Raft) ApplyMsg2B(thisEntry *LogEntry, index int) {
	//nil代表是空entry
	if thisEntry.Command != nil {
		Debug(dCommit, "发送测试数据 command=%#v，内部index=%d 修正index=%d", rf.me, thisEntry.Command, index, thisEntry.Index+1)
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: thisEntry.Command, CommandIndex: thisEntry.Index + 1} //index从一开始，所以返回+1
	} else {
		Debug(dCommit, "因为cmd=nil不发送测试数据", rf.me)
	}
}

func (rf *Raft) FollowerUpdateCommitIndex(LeaderCommit int) {
	myMatchIndex := rf.matchIndex[rf.me]
	Debug(dTrace, "进入FollowerUpdateCommitIndex,matchIndex=%d,commitId=%d,leaderCommit=%d", rf.me,
		myMatchIndex, rf.commitIndex, LeaderCommit)
	c := Min(LeaderCommit, myMatchIndex) //follower就不用唤醒cond了
	//注意空entry就不用apply了。。
	for i := rf.commitIndex + 1; i <= c; i++ {
		rf.ApplyMsg2B(&rf.log[i], i)
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
			rf.leaderId = rf.me
		}
		if to == FOLLOWER {
			rf.matchIndex[rf.me] = rf.commitIndex
			Debug(dInfo, "状态转换为follower，所以初始化matchIndex=commitId=%d", rf.me, rf.commitIndex)
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
	rf.leaderId = leaderId
	rf.term = newTerm
	rf.ChangeState(FOLLOWER)
	Debug(dTerm, "set Term = %d", rf.me, newTerm)
}

//找超过半数的，办法很简单，直接排序，然后取前半数个,找到最大值，一起更新
//注意判断term！ todo
func (rf *Raft) LeaderUpdateCommitIndex() {
	temp := make([]int, len(rf.nextIndex))
	copy(temp, rf.matchIndex)
	temp[rf.me] = len(rf.log) - 1
	//逆序排序
	sort.Sort(sort.Reverse(sort.IntSlice(temp))) //.......
	Debug(dCommit, "LeaderUpdateCommitIndex matchIndex逆序排序的结果为 %#v ", rf.me, temp)
	maxIndex := temp[rf.n/2]
	Debug(dCommit, "LeaderUpdateCommitIndex 选择的过半最小matchIndex=%d", rf.me, maxIndex)
	if maxIndex > rf.commitIndex {
		//过半了,检查当前index的任期
		if rf.log[maxIndex].Term == rf.term {

			//用于测试
			//很坑。。因为可能有fig.8的情况，主节点的commitId不一定是满的！,所以主节点更新commitId的时候，需要多次给chan发送数据！
			for i := rf.commitIndex + 1; i <= maxIndex; i++ {
				rf.ApplyMsg2B(&rf.log[i], i)
			}

			rf.commitIndex = maxIndex
			rf.CommitIndexCondition.Broadcast()
			Debug(dCommit, "commitId 修改为 %d，此时matchIndex=%#v,广播这个消息", rf.me, rf.commitIndex, rf.matchIndex)
		} else {
			Debug(dCommit, "最大的过半matchIndex为%d，但是term为%d，而leader term为%d，所以放弃更新commitId",
				rf.me, maxIndex, rf.log[maxIndex].Term, rf.term)
		}
	}
}

//生成新任务,策略是根据lastSuccess的情况
//如果上次成功了的话，就可以多弄一些，因为已经清楚冲突了,batch需要合适，太大了的话，丢包损失大
//如果上次失败了的话，就以回退为主，log取1个
//todo unit test
func (rf *Raft) generateNewTask(peerIndex int, lastSuccess bool, clearChannel bool) {
	nextIndex := rf.nextIndex[peerIndex]
	Assert(nextIndex <= len(rf.log), "")

	if nextIndex == len(rf.log) {
		Debug(dCommit, "对于S%d index=%d，我的lenLog=%d 没有任务可以生成", rf.me, peerIndex, nextIndex, len(rf.log))
		return
	}

	n := 10 //10个log一个batch
	lastLog := rf.getLastLogOf(nextIndex)
	args := &AppendEntriesArgs{}

	if lastSuccess {
		//归零

		var arr []LogEntry
		if len(rf.log)-nextIndex >= n {
			arr = Copy(arr, rf.log[nextIndex:nextIndex+n])
		} else {
			arr = Copy(arr, rf.log[nextIndex:])
		}
		args = &AppendEntriesArgs{rf.term, arr, nextIndex - 1, //因为nextIndex从0开始，所以可以保证-1
			lastLog.Term, rf.commitIndex, rf.me}
		Assert(len(arr) > 0, "")
	} else {
		args = &AppendEntriesArgs{rf.term, []LogEntry{rf.log[nextIndex]}, nextIndex - 1,
			lastLog.Term, rf.commitIndex, rf.me}
	}

	//
	if clearChannel {
		for len(rf.senderChannel[peerIndex]) > 0 {
			_ = <-rf.senderChannel[peerIndex]
		}
		Debug(dCommit, "生成 S%d 新的任务前，先清除队列中的任务", rf.me, peerIndex)
	}
	rf.senderChannel[peerIndex] <- &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
		args, &AppendEntriesReply{}, "Raft.AppendEntries"}
	Debug(dCommit, "生成 S%d 新的任务 %#v", rf.me, peerIndex, *args)
}

//根据match进行回退,
func (rf *Raft) backward(peerIndex int, reply *AppendEntriesReply) {
	//回退之前的nextIndex应该大于matchIndex+1，因为matchIndex一定匹配。。因为开始时0，-1所以大于好一点
	//Assert(rf.nextIndex[peerIndex] > rf.matchIndex[peerIndex], "")

	//查找是否存在
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
		//找最后一个位置
		for ; ci < len(rf.log) && rf.log[ci].Term == reply.Term; ci++ {

		}
		rf.nextIndex[peerIndex] = ci
		Debug(dCommit, "找到了follower的ConflictTerm=%d,所以设置nextIndex=这个term的下一个term的第一个index=%d",
			rf.me, reply.ConflictTerm, ci)
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

//完全覆盖版本的copy
func Copy(dst, src []LogEntry) []LogEntry {
	Assert(len(dst) == 0, "")
	for _, i := range src {
		dst = append(dst, i)
	}
	return dst
}
