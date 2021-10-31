package raft

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//todo 不用clearChannel了，选举为leader之后clear一下就行了；心跳还是一样，有内容就不发送

const ChannelSize = 100
const HeartbeatInterval = time.Duration(200) * time.Millisecond
const RpcTimeout = time.Duration(100) * time.Millisecond //事实上可以不自己设置超时时间也能通过
const MinElectionTimeoutInterval = 250
const BatchSize = 50 //20个log一个batch

//每个发送线程
//peerIndex 即对应的在peer数组的偏移
func (rf *Raft) messageSender(peerIndex int) {
	//Debug(dLog, "发送线程 %d 初始化成功", rf.me, peerIndex)
	ch := rf.senderChannel[peerIndex]
	endpoint := rf.peers[peerIndex]
	prefix := "发送线程 %d "

	for !rf.killed() {
		task := <-ch

		Debug(dLog2, prefix+"对 S%d 发送请求 %+v", rf.me, peerIndex, peerIndex, task.Args)

		rf.mu.Lock()
		s := rf.state
		rf.mu.Unlock()
		if s == FOLLOWER ||
			(s == CANDIDATE && task.RpcMethod != "Raft.RequestVote") ||
			(s == LEADER && task.RpcMethod == "Raft.RequestVote") {
			Debug(dLog2, prefix+"拒绝发送任务，当前state=%d，任务rpc为：%s", rf.me, peerIndex, s, task.RpcMethod)
			continue
		}
		//Debug(dTrace, prefix+"对 S%d 发送请求 %+v 进入临界区！", rf.me, peerIndex, peerIndex, task.Args)

		rf.TimedWait(rf.rpcTimeout,
			func(rf *Raft) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				Debug(dLog2, prefix+"rpc请求超时%+v", rf.me, peerIndex, task.Args)
				//因为rpc超时时间一定小于选举超时时间，所以就可以
				task.RpcErrorCallback(peerIndex, rf, task.Args, task.Reply)
			},
			func() interface{} {
				//每次调用这个，使用的都是不同的Task对象，所以不会data race
				return endpoint.Call(task.RpcMethod, task.Args, task.Reply)
			},
			func(rf *Raft, result interface{}) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				ok := result.(bool)

				if !ok {
					Debug(dLog2, prefix+"请求失败 %+v", rf.me, peerIndex, task.Args)
					//这个是rpc库本身请求失败
					task.RpcErrorCallback(peerIndex, rf, task.Args, task.Reply)
				} else {
					Debug(dLog2, prefix+"返回结果 %+v", rf.me, peerIndex, task.Reply)
					go task.RpcSuccessCallback(peerIndex, rf, task.Args, task.Reply) //这里因为要wait所以。。
				}
			})
		Debug(dLog2, prefix+"对 S%d 发送请求结束 %+v", rf.me, peerIndex, peerIndex, task.Args)
	}
}

//Candidate
func (rf *Raft) broadcastVote() {
	//Debug(dVote, "调用 broadcastVote", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//logA-1.log 校验状态
	if rf.state != CANDIDATE {
		Debug(dVote, "进入broadcastVote临界区后，发现状态发生改变为 %d ，所以终止选举", rf.me, rf.state)
		return
	}
	rf.term++
	rf.voteFor = rf.me
	rf.persist() //此方法内部没有lock
	rf.doneRPCs = 0
	rf.agreeCounter = 1
	rf.leaderId = -1
	localTerm := rf.term //因为可能在这个轮次中，返回term修改
	var args *RequestVoteArgs
	lastLog := rf.getLastLog()
	lastLogIndex := rf.IndexSmall2Big(len(rf.log) - 1)
	args = &RequestVoteArgs{rf.term, rf.me, lastLogIndex, lastLog.Term}
	Debug(dVote, "开始一轮选举 Term = %d req = %+v", rf.me, rf.term, *args)
	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			//避免多个任务访问args的竞争。。
			args = &RequestVoteArgs{rf.term, rf.me, lastLogIndex, lastLog.Term}
			rf.senderChannel[i] <- &Task{voteRpcFailureCallback,
				voteRpcSuccessCallback, args, &RequestVoteReply{}, "Raft.RequestVote"}
			//注意reply每次都要创建新的才行
		}
	}
	for !(rf.agreeCounter >= rf.n/2+1 || rf.doneRPCs == rf.n-1) {
		rf.broadCastCondition.Wait()
		//重新获得了锁
		//Debug(dTrace, "broadCastCondition wait被唤醒，%d，%d", rf.me, rf.agreeCounter, rf.doneRPCs)
	}

	//其他人变成leader
	if rf.leaderId != -1 {
		//心跳handler中处理了
		Debug(dVote, "%d 轮次选举完成，已发现新的leader S%d", rf.me, rf.term, rf.leaderId)
		return
	}
	if localTerm != rf.term {
		//callback中修改了
		Debug(dVote, "%d轮次选举完成，term修改为 %d,状态变为follower", rf.me, localTerm, rf.term)
		return
	}
	if rf.agreeCounter >= rf.n/2+1 {
		Debug(dInfo, "%d轮次选举完成，票数为 %d,我 S%d 成为了leader！", rf.me, localTerm, rf.agreeCounter, rf.me)

		rf.ChangeState(LEADER)
		rf.leaderId = rf.me

		//同步执行init，保证一定可以初始化matchIndex等数组
		//start第一个条日志的时候，也依赖于LEADER状态
		rf.initLeader()

		return
	} else {
		Debug(dVote, "%d轮次选举完成，票数为 %d,没有过半", rf.me, localTerm, rf.agreeCounter)
		return
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dTest, "GetState 被调用！ leader = %d, state = %d", rf.me, rf.leaderId, rf.state)
	return rf.term, rf.state == LEADER
}

//即使是log数组，也可以恢复，只要过半崩溃
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor) //注意这两个是有顺序的，，毕竟是成为byte数组。。
	e.Encode(rf.log)
	e.Encode(rf.snapshot)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotMachineIndex)
	e.Encode(rf.snapshotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "保存持久化数据成功 term=%d,voteFor=%d,log=%+v,snapshotIndex=%d,snapshotMachineIndex=%d,"+
		"snapshotTerm=%d", rf.me, rf.term, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotMachineIndex, rf.snapshotTerm)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []LogEntry
	var si, st, mi int
	var ss []byte
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&ss) != nil ||
		d.Decode(&si) != nil ||
		d.Decode(&mi) != nil ||
		d.Decode(&st) != nil {
		panic("decode err")
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.log = log
		rf.snapshot = ss
		rf.snapshotIndex = si
		rf.snapshotMachineIndex = mi
		rf.snapshotTerm = st
	}
	Debug(dPersist, "读取持久化数据成功 term=%d,voteFor=%d,log=%+v,snapshotIndex=%d,snapshotMachineIndex=%d,snapshotTerm=%d",
		rf.me, rf.term, rf.voteFor, rf.log, rf.snapshotIndex, rf.snapshotMachineIndex, rf.snapshotTerm)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//

// CondInstallSnapshot 状态机用来询问raft是否可以安装快照,对应follower
//状态机通过chan读取RPC传送过来的快照信息，并查询是否可以安装
//raft在接收到快照之后，就会安装，即安装别人的快照，根据当前的commitIndex决定是否安装
//对于状态机而言，除非这个比commitId大，否则没必要安装，不过对于raft而言，是肯定要截断的
//状态机应该同步执行这个方法，保证先安装快照，在读取后面的commit数据
//见 https://www.cnblogs.com/sun-lingyu/p/14591757.html
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "CondInstallSnapshot请求 lastIncludedTerm=%d,lastIncludedIndex=%d", rf.me, lastIncludedTerm, lastIncludedIndex)
	Debug(dSnap, "CondInstallSnapshot请求 当前log为 %+v", rf.me, rf.log)
	//直接判断commitId即可
	if rf.commitIndex >= rf.snapshotIndex {
		Debug(dSnap, "CondInstallSnapshot请求 返回true,commitId=%d,snapshotIndex=%d", rf.me, rf.commitIndex, rf.snapshotIndex)
		return false
	} else {
		Debug(dSnap, "CondInstallSnapshot请求 返回false,commitId=%d,snapshotIndex=%d", rf.me, rf.commitIndex, rf.snapshotIndex)
		return true
	}
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.

// Snapshot 状态机通过这个方法告诉raft，它进行了状态快照
//快照的byte由raft存储，状态机只负责创建
//leader和follower都可以创建快照,这个index指的是快照对应的最后一个index
//index从1开始
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "状态机提交快照请求 原始index=%d，log=%+v", rf.me, index, rf.log)
	Debug(dSnap, "状态机当前快照信息：snapshotIndex=%d,snapshotTerm=%d,snapshotMachineIndex=%d", rf.me,
		rf.snapshotIndex, rf.snapshotTerm, rf.snapshotMachineIndex)

	//先找log index
	smallIndex := rf.findSmallIndex(index)

	if smallIndex == len(rf.log) {
		Debug(dSnap, "index=%d 不存在，所以忽略,可能是太大或太小", rf.me, index)
		return
	}

	bigIndex := rf.IndexSmall2Big(smallIndex)

	Assert(bigIndex <= rf.commitIndex, "")

	if bigIndex <= rf.snapshotIndex {
		Debug(dSnap, "bigIndex=%d <= snapIndex=%d，所以忽略", rf.me, bigIndex, rf.snapshotIndex)
		return
	}

	//更新和截断
	rf.snapshot = snapshot
	rf.snapshotIndex = bigIndex
	rf.snapshotTerm = rf.log[smallIndex].Term
	rf.snapshotMachineIndex = index

	//截断日志
	rf.TrimLog(smallIndex)

	//持久化
	rf.persist()
	Debug(dSnap, "更新快照信息为snapshotIndex=%d，term=%d", rf.me, rf.snapshotIndex, rf.snapshotTerm)

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dCommit, "state=%d 接收到日志提交请求： %+v", rf.me, rf.state, command)

	if rf.state != LEADER {
		Debug(dCommit, "我不是leader，忽略日志提交请求： %+v", rf.me, command)
		return rf.commitIndex, rf.term, false
	}

	//添加到本地
	lastLog := rf.getLastLog()
	if command == nil {
		//开始的时候lastLog的index是-1，所以这个index会变成0
		rf.log = append(rf.log, LogEntry{Term: rf.term, Index: lastLog.Index, Command: command})
	} else {
		rf.log = append(rf.log, LogEntry{Term: rf.term, Index: lastLog.Index + 1, Command: command})
	}
	rf.persist()
	rf.logAppendCondition.Broadcast()
	thisIndex := rf.IndexSmall2Big(len(rf.log) - 1)
	localLastLog := rf.log[len(rf.log)-1] //因为可能并发修改

	//开始广播
	Debug(dCommit, "广播日志 %+v", rf.me, rf.getLastLog())
	Debug(dCommit, "广播日志 此时leader的log为 %+v", rf.me, rf.log)
	if command == nil {
		//对应刚开始，matchIndex=-1,启动日志发送
		for i := 0; i < rf.n; i++ {
			if i != rf.me {
				//Debug(dCommit, "对 S%d发送日志", rf.me, i)
				if len(rf.senderChannel[i]) > 0 {
					Debug(dCommit, "WARN:企图对S%d发送日志的时候，队列中还有别的内容等待发送len=%d", rf.me, i, len(rf.senderChannel[i]))
				}

				prev, prevIndex := rf.getLastLastLog() //prevIndex是bigIndex
				args := &AppendEntriesArgs{rf.term, []LogEntry{rf.getLastLog()}, prevIndex,
					prev.Term, rf.commitIndex, rf.me}

				rf.cleanupSenderChannelFor(i)
				rf.senderChannel[i] <- &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
					args, &AppendEntriesReply{}, "Raft.AppendEntries"}
			}
		}
	}

	Debug(dCommit, " 日志提交请求返回，结果为 commitIndex=%d,返回logindex=%d,修正后为 %d",
		rf.me, rf.commitIndex, thisIndex, localLastLog.Index+1)

	//Assert(rf.state == LEADER, "") //完全可能被修改了，毕竟不是整体的锁
	return localLastLog.Index + 1, rf.term, rf.state == LEADER //index从一开始，所以返回+1
}

func (rf *Raft) Kill() {
	Debug(dPersist, "服务器下线，被kill了", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) broadCastHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLeader, "广播心跳", rf.me)
	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			//Debug(dTrace, "对 S%d发送心跳", rf.me, i)

			if len(rf.senderChannel[i]) == 0 && rf.IndexBig2Small(rf.matchIndex[i]) == len(rf.log)-1 {
				args := &AppendEntriesArgs{rf.term, nil, -1, -1, rf.commitIndex, rf.me}
				rf.senderChannel[i] <- &Task{heartBeatRpcFailureCallback, heartBeatRpcSuccessCallback,
					args, &AppendEntriesReply{}, "Raft.AppendEntries"}
			} else {
				Debug(dLeader, " -> S%d 不发送心跳,此时队列长度为%d，matchIndex=%d，len of log=%d(未修正)", rf.me,
					i, len(rf.senderChannel[i]), rf.matchIndex[i], len(rf.log))
			}
		}
	}
}

func (rf *Raft) initLeader() {
	Debug(dLeader, "开始leader初始化!", rf.me)

	rf.leaderId = rf.me
	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)
	SetArrayValue(rf.nextIndex, rf.IndexSmall2Big(len(rf.log)))
	SetArrayValue(rf.matchIndex, -1)

	//todo 心跳检测等待结果，决定是否主动降级
	//rf.broadCastHeartBeat() //只需要广播一次即可

	//异步执行，否则会死锁
	//空entry，这个可以替代broadCastHeartBeat
	go rf.Start(nil) //这个自带心跳广播的功能

	Debug(dLeader, "init leader done!nil start已提交，但是可能没执行完", rf.me)
}

func (rf *Raft) processLeader() {
	Debug(dInfo, "主循环进入 Leader", rf.me)
	select {
	case states := <-rf.stateChanging:
		{
			rf.mu.Lock() //这里似乎有死锁的可能，比如一个获得锁的线程企图写chan，当前线程却正好阻塞在这里。。，所以chan设置为有缓冲区的
			Debug(dInfo, "主循环接收到状态转换 %d -> %d", rf.me, states.from, states.to)
			if states.to == CANDIDATE {
				//CANDIDATE是超时转换的！
				Debug(dError, "状态转换无效！ %d -> %d", rf.me, states.from, states.to)
			} else {
				//变成follower了,清空发送队列
				rf.cleanupSenderChannel()
				Debug(dLeader, "leader 被 S%d term=%d 降级了为follower！，清空发送队列", rf.me, rf.leaderId, rf.term)
			}
			rf.mu.Unlock()
			break
		}
	case _ = <-time.After(HeartbeatInterval):
		{
			rf.broadCastHeartBeat()
			break
		}
	}
}

func (rf *Raft) processFollower() {
	Debug(dInfo, "主循环进入 follower", rf.me)

	select {
	case states := <-rf.stateChanging:
		{
			rf.mu.Lock()
			Debug(dInfo, "主循环接收到状态转换 %d -> %d", rf.me, states.from, states.to) //用于debug
			if states.to == CANDIDATE || states.to == LEADER {
				//CANDIDATE是超时转换的！
				Debug(dError, "状态转换无效！ %d -> %d", rf.me, states.from, states.to)
			}
			rf.mu.Unlock()
			break
		}
	case _ = <-time.After(rf.electionInterval):
		{
			rf.mu.Lock()
			Debug(dTrace, "校验超时 %d %d", rf.me, GetNow()-rf.lastAccessTime, rf.electionInterval.Milliseconds())
			if GetNow()-rf.lastAccessTime >= rf.electionInterval.Milliseconds() {
				//选举超时
				Debug(dVote, " %d > %d 超时，进入candidate，并开始选举", rf.me, GetNow()-rf.lastAccessTime, rf.electionInterval.Milliseconds())
				rf.state = CANDIDATE
				rf.ResetTimer()
				go rf.broadcastVote()
			}
			rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) processCandidate() {
	Debug(dInfo, "主循环进入 Candidate", rf.me)
	select {
	case states := <-rf.stateChanging:
		{
			rf.mu.Lock() //rf.me
			Debug(dInfo, "主循环接收到状态转换 %d -> %d", rf.me, states.from, states.to)
			if states.to == FOLLOWER {
				rf.cleanupSenderChannel()
			}
			rf.mu.Unlock()
			break
		}
	case _ = <-time.After(rf.electionInterval):
		{
			rf.mu.Lock()
			if GetNow()-rf.lastAccessTime >= rf.electionInterval.Milliseconds() {
				//选举超时
				Debug(dVote, "选举超时，candidate，再次开始选举", rf.me)
				rf.ResetTimer()
				go rf.broadcastVote()
			}
			rf.mu.Unlock()
			break
		}
	}
}

// 心跳线程是真正的主线程
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case LEADER:
			rf.processLeader()
			break
		case CANDIDATE:
			rf.processCandidate()
			break
		case FOLLOWER:
			rf.processFollower()
			break
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	InitLog() // 初始化日志系统

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.n = len(peers)
	rf.applyCh = applyCh
	//选举相关
	rf.state = FOLLOWER
	rf.leaderId = -1
	rf.voteFor = -1
	rf.term = 0

	//commit
	rf.commitIndex = -1
	rf.matchIndex = make([]int, rf.n) //初始化用于leader，或者follower自己的matchIndex
	SetArrayValue(rf.matchIndex, -1)  //必备，因为持久化后重启的时候，需要是-1
	rf.snapshotIndex = -1             //用于计算log的index
	rf.snapshotTerm = -1
	rf.snapshotMachineIndex = -1

	//通信
	rf.mu = NewReentrantLock()
	rf.broadCastCondition = sync.NewCond(rf.mu)
	rf.logAppendCondition = sync.NewCond(rf.mu)
	rf.senderChannel = make([]chan *Task, rf.n)
	rf.fuckerChan = make(chan *ApplyMsg, 1000)
	rf.stateChanging = make(chan *ChangedState, ChannelSize)
	rf.rpcTimeout = RpcTimeout //Rpc timeout要短一些
	rf.ResetTimer()

	for i := 0; i < rf.n; i++ {
		rf.senderChannel[i] = make(chan *Task, ChannelSize)
		if i != me {
			go rf.messageSender(i)
		}
	}

	//初始化选举超时时间,避免重复，增加间隔
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionInterval = time.Duration(r.Int31n(151)+MinElectionTimeoutInterval) * time.Millisecond
	Debug(dInfo, "选举超时时间为 %s", rf.me, rf.electionInterval.String())

	//放在这里也行，毕竟是同步执行的，而且此时ticker线程还没启动，不用加锁
	rf.readPersist(persister.ReadRaftState())
	if rf.voteFor == rf.me {
		rf.increaseTerm(rf.term+1, -1)
	}

	go rf.ticker()

	go func() {
		fc := rf.fuckerChan
		ac := rf.applyCh
		for i := range fc {
			ac <- *i
		}
	}()

	return rf
}
