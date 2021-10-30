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

//todo batchsize选择
//fixme labgob warning: Decoding into a non-default variable/field Term may not work
//todo 修改回溯的log也为batch
//fixme datarace
//fixme limit on 8128 simultaneously alive goroutines is exceeded, dying

const ChannelSize = 100
const HeartbeatInterval = time.Duration(200) * time.Millisecond
const EnableCheckThread = false //启动周期检查,todo 测试时别忘了关闭。。
const RpcTimeout = time.Duration(200) * time.Millisecond
const MinElectionTimeoutInterval = 250
const CommitAgreeTimeout = time.Duration(80) * time.Millisecond //一次日志添加的超时时间

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
			(s == CANDIDATE && task.RpcMethod == "Raft.AppendEntries") ||
			(s == LEADER && task.RpcMethod != "Raft.AppendEntries") {
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
				if retry := task.RpcErrorCallback(peerIndex, rf, task.Args, task.Reply); retry {
					if rf.state == FOLLOWER {
						Debug(dLog2, prefix+"state = %d,为FOLLOWER，放弃重试", rf.me, peerIndex, rf.state)
					} else {
						ch <- task
						Debug(dLog2, prefix+"rpc请求超时%+v,重试!!", rf.me, peerIndex, task.Args)
					}
				}
			},
			func() interface{} {
				//fixme 这里会data race，但是没法加锁，因为这个调用很慢
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
					if retry := task.RpcErrorCallback(peerIndex, rf, task.Args, task.Reply); retry {
						if rf.state == FOLLOWER {
							Debug(dLog2, prefix+"state = %d,为FOLLOWER，放弃重试", rf.me, peerIndex, rf.state)
						} else {
							Debug(dLog2, prefix+"请求失败%+v,重试!!", rf.me, peerIndex, task.Args)
							ch <- task
						}
					} else {
						Debug(dLog2, prefix+"对 S%d 发送请求 %+v RpcErrorCallback 不重试！", rf.me, peerIndex, peerIndex, task.Args)
					}
				} else {
					Debug(dLog2, prefix+"返回结果 %+v", rf.me, peerIndex, task.Reply)
					task.RpcSuccessCallback(peerIndex, rf, task.Args, task.Reply, task)
				}
			})
		Debug(dLog2, prefix+"对 S%d 发送请求结束 %+v", rf.me, peerIndex, peerIndex, task.Args)
	}
}

//广播，然后收集等待过半返回，指定超时时间
//日志复制的一种实现就是，区分传播轮次，每次广播的目标都是复制到leader的最新id，只要接收到过半就返回，此时可能还有发送线程在补日志，但是没事，只会影响下一轮次
//这也不会影响心跳，因为appendEntriesRPC只要不是term问题，都可以更新lastAccessTime，所以心跳可以理解为会话超时

//返回值为选举结束
//Candidate
func (rf *Raft) broadcastVote() bool {
	//Debug(dVote, "调用 broadcastVote", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//logA-1.log 校验状态
	if rf.state != CANDIDATE {
		Debug(dVote, "进入broadcastVote临界区后，发现状态发生改变为 %d ，所以终止选举", rf.me, rf.state)
		return true
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
	lastLogIndex := len(rf.log) - 1
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
		return true
	}
	if localTerm != rf.term {
		//callback中修改了
		Debug(dVote, "%d轮次选举完成，term修改为 %d,状态变为follower", rf.me, localTerm, rf.term)
		return true
	}
	if rf.agreeCounter >= rf.n/2+1 {
		Debug(dInfo, "%d轮次选举完成，票数为 %d,我 S%d 成为了leader！", rf.me, localTerm, rf.agreeCounter, rf.me)

		rf.ChangeState(LEADER)
		rf.leaderId = rf.me

		//同步执行init，保证一定可以初始化matchIndex等数组
		//start第一个条日志的时候，也依赖于LEADER状态
		rf.initLeader()

		return true
	} else {
		Debug(dVote, "%d轮次选举完成，票数为 %d,没有过半", rf.me, localTerm, rf.agreeCounter)
		return false
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dTest, "GetState 被调用！ leader = %d, state = %d", rf.me, rf.leaderId, rf.state)
	return rf.term, rf.state == LEADER
}

func (rf *Raft) check() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.state {
	case LEADER:
		Assert(rf.leaderId == rf.me, "")
		Assert(rf.voteFor == rf.me, "")
		break
	case FOLLOWER:
		Assert(rf.leaderId != rf.me, "")
		break
	case CANDIDATE:
		Assert(rf.voteFor == rf.me, "")
		Assert(rf.leaderId == -1, "")
		break
	}
	time.Sleep(time.Duration(2) * time.Second)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

//即使是log数组，也可以恢复，只要过半崩溃
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor) //注意这两个是有顺序的，，毕竟是成为byte数组。。
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	Debug(dPersist, "保存持久化数据成功 term=%d,voteFor=%d,log=%+v", rf.me, rf.term, rf.voteFor, rf.log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		panic("decode err")
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.log = log
	}
	Debug(dPersist, "读取持久化数据成功 term=%d,voteFor=%d,log=%+v", rf.me, rf.term, rf.voteFor, rf.log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	Debug(dCommit, "state=%d 接收到日志提交请求： %+v", rf.me, rf.state, command)
	//Assert(rf.state == LEADER, "") //只有leader才能提交

	if rf.state != LEADER {
		Debug(dCommit, "我不是leader，忽略日志提交请求： %+v", rf.me, command)
		defer rf.mu.Unlock() //!
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
	thisIndex := len(rf.log) - 1
	localLastLog := rf.log[len(rf.log)-1] //因为可能并发修改

	//开始广播
	Debug(dCommit, "广播日志 %+v", rf.me, rf.getLastLog())
	Debug(dCommit, "广播日志 此时leader的log为 %+v", rf.me, rf.log)
	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			Debug(dCommit, "对 S%d发送日志", rf.me, i)
			if len(rf.senderChannel[i]) > 0 {
				Debug(dCommit, "WARN:企图对S%d发送日志的时候，队列中还有别的内容等待发送len=%d", rf.me, i, len(rf.senderChannel[i]))
			}
			prev, prevIndex := rf.getLastLastLog()
			args := &AppendEntriesArgs{rf.term, []LogEntry{rf.getLastLog()}, prevIndex,
				prev.Term, rf.commitIndex, rf.me}
			//task := &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
			//	args, &AppendEntriesReply{}, "Raft.AppendEntries"}
			if command == nil {
				//对应刚开始，matchIndex=-1
				rf.cleanupSenderChannelFor(i)
				rf.senderChannel[i] <- &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
					args, &AppendEntriesReply{}, "Raft.AppendEntries"}
			} else {
				rf.cleanupSenderChannelFor(i)
				//根据matchIndex生成任务
				rf.generateNewTask(i, true, true)
			}
			//rf.checkSenderChannelTask(i, task)
			//rf.senderChannel[i] <- &Task{appendEntriesRpcFailureCallback, appendEntriesRpcSuccessCallback,
			//	args, &AppendEntriesReply{}, "Raft.AppendEntries"}
		}
	}
	rf.mu.Unlock()

	rf.TimedWait(CommitAgreeTimeout, func(rf *Raft) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dCommit, "日志index=%d提交超时,失败！此时commitIndex为%d", rf.me, thisIndex, rf.commitIndex)
	}, func() interface{} {
		rf.mu.Lock()
		defer rf.mu.Unlock() //todo 可能死锁
		for rf.commitIndex < thisIndex {
			rf.CommitIndexCondition.Wait()
			Debug(dCommit, "接收到commitId修改的广播，check", rf.me)
			Debug(dCommit, "我要的index=%d但是commitId=%d", rf.me, thisIndex, rf.commitIndex)
		}
		return rf.commitIndex
	}, func(rf *Raft, result interface{}) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dCommit, "日志index=%d提交成功，此时commitIndex为%d", rf.me, thisIndex, rf.commitIndex)

		//广播commitId,go 防止死锁，因为defer
		//go rf.broadCastHeartBeat() //这个需要吗？
	})

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dCommit, " 日志提交请求返回，结果为 commitIndex=%d,返回logindex=%d,修正后为 %d",
		rf.me, rf.commitIndex, thisIndex, localLastLog.Index+1)

	//Assert(rf.state == LEADER, "") //todo ????????
	//完全可能被修改了，毕竟不是整体的锁
	return localLastLog.Index + 1, rf.term, rf.state == LEADER //index从一开始，所以返回+1
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a logInitLock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
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
			//Debug(dLeader, "对 S%d发送心跳", rf.me, i)
			if len(rf.senderChannel[i]) == 0 {
				args := &AppendEntriesArgs{rf.term, nil, -1, -1, rf.commitIndex, rf.me}
				rf.senderChannel[i] <- &Task{heartBeatRpcFailureCallback, heartBeatRpcSuccessCallback,
					args, &AppendEntriesReply{}, "Raft.AppendEntries"}
			} else {
				Debug(dLeader, " -> S%d 发送队列中还有数据，长度为%d，所以不发送心跳", rf.me, i, len(rf.senderChannel[i]))
			}
		}
	}
}

func (rf *Raft) initLeader() {
	Debug(dLeader, "开始leader初始化!", rf.me)

	rf.leaderId = rf.me
	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)
	SetArrayValue(rf.nextIndex, len(rf.log))
	SetArrayValue(rf.matchIndex, -1)

	//todo 心跳检测等待结果，决定是否主动降级
	//rf.broadCastHeartBeat() //只需要广播一次即可

	//异步执行，否则会死锁
	//空entry，这个可以替代broadCastHeartBeat
	go rf.Start(nil) //这个自带心跳广播的功能

	Debug(dLeader, "init leader done!nil start已提交，但是可能没执行完", rf.me)
}

//也要负责和状态转换有关的属性设置
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
	//超时检查
	Debug(dInfo, "主循环进入 follower", rf.me)
	//等待状态转换或者超时
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
			//if states.to == LEADER {
			//变成leader了
			//rf.initLeader() //同步的
			//}
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

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
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
	rf.lastApplied = -1
	rf.matchIndex = make([]int, rf.n) //初始化用于leader，或者follower自己的matchIndex
	SetArrayValue(rf.matchIndex, -1)  //必备，因为持久化后重启的时候，需要是-1

	//通信
	rf.mu = NewReentrantLock()
	rf.broadCastCondition = sync.NewCond(rf.mu)
	rf.CommitIndexCondition = sync.NewCond(rf.mu)
	rf.senderChannel = make([]chan *Task, rf.n)
	rf.stateChanging = make(chan *ChangedState, ChannelSize)
	rf.waitGroup = sync.WaitGroup{}
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

	// initialize from state persisted before a crash
	//放在这里也行，毕竟是同步执行的，而且此时ticker线程还没启动，不用加锁
	rf.readPersist(persister.ReadRaftState())
	if rf.voteFor == rf.me {
		rf.increaseTerm(rf.term+1, -1)
	}

	go rf.ticker()

	if EnableCheckThread {
		go rf.check()
	}

	return rf
}
