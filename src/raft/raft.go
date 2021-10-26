package raft

//fixme 超时了。。10s
//todo 心跳不足则leader降级
//todo 日志提交的时候也是，如果要发心跳了，而此时队列里还有东西，就没必要发送心跳了，因为日志提交也有心跳的功能

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const ChannelSize = 10
const HeartbeatInterval = time.Duration(200) * time.Millisecond
const MaxElectionInterval = time.Duration(400) * time.Millisecond
const EnableCheckThread = false //启动周期检查,todo 测试时别忘了关闭。。
const RpcTimeout = time.Duration(200) * time.Millisecond
const MinElectionTimeoutInterval = 250

//每个发送线程
//peerIndex 即对应的在peer数组的偏移
func (rf *Raft) messageSender(peerIndex int) {
	Debug(dLog, "发送线程 %d 初始化成功", rf.me, peerIndex)
	ch := rf.senderChannel[peerIndex]
	endpoint := rf.peers[peerIndex]
	prefix := "发送线程 %d "
	counter := 0

	for !rf.killed() {
		task := <-ch
		//t := GetNow() //控制超时，超时则会放弃执行回调,rpc库的超时最多7s，会影响下一次rpc。。
		Debug(dLog, prefix+"对 S%d 发送请求 %#v", rf.me, peerIndex, peerIndex, task.args)
		rf.TimedWait(rf.rpcTimeout,
			func(rf *Raft) {
				Debug(dLog, prefix+"rpc请求超时%#v", rf.me, peerIndex, task.args)
				counter++
				//因为rpc超时时间一定小于选举超时时间，所以就可以
				if retry := task.RpcErrorCallback(peerIndex, rf, task.args, task.reply, &counter); retry {
					Debug(dLog, prefix+"rpc请求超时%#v,重试!!", rf.me, peerIndex, task.args)
					ch <- task
				} else {
					counter = 0
				}
			},
			func() interface{} {
				//fixme 这里会data race，但是没法加锁，因为这个调用很慢
				//但是发生概率很低
				return endpoint.Call(task.rpcMethod, task.args, task.reply)
			},
			func(rf *Raft, result interface{}) {
				ok := result.(bool)
				if !ok {
					Debug(dLog, prefix+"请求失败 %#v", rf.me, peerIndex, task.args)
					counter++
					//这个是rpc库本身请求失败
					if retry := task.RpcErrorCallback(peerIndex, rf, task.args, task.reply, &counter); retry {
						Debug(dLog, prefix+"请求失败%#v,重试!!", rf.me, peerIndex, task.args)
						ch <- task
					} else {
						counter = 0
					}
				} else {
					Debug(dLog, prefix+"返回结果 %#v", rf.me, peerIndex, task.reply)
					task.RpcSuccessCallback(peerIndex, rf, task.args, task.reply)
					counter = 0
				}
			})
		if counter == 0 {
			Debug(dLog, prefix+"对 S%d 发送请求结束 %#v", rf.me, peerIndex, peerIndex, task.args)
		}
	}
}

//广播，然后收集等待过半返回，指定超时时间
//日志复制的一种实现就是，区分传播轮次，每次广播的目标都是复制到leader的最新id，只要接收到过半就返回，此时可能还有发送线程在补日志，但是没事，只会影响下一轮次
//这也不会影响心跳，因为appendEntriesRPC只要不是term问题，都可以更新lastAccessTime，所以心跳可以理解为会话超时

//返回值为选举结束
//Candidate
func (rf *Raft) broadcastVote() bool {
	Debug(dVote, "调用 broadcastVote", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//logA-1.log 校验状态
	if rf.state != CANDIDATE {
		Debug(dVote, "进入broadcastVote临界区后，发现状态发生改变为 %d ，所以终止选举", rf.me, rf.state)
		return true
	}
	rf.term++
	rf.voteFor = rf.me
	rf.doneRPCs = 0
	rf.agreeCounter = 1
	rf.leaderId = -1
	localTerm := rf.term //因为可能在这个轮次中，返回term修改
	var args *RequestVoteArgs
	lastLog := rf.getLastLog()
	args = &RequestVoteArgs{rf.term, rf.me, lastLog.Index, lastLog.Term}
	Debug(dVote, "开始一轮选举 Term = %d req = %#v", rf.me, rf.term, *args)
	for i := 0; i < rf.n; i++ {
		if i != rf.me {
			//避免多个任务访问args的竞争。。
			args = &RequestVoteArgs{rf.term, rf.me, lastLog.Index, lastLog.Term}
			rf.senderChannel[i] <- &Task{voteRpcFailureCallback,
				voteRpcSuccessCallback, args, &RequestVoteReply{}, "Raft.RequestVote"}
			//注意reply每次都要创建新的才行
		}
	}
	//rf.waitGroup.Add(rf.n / 2) //算上自己就是过半了！
	//rf.mu.Unlock()             //!不会自动释放锁
	//rf.waitGroup.Wait()
	for !(rf.agreeCounter >= rf.n/2+1 || rf.doneRPCs == rf.n-1) {
		rf.broadCastCondition.Wait()
		//重新获得了锁
		//等待所有rpc结束，非常影响性能
		Debug(dTrace, "broadCastCondition wait被唤醒，%d，%d", rf.me, rf.agreeCounter, rf.doneRPCs)
	}

	//其他人变成leader
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
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
		Debug(dVote, "%d轮次选举完成，票数为 %d,我 S%d 成为了leader！", rf.me, localTerm, rf.agreeCounter, rf.me)
		//主循环处理任何状态变更
		rf.ChangeState(LEADER)
		rf.leaderId = rf.me
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
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
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

//初始化一个新的leader
func (rf *Raft) initLeader() {
	Debug(dLeader, "开始leader初始化!", rf.me)
	//细粒度锁
	rf.mu.Lock()
	rf.leaderId = rf.me
	rf.nextIndex = make([]int, rf.n)
	rf.matchIndex = make([]int, rf.n)
	SetArrayValue(rf.nextIndex, len(rf.log))
	SetArrayValue(rf.matchIndex, -1)
	rf.mu.Unlock()

	//不用一直持续广播，见fig.2，暂时广播一整个超时时间
	//todo 心跳检测等待结果，决定是否主动降级
	rf.broadCastHeartBeat()

	//t := GetNow()
	//for true {
	//	if GetNow()-t > MaxElectionInterval.Milliseconds() {
	//		break
	//	}
	//	rf.broadCastHeartBeat()
	//	time.Sleep(time.Duration(50) * time.Millisecond)
	//}

	Debug(dLeader, "init leader done!", rf.me)
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
				//变成follower了
				Debug(dLeader, "leader 被 S%d term=%d 降级了！", rf.me, rf.leaderId, rf.term)
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
			//rf.mu.Lock() 不要！
			Debug(dInfo, "主循环接收到状态转换 %d -> %d", rf.me, states.from, states.to)
			if states.to == LEADER {
				//变成leader了
				rf.initLeader() //同步的
			}
			//rf.mu.Unlock()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
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

	//通信
	rf.broadCastCondition = sync.NewCond(&rf.mu)
	rf.senderChannel = make([]chan *Task, rf.n)
	rf.stateChanging = make(chan *ChangedState, ChannelSize)
	rf.waitGroup = sync.WaitGroup{}
	rf.rpcTimeout = RpcTimeout //Rpc timeout要短一些
	rf.stateChanging = make(chan *ChangedState)
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
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	if EnableCheckThread {
		go rf.check()
	}

	return rf
}
