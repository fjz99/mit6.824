package raft

//处理其他server的 vote rpc
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	Debug(dVote, "接收到 S%d 的投票请求 %#v", rf.me, args.CandidateId, *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term {
		Debug(dVote, "不投票给 S%d，因为他的term=%d，小于我的%d", rf.me, args.CandidateId, args.Term, rf.term)
		*reply = RequestVoteReply{Term: rf.term, VoteGranted: false}
		return //!
	}
	if args.Term > rf.term {
		rf.increaseTerm(args.Term, -1)
		Debug(dTerm, "在RequestVote RPC中 接收到 S%d 的term = %d，修改", rf.me, args.CandidateId, args.Term)
	}

	lastLog := rf.getLastLog()
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if args.LastLogTerm > lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index) {
			Debug(dVote, "决定投票给 S%d，因为args=%#v,lastLog=%#v,我的term=%d，我的voteFor=%d",
				rf.me, args.CandidateId, args, lastLog, rf.term, rf.voteFor)
			rf.ResetTimer() //！！
			rf.voteFor = args.CandidateId
			*reply = RequestVoteReply{Term: rf.term, VoteGranted: true}
		} else {
			Debug(dVote, "拒绝给S%d投票,因为args=%#v,lastLog=%#v", rf.me, args.CandidateId, args, lastLog)
			*reply = RequestVoteReply{Term: rf.term, VoteGranted: false}
		}
	} else {
		Debug(dVote, "已经决定投票给 S%d 了，故拒绝S%d的投票", rf.me, rf.voteFor, args.CandidateId)
		*reply = RequestVoteReply{Term: rf.term, VoteGranted: false}
	}
	Debug(dVote, "返回 S%d的 RequestVote RPC 为 %#v", rf.me, args.CandidateId, *reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Assert(args.LeaderId != rf.me, "")
	//Debug(dInfo, "接收到 leader:S%d 的AppendEntries rpc %#v", rf.me, args.LeaderId, *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		Debug(dInfo, "接收到 leader:S%d 的 Term = %d,忽略", rf.me, args.LeaderId, args.Term)
		*reply = AppendEntriesReply{Term: rf.term, Success: false}
		return
	} else if args.Term > rf.term {
		//leader被废黜
		Debug(dTerm, "接收到S%d的term=%d大于S%d的，修改=%d", rf.me, args.LeaderId, args.Term, rf.me, args.Term)
		if rf.state == LEADER {
			Debug(dTerm, " 被 S%d 废黜", rf.me, args.LeaderId)
		}
		rf.increaseTerm(args.Term, args.LeaderId) //此时需要修改leaderId
	}
	rf.ResetTimer() //!

	//处理leader的改动
	Assert(rf.state != LEADER, "发生脑裂") //如果term小，则丢弃；如果term大则降级为follower；如果term相同，还为leader，那就脑裂了
	rf.leaderId = args.LeaderId
	if rf.state == CANDIDATE {
		rf.ChangeState(FOLLOWER)
	}
	//用于candidatie转换为follower
	//偶然间发现的bug，事实上，可能有一个term相同的candidate，但是另一个选举成功了，所以此时收到了leaderId转换

	if args.Log == nil || len(args.Log) == 0 {
		//心跳
		Debug(dLog, "接收到 leader:S%d 的心跳 rpc", rf.me, args.LeaderId)

		rf.FollowerUpdateCommitIndex(args.LeaderCommit) //!

		*reply = AppendEntriesReply{Term: rf.term, Success: true}
	} else {
		//todo logs
		Debug(dCommit, "接收到 leader:S%d 日志log rpc,leader=%#v,follower=%#v", rf.me, args.LeaderId, args.Log, rf.log)

		//检查日志索引位置是否存在
		exists := true
		thisLog := LogEntry{-1, -1, nil}
		if args.PrevLogIndex == -1 {
			exists = true //第一次log
		} else if args.PrevLogIndex >= len(rf.log) {
			exists = false
		} else {
			thisLog = rf.log[args.PrevLogIndex]
			Assert(thisLog.Term <= args.PrevLogTerm, "") //否则不会选举为leader
			//todo 此时的index是多少？？!!!
			Assert(thisLog.Index == args.PrevLogIndex, "") //????

			if thisLog.Term < args.PrevLogTerm { //即不等于
				exists = false
			}
		}

		if !exists {
			Debug(dCommit, "没！找到log数组中对应的前驱位置，rpc 参数为 %#v,rpc handler返回", rf.me, *args)
			*reply = AppendEntriesReply{Term: rf.term, Success: false}
			return
		}
		Debug(dCommit, "找到了log数组中对应的前驱位置，entry为 %#v", rf.me, thisLog)
		//复制
		mismatchIndex := -1
		followerLogIndex := args.PrevLogIndex + 1
		leaderLogIndex := 0
		leaderLog := args.Log
		followerLog := rf.log

		//1.查找mismatch的地方，然后执行删除
		for leaderLogIndex < len(leaderLog) && followerLogIndex < len(followerLog) {
			if leaderLog[leaderLogIndex].Term != followerLog[followerLogIndex].Term {
				Debug(dCommit, "找到了log数组中第一个不匹配的位置，entry为 leader=%#v，follower=%#v",
					rf.me, leaderLog[leaderLogIndex], followerLog[followerLogIndex])
				mismatchIndex = followerLogIndex
			}
			leaderLogIndex++
			followerLogIndex++
		}
		//1.执行删除
		if mismatchIndex != -1 {
			//这个做法速度慢
			temp := rf.log
			rf.log = []LogEntry{}
			rf.log = Copy(rf.log, temp[:mismatchIndex])
			//2.复制日志
			for i := leaderLogIndex; i < len(leaderLog); i++ {
				rf.log = append(rf.log, leaderLog[i])
			}
		} else {
			//匹配不到，查看是到达哪个边界了
			if followerLogIndex == len(followerLog) {
				Debug(dCommit, "这次日志复制中,follower日志中没有不匹配的位置，选择将leader S%d 的日志追加到后面！", rf.me, rf.leaderId)
				//2.复制日志
				for i := leaderLogIndex; i < len(leaderLog); i++ {
					rf.log = append(rf.log, leaderLog[i])
				}
			} else {
				Debug(dCommit, "WARN:这次日志复制中，leader S%d 发送的日志是follower日志的子集！", rf.me, rf.leaderId)
			}
		}
		Debug(dCommit, "我日志复制的结果为 %#v", rf.me, rf.log)

		rf.FollowerUpdateCommitIndex(args.LeaderCommit)

		//获得返回值
		Debug(dCommit, "日志复制完成", rf.me)
		*reply = AppendEntriesReply{Term: rf.term, Success: true}
	}
}
