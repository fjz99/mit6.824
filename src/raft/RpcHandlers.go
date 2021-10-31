package raft

// RequestVote 处理其他server的 vote rpc
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	Debug(dVote, "接收到 S%d 的投票请求 %+v", rf.me, args.CandidateId, *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Debug(dTrace, "接收到 S%d 的投票请求 %+v,进入临界区", rf.me, Args.CandidateId, *Args)

	if args.Term < rf.term {
		Debug(dVote, "不投票给 S%d，因为他的term=%d，小于我的%d", rf.me, args.CandidateId, args.Term, rf.term)
		*reply = RequestVoteReply{Term: rf.term, VoteGranted: false}
		return //!
	}
	//Debug(dTrace, "接收到 S%d 的投票请求 %+v,进入临界区 15行", rf.me, Args.CandidateId, *Args)
	if args.Term > rf.term {
		//Debug(dTrace, "接收到 S%d 的投票请求 %+v,进入临界区 17行", rf.me, Args.CandidateId, *Args)
		rf.increaseTerm(args.Term, -1)
		Debug(dTerm, "在RequestVote RPC中 接收到 S%d 的term = %d，修改", rf.me, args.CandidateId, args.Term)
	}
	//Debug(dTrace, "接收到 S%d 的投票请求 %+v,进入临界区 21行", rf.me, Args.CandidateId, *Args)
	lastLog := rf.getLastLog()
	lasLogIndex := rf.IndexSmall2Big(len(rf.log) - 1) //len=0的时候，自动变为快照的index
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if args.LastLogTerm > lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lasLogIndex) {
			Debug(dVote, "决定投票给 S%d，因为args=%+v,lastLogIndex=%d,我的term=%d，我的voteFor=%d",
				rf.me, args.CandidateId, args, lasLogIndex, rf.term, rf.voteFor)
			rf.ResetTimer() //！！
			rf.voteFor = args.CandidateId
			rf.persist()
			*reply = RequestVoteReply{Term: rf.term, VoteGranted: true}
		} else {
			Debug(dVote, "拒绝给S%d投票,因为args=%+v,lastLogIndex=%d", rf.me, args.CandidateId, args, lasLogIndex)
			*reply = RequestVoteReply{Term: rf.term, VoteGranted: false}
		}
	} else {
		Debug(dVote, "已经决定投票给 S%d 了，故拒绝S%d的投票", rf.me, rf.voteFor, args.CandidateId)
		*reply = RequestVoteReply{Term: rf.term, VoteGranted: false}
	}
	Debug(dVote, "返回 S%d的 RequestVote RPC 为 %+v", rf.me, args.CandidateId, *reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Assert(args.LeaderId != rf.me, "")
	//Debug(dTrace, "接收到 leader:S%d 的AppendEntries rpc %+v", rf.me, Args.LeaderId, *Args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Debug(dTrace, "接收到 leader:S%d 的AppendEntries rpc %+v 进入临界区！！！", rf.me, Args.LeaderId, *Args)

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
	rf.LeaderId = args.LeaderId
	if rf.state == CANDIDATE {
		rf.ChangeState(FOLLOWER)
	}
	//用于candidate转换为follower
	//偶然间发现的bug，事实上，可能有一个term相同的candidate，但是另一个选举成功了，所以此时收到了leaderId转换

	if args.Log == nil || len(args.Log) == 0 {
		//心跳
		Debug(dLog, "接收到 leader:S%d 的心跳 rpc", rf.me, args.LeaderId)

		rf.FollowerUpdateCommitIndex(args.LeaderCommit) //!

		*reply = AppendEntriesReply{Term: rf.term, Success: true}
	} else {
		Debug(dCommit, "接收到 leader:S%d 日志log rpc,leader=%+v,follower=%+v", rf.me, args.LeaderId, *args, rf.log)

		//检查日志索引位置是否存在
		exists := true
		thisLog := LogEntry{-1, -1, "args.PrevLogIndex == -1"}
		conflictIndex, conflictTerm := -1, -1

		smallPrevLogIndex := rf.IndexBig2Small(args.PrevLogIndex)
		if args.PrevLogIndex == -1 { //这个是-1才是第一次log
			exists = true //第一次log
		} else if smallPrevLogIndex >= len(rf.log) { //超出数组了
			//刚好前驱的话，smallPrevLogIndex会变成-1
			exists = false
			conflictIndex = rf.IndexSmall2Big(len(rf.log))
			conflictTerm = -1
		} else if smallPrevLogIndex < 0 {
			//比数组小
			Assert(smallPrevLogIndex == -1, "") //最多是前驱快照
			exists = true
			thisLog = LogEntry{-1, -1, "snapshot!"}
		} else {
			thisLog = rf.log[smallPrevLogIndex]
			//Assert(thisLog.Term <= Args.PrevLogTerm, "") //否则不会选举为leader，其实不是的。。严格按照fig2来，直接判断相等即可

			if thisLog.Term != args.PrevLogTerm {
				exists = false

				//查找这个term的第一个index
				conflictIndex = smallPrevLogIndex
				conflictTerm = thisLog.Term
				for conflictIndex >= 0 && rf.log[conflictIndex].Term == conflictTerm {
					conflictIndex--
				}
				conflictIndex++ //++才等于
				conflictIndex = rf.IndexSmall2Big(conflictIndex)
			}
		}

		if !exists {
			//实行backward优化
			Debug(dCommit, "没！找到log数组中对应的前驱位置，rpc 参数为 %+v,rpc handler返回", rf.me, *args)
			Debug(dCommit, "conflictIndex=%d,conflictTerm=%d", rf.me, conflictIndex, conflictTerm)
			*reply = AppendEntriesReply{Term: rf.term, Success: false, ConflictIndex: conflictIndex, ConflictTerm: conflictTerm}
			return
		}
		Debug(dCommit, "找到了log数组中对应的前驱位置，entry为 %+v", rf.me, thisLog)
		//复制
		mismatchIndex := -1
		followerLogIndex := rf.IndexBig2Small(args.PrevLogIndex + 1)
		leaderLogIndex := 0
		leaderLog := args.Log
		followerLog := rf.log

		//1.查找mismatch的地方，然后执行删除
		for leaderLogIndex < len(leaderLog) && followerLogIndex < len(followerLog) {
			if leaderLog[leaderLogIndex].Term != followerLog[followerLogIndex].Term {
				Debug(dCommit, "找到了log数组中第一个不匹配的位置，entry为 leader=%+v，follower=%+v",
					rf.me, leaderLog[leaderLogIndex], followerLog[followerLogIndex])
				mismatchIndex = followerLogIndex
				break
			}
			leaderLogIndex++
			followerLogIndex++
		}
		//1.执行删除
		if mismatchIndex != -1 {
			Debug(dTrace, "开始执行删除 mismatchIndex=%d", rf.me, mismatchIndex)
			//这个做法速度慢
			temp := rf.log
			rf.log = []LogEntry{}
			rf.log = Copy(rf.log, temp[:mismatchIndex])
			Debug(dTrace, "删除结果为 %+v", rf.me, rf.log)

			//2.复制日志
			for i := leaderLogIndex; i < len(leaderLog); i++ {
				rf.log = append(rf.log, leaderLog[i])
			}

			//删除的情况下，结尾就是新的matchIndex
			rf.matchIndex[rf.me] = Max(rf.matchIndex[rf.me], rf.IndexSmall2Big(len(rf.log)-1))
			Debug(dTrace, "更新matchIndex为 %d", rf.me, rf.matchIndex[rf.me])
		} else {
			//匹配不到，查看是到达哪个边界了
			if followerLogIndex == len(followerLog) {
				Debug(dCommit, "这次日志复制中,follower日志中没有不匹配的位置，选择将leader S%d 的日志追加到后面！", rf.me, rf.LeaderId)
				//2.复制日志
				for i := leaderLogIndex; i < len(leaderLog); i++ {
					rf.log = append(rf.log, leaderLog[i])
				}

				//追加的情况下，结尾就是新的matchIndex
				rf.matchIndex[rf.me] = Max(rf.matchIndex[rf.me], rf.IndexSmall2Big(len(rf.log)-1))
				Debug(dTrace, "更新matchIndex为 %d", rf.me, rf.matchIndex[rf.me])
			} else {
				Debug(dCommit, "WARN:这次日志复制中，leader S%d 发送的日志是follower日志的子集！", rf.me, rf.LeaderId)

				//子集的情况下，子集的结尾才是新的matchIndex！！！
				rf.matchIndex[rf.me] = Max(rf.matchIndex[rf.me], args.PrevLogIndex+len(args.Log))
				Debug(dTrace, "更新matchIndex为 %d", rf.me, rf.matchIndex[rf.me])
			}
		}
		Debug(dCommit, "我日志复制的结果为 %+v", rf.me, rf.log)

		rf.FollowerUpdateCommitIndex(args.LeaderCommit)

		//获得返回值
		Debug(dCommit, "日志复制完成", rf.me)
		rf.persist()
		*reply = AppendEntriesReply{Term: rf.term, Success: true, ConflictIndex: -1, ConflictTerm: -1}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Assert(args.LeaderId != rf.me, "")
	//Debug(dTrace, "接收到 leader:S%d 的AppendEntries rpc %+v", rf.me, Args.LeaderId, *Args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//Debug(dTrace, "接收到 leader:S%d 的AppendEntries rpc %+v 进入临界区！！！", rf.me, Args.LeaderId, *Args)

	if args.Term < rf.term {
		Debug(dInfo, "接收到 leader:S%d 的 Term = %d,忽略", rf.me, args.LeaderId, args.Term)
		*reply = InstallSnapshotReply{Term: rf.term}
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

	Debug(dSnap, "开始处理快照请求，args=%+v,我的log=%+v", rf.me, *args, rf.log)
	//保存快照，并清除旧快照
	if args.LastIncludedIndex <= rf.snapshotIndex {
		Debug(dSnap, "接收到S%d的快照的LastIncludedIndex=%d，我的snapshotIndex=%d，所以忽略",
			rf.me, args.LeaderId, args.LastIncludedIndex, rf.snapshotIndex)
		*reply = InstallSnapshotReply{Term: rf.term}
		return
	}

	rf.snapshot = args.Snapshot
	rf.snapshotMachineIndex = args.LastIncludedMachineIndex
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	//清除日志
	smallIndex := rf.IndexBig2Small(args.LastIncludedIndex)
	exists := false
	if smallIndex >= 0 && smallIndex < len(rf.log) {
		if rf.log[smallIndex].Term == args.LastIncludedTerm {
			exists = true
		} else {
			exists = false
		}
	} else {
		exists = false
	}

	if exists {
		Debug(dSnap, "快照对应的index位置找到了smallIndex=%d;%+v", rf.me, smallIndex, rf.log[smallIndex])
		rf.TrimLog(smallIndex)
		Debug(dSnap, "日志裁剪完成，结果为%+v", rf.me, rf.log)
	} else {
		Debug(dSnap, "快照对应的index位置没找到,清空log数组", rf.me)
		rf.log = []LogEntry{}
	}
	rf.persist()

	//发送快照到apply chan
	go func() {
		rf.applyCh <- ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: rf.snapshot,
			SnapshotIndex: rf.snapshotMachineIndex, SnapshotTerm: rf.snapshotTerm} //index从一开始，所以返回+1
	}()
	Debug(dSnap, "follower接收到S%d的快照，更新完成！", rf.me, args.LeaderId)
}
