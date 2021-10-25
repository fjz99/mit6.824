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
		rf.increaseTerm(args.Term)
		Debug(dTerm, "在RequestVote RPC中 接收到 S%d 的term = %d，修改", rf.me, args.CandidateId, args.Term)
	}
	lastLog := rf.getLastLog()
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if args.LastLogTerm > lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index) {
			Debug(dVote, "决定投票给 S%d，因为args=%#v,lastLog=%#v", rf.me, args.CandidateId, args, lastLog)
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
	Debug(dLog2, "接收到 leader:S%d 的AppendEntries rpc", rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term {
		Debug(dVote, "接收到 leader:S%d 的 Term = %d,忽略", rf.me, args.LeaderId, args.Term)
		*reply = AppendEntriesReply{Term: rf.term, Success: false}
		return
	} else if args.Term > rf.term {
		//leader被废黜
		Debug(dTerm, "接收到S%d的term=%d大于S%d的，修改=%d", rf.me, args.LeaderId, args.Term, rf.me, args.Term)
		if rf.state == LEADER {
			Debug(dTerm, " 被 S%d 废黜", rf.me, args.LeaderId)
		}
		rf.increaseTerm(args.Term)
		rf.leaderId = args.LeaderId //此时需要修改leaderId
	}
	rf.ResetTimer() //!

	//处理leader的改动
	if rf.state != LEADER {
		rf.leaderId = args.LeaderId
		rf.ChangeState(FOLLOWER)
		//用于candidatie转换为follower
		//偶然间发现的bug，事实上，可能有一个term相同的candidate，但是另一个选举成功了，所以此时收到了leaderId转换
	}

	if args.Log == nil || len(args.Log) == 0 {
		//心跳
		Debug(dLog2, "接收到 leader:S%d 的心跳 rpc", rf.me, args.LeaderId)
		*reply = AppendEntriesReply{Term: rf.term, Success: true}
	} else {
		//logs
	}
	//if rf.commitIndex < args.LeaderCommit {
	//	rf.commitIndex = args.LeaderCommit
	//	Debug(dCommit, "S%d 接收到 leader:S%d 的 set commitId = %d", rf.me, args.LeaderId, args.LeaderCommit)
	//}
}
