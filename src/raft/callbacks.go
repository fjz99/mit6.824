package raft

//只有rpc通信是并行的
func voteRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//req := Args.(*RequestVoteArgs

	Debug(dVote, "S%d -> S%d 选举 RPC失败，不重试 state=%d", rf.me, rf.me, peerIndex, rf.state)
	rf.doneRPCs++ //return false才这样！
	rf.broadCastCondition.Broadcast()
	//rf.waitGroup.Done()
	return false
}

func voteRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}, task *Task) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//req := Args.(*RequestVoteArgs)
	resp := reply.(*RequestVoteReply)
	rf.doneRPCs++
	if resp.Term > rf.term {
		//转为follower
		Debug(dTerm, "接收到S%d返回，但是term大于当前服务器S%d", rf.me, peerIndex, rf.me)
		rf.increaseTerm(resp.Term, -1)
	}
	//验证投票同意与否
	if resp.VoteGranted {
		rf.agreeCounter++
		Debug(dVote, "接收到S%d返回同意投票", rf.me, peerIndex)
	} else {
		Debug(dVote, "接收到S%d返回拒绝投票！！", rf.me, peerIndex)
	}
	//rf.waitGroup.Done()
	rf.broadCastCondition.Broadcast()
}

func heartBeatRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) bool {
	Debug(dLeader, "leader：对 S%d发送心跳rpc失败！", rf.me, peerIndex)
	return false
}

//这里，假如选为leader之后，在init的地方for循环多次发送的话，因为外部加的是for循环整体的锁，而回调函数需要锁，所以阻塞了所有的回调函数，导致发送队列阻塞
func heartBeatRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}, task *Task) {
	Debug(dLeader, "leader：对 S%d发送心跳rpc成功！", rf.me, peerIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp := reply.(*AppendEntriesReply)
	if resp.Term > rf.term {
		Debug(dLeader, "接收到S%d返回，但是term大于当前服务器S%d,被降级", rf.me, peerIndex, rf.me)
		rf.increaseTerm(resp.Term, -1)
	}
}

func appendEntriesRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dCommit, "leader：对 S%d发送日志log rpc失败,自动重试", rf.me, peerIndex)
	rf.cleanupSenderChannelFor(peerIndex) //也可以清空发送队列,否则网络分区故障之后，会因为chan size不足而死锁
	req := args.(*AppendEntriesArgs)
	//重试不要通过返回true来实现，而要通过自己手动添加到chan来实现,这样就可以完成自己复制一个Task就不会完成任何冲突
	thisArgs := &AppendEntriesArgs{req.Term, req.Log, req.PrevLogIndex,
		req.PrevLogTerm, req.LeaderCommit, req.LeaderId}

	rf.senderChannel[peerIndex] <- &Task{appendEntriesRpcFailureCallback,
		appendEntriesRpcSuccessCallback, thisArgs, &AppendEntriesReply{}, "Raft.AppendEntries"}
	return false
}

func appendEntriesRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}, task *Task) {
	Debug(dCommit, "leader：对 S%d发送日志log rpc成功！", rf.me, peerIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp := reply.(*AppendEntriesReply)
	req := args.(*AppendEntriesArgs)
	if resp.Term > rf.term {
		Debug(dTerm, "接收到S%d返回，但是term大于当前服务器S%d,被降级", rf.me, peerIndex, rf.me)
		rf.increaseTerm(resp.Term, -1)
	}

	if resp.Success {
		Assert(req.Log != nil && len(req.Log) > 0, "") //这个是心跳，按理说不应该执行这个回调

		//matchIndex check
		reqMaxIndex := req.PrevLogIndex + len(req.Log)
		//因为完全包含的话，日志也算复制成功。。
		//修改matchIndex,matchIndex可以在不提交的时候修改
		if rf.matchIndex[peerIndex] < reqMaxIndex {
			rf.matchIndex[peerIndex] = reqMaxIndex
			rf.LeaderUpdateCommitIndex()
		}

		//nextIndex check
		rf.nextIndex[peerIndex] = reqMaxIndex + 1
		//这个不要最大值。。因为初始化的时候是乐观估计，是leader的log最大值
		Debug(dCommit, "接收到S%d返回，日志复制成功,修改matchIndex=%d，nextIndex=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], rf.nextIndex[peerIndex])
		rf.generateNewTask(peerIndex, true, true)
		//归零
	} else {
		rf.backward(peerIndex, resp)
		Debug(dCommit, "接收到S%d返回，日志复制失败,修改matchIndex=%d，nextIndex=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], rf.nextIndex[peerIndex])

		//fixme 根据match进行回退
		rf.generateNewTask(peerIndex, false, true)
	}

}
