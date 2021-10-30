package raft

//只有rpc通信是并行的
func voteRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//req := Args.(*RequestVoteArgs

	Debug(dVote, "S%d -> S%d 选举 RPC失败，不重试 state=%d", rf.me, rf.me, peerIndex, rf.state)
	rf.doneRPCs++ //return false才这样！
	rf.broadCastCondition.Broadcast()
	//rf.waitGroup.Done()
	return
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

func heartBeatRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	Debug(dLeader, "leader：对 S%d发送心跳rpc失败！", rf.me, peerIndex)
	return
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

func appendEntriesRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	//重试之前也要生成新的任务
	if rf.matchIndex[peerIndex] != -1 {
		//不是刚开始的backward阶段
		newTask := rf.generateNewTask(peerIndex, true)
		Assert(newTask != nil, "")
		rf.senderChannel[peerIndex] <- newTask
		Debug(dCommit, "leader：对 S%d发送日志log rpc失败,自动重试,新的日志为%+v", rf.me, peerIndex, *newTask)
	} else {
		rf.cleanupSenderChannelFor(peerIndex) //也可以清空发送队列,否则网络分区故障之后，会因为chan size不足而死锁
		req := args.(*AppendEntriesArgs)
		//重试不要通过返回true来实现，而要通过自己手动添加到chan来实现,这样就可以完成自己复制一个Task就不会完成任何冲突
		thisArgs := &AppendEntriesArgs{req.Term, req.Log, req.PrevLogIndex,
			req.PrevLogTerm, req.LeaderCommit, req.LeaderId}
		newTask := &Task{appendEntriesRpcFailureCallback,
			appendEntriesRpcSuccessCallback, thisArgs, &AppendEntriesReply{}, "Raft.AppendEntries"}
		rf.senderChannel[peerIndex] <- newTask
		Debug(dCommit, "leader：对 S%d发送日志log rpc失败,自动重试,日志不变，为%+v", rf.me, peerIndex, *newTask)
	}
	return
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
	} else {
		rf.backward(peerIndex, resp)
		Debug(dCommit, "接收到S%d返回，日志复制失败,修改matchIndex=%d，nextIndex=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], rf.nextIndex[peerIndex])
	}

	newTask := rf.generateNewTask(peerIndex, true)
	//这个阻塞过程会导致发送线程无法返回。。就导致无法广播心跳等,所以要异步执行
	for newTask == nil {
		if rf.state != LEADER {
			Debug(dCommit, "等待给S%d生成任务的过程中，我被降级为state=%d了！", rf.me, peerIndex, rf.state)
			return
		}

		//没有任务可以生成
		Debug(dCommit, "等待给S%d生成任务，matchIndex=%d，log len=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], len(rf.log))
		rf.logAppendCondition.Wait() //todo稍微等一会？当leader commit超过这个的时候，可以攒一波log

		newTask = rf.generateNewTask(peerIndex, true)
	}

	Debug(dCommit, "给S%d生成任务完成，task.args=", rf.me, peerIndex, *newTask.Args.(*AppendEntriesArgs))
	rf.senderChannel[peerIndex] <- newTask
}
