package raft

//只有rpc通信是并行的
func voteRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d -> S%d 选举 RPC失败，不重试 state=%d", rf.me, rf.me, peerIndex, rf.state)
	rf.doneRPCs++ //return false才这样！
	rf.broadCastCondition.Broadcast()
}

func voteRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	rf.broadCastCondition.Broadcast()
}

func heartBeatRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	Debug(dLeader, "leader：对 S%d发送心跳rpc失败！", rf.me, peerIndex)
}

//这里，假如选为leader之后，在init的地方for循环多次发送的话，因为外部加的是for循环整体的锁，而回调函数需要锁，所以阻塞了所有的回调函数，导致发送队列阻塞
func heartBeatRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
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
		newTask := rf.generateNewTask(peerIndex)
		Assert(newTask != nil, "")
		rf.senderChannel[peerIndex] <- newTask
		Debug(dCommit, "leader：对 S%d发送日志log rpc失败,自动重试,新的日志为%s,%+v", rf.me, peerIndex, newTask.RpcMethod, newTask.Args)
	} else {

		req := args.(*AppendEntriesArgs)
		//重试不要通过返回true来实现，而要通过自己手动添加到chan来实现,这样就可以完成自己复制一个Task就不会完成任何冲突
		thisArgs := &AppendEntriesArgs{req.Term, req.Log, req.PrevLogIndex,
			req.PrevLogTerm, req.LeaderCommit, req.LeaderId}
		newTask := &Task{appendEntriesRpcFailureCallback,
			appendEntriesRpcSuccessCallback, thisArgs, &AppendEntriesReply{}, "Raft.AppendEntries"}
		rf.senderChannel[peerIndex] <- newTask
		Debug(dCommit, "leader：对 S%d发送日志log rpc失败,自动重试,日志不变，为%s,%+v", rf.me, peerIndex, newTask.RpcMethod, newTask.Args)
	}
}

func appendEntriesRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
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
			//nextIndex check
			rf.nextIndex[peerIndex] = reqMaxIndex + 1 //有意义才更新，避免直接更新nextIndex导致指针后退!!
		}

		Debug(dCommit, "接收到S%d返回，日志复制成功,修改matchIndex=%d，nextIndex=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], rf.nextIndex[peerIndex])
	} else {
		rf.backward(peerIndex, resp)
		Debug(dCommit, "接收到S%d返回，日志复制失败,修改matchIndex=%d，nextIndex=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], rf.nextIndex[peerIndex])
	}

	newTask := rf.generateNewTask(peerIndex) //可能接受到快照任务
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

		newTask = rf.generateNewTask(peerIndex)
	}

	Debug(dCommit, "给S%d生成任务完成，task.args=%+v", rf.me, peerIndex, newTask.Args)
	rf.senderChannel[peerIndex] <- newTask
}

func snapshotRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "对S%d发送快照失败,自动重试", rf.me, peerIndex)

	req := args.(*InstallSnapshotArgs)

	//注意使用最新的快照，而不要直接复制一份
	thisArgs := &InstallSnapshotArgs{rf.term, rf.LeaderId, rf.snapshot, req.Done,
		req.Offset, rf.snapshotIndex, rf.snapshotMachineIndex,
		rf.snapshotTerm}

	newTask := &Task{snapshotRpcFailureCallback, snapshotRpcSuccessCallback,
		thisArgs, &InstallSnapshotReply{}, "Raft.InstallSnapshot"}
	rf.senderChannel[peerIndex] <- newTask
	Debug(dCommit, "leader：对S%d发送快照失败,自动重试,args，为%+v", rf.me, peerIndex, newTask.Args)
}

func snapshotRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	resp := reply.(*InstallSnapshotReply)
	req := args.(*InstallSnapshotArgs)
	if resp.Term > rf.term {
		Debug(dTerm, "快照rpc： 接收到S%d返回，但是term大于当前服务器S%d,被降级", rf.me, peerIndex, rf.me)
		rf.increaseTerm(resp.Term, -1)
		//都被降级了，直接返回就行了,AE rpc需要backward，所以不返回
		return
	}

	Debug(dSnap, "快照rpc： 对S%d发送快照成功,自动生成AE rpc", rf.me, peerIndex)
	//更新matchIndex,nextIndex
	if rf.matchIndex[peerIndex] < req.LastIncludedIndex {
		rf.matchIndex[peerIndex] = req.LastIncludedIndex
		//rf.LeaderUpdateCommitIndex() 不需要更新，因为snapshot肯定低于commitIndex
		rf.nextIndex[peerIndex] = req.LastIncludedIndex + 1 //有意义才更新
	}

	//做法相同。。
	//newTask还可能是snapshot!!
	newTask := rf.generateNewTask(peerIndex)
	//这个阻塞过程会导致发送线程无法返回。。就导致无法广播心跳等,所以要异步执行
	for newTask == nil {
		if rf.state != LEADER {
			Debug(dCommit, "快照rpc： 等待给S%d生成任务的过程中，我被降级为state=%d了！", rf.me, peerIndex, rf.state)
			return
		}

		//没有任务可以生成
		Debug(dCommit, "快照rpc： 等待给S%d生成任务，matchIndex=%d，log len=%d", rf.me, peerIndex,
			rf.matchIndex[peerIndex], len(rf.log))
		rf.logAppendCondition.Wait() //todo稍微等一会？当leader commit超过这个的时候，可以攒一波log

		newTask = rf.generateNewTask(peerIndex)
	}

	Debug(dCommit, "快照rpc： 给S%d生成任务完成，task.args=%+v", rf.me, peerIndex, newTask.Args)
	rf.senderChannel[peerIndex] <- newTask
}
