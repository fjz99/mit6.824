package raft

//go语言接口断言
//只有rpc通信是并行的
func voteRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}, counter *int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//req := args.(*RequestVoteArgs

	Debug(dVote, "S%d -> S%d 选举 RPC失败，不重试 state=%d", rf.me, rf.me, peerIndex, rf.state)
	rf.doneRPCs++ //return false才这样！
	rf.broadCastCondition.Broadcast()
	//rf.waitGroup.Done()
	return false
}

func voteRpcSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//req := args.(*RequestVoteArgs)
	resp := reply.(*RequestVoteReply)
	rf.doneRPCs++
	if resp.Term > rf.term {
		//转为follower
		Debug(dVote, "接收到S%d返回，但是term大于当前服务器S%d", rf.me, peerIndex, rf.me)
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

func heartBeatRpcFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}, counter *int) bool {
	Debug(dLeader, "leader：对 S%d发送心跳rpc失败！", rf.me, peerIndex)
	return false
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
