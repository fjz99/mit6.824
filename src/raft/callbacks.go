package raft

//go语言接口断言
//只有rpc通信是并行的
func voteFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dVote, "S%d -> S%d RPC失败，不重试", rf.me, G(rf.state), rf.me, peerIndex)
	//req := args.(*RequestVoteArgs)
	//resp := reply.(*RequestVoteReply)
	//*resp = RequestVoteReply{}
	rf.doneRPCs++ //return false才这样！
	rf.broadCastCondition.Broadcast()
	//rf.waitGroup.Done()
	return false
}

func voteSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//req := args.(*RequestVoteArgs)
	resp := reply.(*RequestVoteReply)
	rf.doneRPCs++
	if resp.Term > rf.term {
		//转为follower
		Debug(dVote, "接收到S%d返回，但是term大于当前服务器S%d", rf.me, G(rf.state), peerIndex, rf.me)
		rf.increaseTerm(resp.Term)
	}
	//验证投票同意与否
	if resp.VoteGranted {
		rf.agreeCounter++
		Debug(dVote, "接收到S%d返回同意投票", rf.me, G(rf.state), peerIndex, rf.me)
	} else {
		Debug(dVote, "接收到S%d返回拒绝！！投票", rf.me, G(rf.state), peerIndex, rf.me)
	}
	//rf.waitGroup.Done()
	rf.broadCastCondition.Broadcast()
}

//todo 心跳不足则leader降级
func heartBeatFailureCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) bool {
	Debug(dLeader, "leader：对 S%d发送心跳失败！", rf.me, G(rf.state), peerIndex)
	return false
}

func heartBeatSuccessCallback(peerIndex int, rf *Raft, args interface{}, reply interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp := reply.(*AppendEntriesReply)
	Assert(resp.Success, "")
	if resp.Term > rf.term {
		Debug(dLeader, "接收到S%d返回，但是term大于当前服务器S%d,被降级", rf.me, G(rf.state), peerIndex, rf.me)
		rf.increaseTerm(resp.Term)
	}
}
