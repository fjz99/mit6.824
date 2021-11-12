package shardctrler

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	Debug(dServer, "S%d 接收到Join rpc", sc.me)
	op := &Op{Type: JOIN, Servers: args.Servers}
	cmd := sc.buildCmd(op, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(cmd)

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = JoinReply{true, ErrWrongLeader}
	} else {
		output := sc.waitFor(index)
		Debug(dServer, "S%d Join output = %+v", sc.me, output)
		if output.Err == ErrWrongLeader {
			*reply = JoinReply{true, ErrWrongLeader}
		} else {
			*reply = JoinReply{false, OK}
		}
	}
	Debug(dServer, "S%d Join rpc,返回 %+v", sc.me, *reply)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	Debug(dServer, "S%d 接收到Leave rpc", sc.me)
	op := &Op{Type: LEAVE, GIDs: args.GIDs}
	cmd := sc.buildCmd(op, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(cmd)

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = LeaveReply{true, ErrWrongLeader}
	} else {
		output := sc.waitFor(index)
		Debug(dServer, "S%d Leave output = %+v", sc.me, output)
		if output.Err == ErrWrongLeader {
			*reply = LeaveReply{true, ErrWrongLeader}
		} else {
			*reply = LeaveReply{false, OK}
		}
	}
	Debug(dServer, "S%d Leave rpc,返回 %+v", sc.me, *reply)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	Debug(dServer, "S%d 接收到Move rpc", sc.me)
	op := &Op{Type: MOVE, GID: args.GID, Shard: args.Shard}
	cmd := sc.buildCmd(op, args.ClientId, args.SeqId)
	index, _, isLeader := sc.rf.Start(cmd)

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = MoveReply{true, ErrWrongLeader}
	} else {
		output := sc.waitFor(index)
		Debug(dServer, "S%d Move output = %+v", sc.me, output)
		if output.Err == ErrWrongLeader {
			*reply = MoveReply{true, ErrWrongLeader}
		} else {
			*reply = MoveReply{false, OK}
		}
	}
	Debug(dServer, "S%d Move rpc,返回 %+v", sc.me, *reply)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	Debug(dServer, "S%d 接收到Query rpc", sc.me)
	//简单处理，否则不快照的话，会因为query太多而err
	if sc.rf.GetLeaderId() == sc.me {
		if args.Num == -1 {
			*reply = QueryReply{false, OK, sc.configs[len(sc.configs)-1]}
		}
		if len(sc.configs) <= args.Num && args.Num != -1 {
			*reply = QueryReply{false, OK, Config{-1, [NShards]int{}, map[int][]string{}}}
		}
		if len(sc.configs) > args.Num && args.Num != -1 {
			*reply = QueryReply{false, OK, sc.configs[args.Num]}
		}
	} else {
		*reply = QueryReply{true, ErrWrongLeader, Config{}}
	}

	//op := &Op{Type: QUERY, Num: args.Num}
	//cmd := sc.buildCmd(op, -1, -1)
	//index, _, isLeader := sc.rf.Start(cmd)

	//if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
	//	*reply = QueryReply{true, ErrWrongLeader, Config{}}
	//} else {
	//	output := sc.waitFor(index)
	//	Debug(dServer, "S%d Query output = %+v", sc.me, output)
	//	if output.Err == ErrWrongLeader {
	//		*reply = QueryReply{true, ErrWrongLeader, Config{}}
	//	} else {
	//		*reply = QueryReply{false, OK, output.Data.(Config)}
	//	}
	//}
	Debug(dServer, "S%d Query rpc,返回 %+v", sc.me, *reply)
}

func (sc *ShardCtrler) ClientRegister(args *ClientRegisterArgs, reply *ClientRegisterReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	Debug(dServer, "S%d 接收到ClientRegister rpc", sc.me)

	op := &Op{Type: REGISTER}
	cmd := sc.buildCmd(op, -1, -1)
	index, _, isLeader := sc.rf.Start(cmd)
	leaderId := sc.rf.GetLeaderId()

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = ClientRegisterReply{leaderId, -1, ErrWrongLeader}
	} else {
		output := sc.waitFor(index)

		Debug(dServer, "S%d register debug output = %+v", sc.me, output)
		if output.Err == ErrWrongLeader {
			*reply = ClientRegisterReply{sc.rf.GetLeaderId(), -1, ErrWrongLeader}
		} else {
			*reply = ClientRegisterReply{leaderId, output.Data.(int), OK}
		}
	}
	Debug(dServer, "S%d ClientRegister rpc,返回 %+v", sc.me, *reply)
}
