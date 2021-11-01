package kvraft

func (kv *KVServer) ClientRegister(args *ClientRegisterArgs, reply *ClientRegisterReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dServer, "S%d 接收到ClientRegister rpc", kv.me)

	op := &Op{RegisterType, "", ""}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)
	leaderId := kv.rf.GetLeaderId()

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		if leaderId != -1 {
			*reply = ClientRegisterReply{leaderId, -1, ErrWrongLeader}
		} else {
			*reply = ClientRegisterReply{leaderId, -1, ErrNoLeader}
		}
	} else {
		output := kv.waitFor(index)
		*reply = ClientRegisterReply{leaderId, output.Data.(int), OK}
	}
	Debug(dServer, "S%d ClientRegister rpc,返回 %+v", kv.me, *reply)
}

func (kv *KVServer) ClientQuery(args *ClientQueryArgs, reply *ClientQueryReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dServer, "S%d 接收到ClientQuery rpc,args=%+v", kv.me, *args)

	op := &Op{GetType, args.Key, ""}
	cmd := kv.buildCmd(op, -1, -1) //get不关心会话
	index, _, isLeader := kv.rf.Start(cmd)
	leaderId := kv.rf.GetLeaderId()

	if !isLeader {
		if leaderId != -1 {
			*reply = ClientQueryReply{leaderId, ErrWrongLeader, ""}
		} else {
			*reply = ClientQueryReply{leaderId, ErrNoLeader, ""}
		}
	} else {
		output := kv.waitFor(index)
		*reply = ClientQueryReply{leaderId, output.Status, output.Data.(string)}
	}
	Debug(dServer, "S%d ClientQuery rpc,返回  %+v", kv.me, *reply)
}

// ClientRequest 无论如何都提交日志，提交完状态机再考虑重复验证
func (kv *KVServer) ClientRequest(args *ClientRequestArgs, reply *ClientRequestReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dServer, "S%d 接收到ClientRequest rpc,args=%+v", kv.me, *args)

	op := &args.Op
	cmd := kv.buildCmd(op, args.ClientId, args.SequenceId)
	index, _, isLeader := kv.rf.Start(cmd)
	leaderId := kv.rf.GetLeaderId()

	if !isLeader {
		if leaderId != -1 {
			*reply = ClientRequestReply{leaderId, ErrWrongLeader}
		} else {
			*reply = ClientRequestReply{leaderId, ErrNoLeader}
		}
	} else {
		output := kv.waitFor(index)
		*reply = ClientRequestReply{leaderId, output.Status}
	}
	Debug(dServer, "S%d ClientRequest rpc,返回  %+v", kv.me, *reply)
}
