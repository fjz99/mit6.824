package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	Debug(dServer, "G%d-S%d 接收到Get rpc,args=%+v,对应分片为%d", kv.gid, kv.me, *args, key2shard(args.Key))
	shard := key2shard(args.Key)
	kv.waitUntilReady(shard, args.Version) //因为会sleep

	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dTrace, "G%d-S%d waitUntilReady 结束，当前version为%d,status=%+v", kv.gid, kv.me, kv.Version, kv.ShardStatus)

	if !kv.isLeader() {
		*reply = GetReply{ErrWrongLeader, "fast"}
		Debug(dServer, "G%d-S%d Get rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}
	if !kv.verifyKeyResponsibility(args.Key) {
		*reply = GetReply{ErrWrongGroup, ""}
		Debug(dServer, "G%d-S%d Get rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	//if kv.Version != args.Version {
	//	Assert(kv.Version < args.Version, "")
	//	*reply = GetReply{ErrOutdated, ""}
	//	Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
	//	return
	//}

	op := &Op{GetType, args.Key, "", nil, -1, nil, -1}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = GetReply{ErrWrongLeader, ""}
	} else {
		output := kv.waitFor(index)

		//因为重新获得锁了
		Debug(dServer, "G%d-S%d Get debug output = %+v", kv.gid, kv.me, output)
		if output.Err == ErrWrongLeader {
			*reply = GetReply{ErrWrongLeader, "slow"}
		} else if output.Err == ErrWrongGroup {
			*reply = GetReply{ErrWrongGroup, ""}
		} else if output.Err == OK {
			*reply = GetReply{OK, output.Data.(string)}
		} else if output.Err == ErrNoKey {
			*reply = GetReply{ErrNoKey, ""}
		} else {
			panic(1)
		}
	}
	Debug(dServer, "G%d-S%d Get rpc,,对应分片为%d,返回 %+v", kv.gid, kv.me, key2shard(args.Key), *reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	Debug(dServer, "G%d-S%d 接收到PutAppend rpc,args=%+v,对应分片为%d", kv.gid, kv.me, *args, key2shard(args.Key))
	shard := key2shard(args.Key)
	kv.waitUntilReady(shard, args.Version)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dTrace, "G%d-S%d PutAppend rpc,args=%+v： waitUntilReady 结束，当前version为%d,status=%+v", kv.gid, kv.me, *args, kv.Version, kv.ShardStatus)

	if !kv.isLeader() {
		*reply = PutAppendReply{ErrWrongLeader}
		Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
		return
	}
	//if kv.Version != args.Version {
	//	Assert(kv.Version < args.Version, "")
	//	*reply = PutAppendReply{ErrOutdated}
	//	Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
	//	return
	//}
	if !kv.verifyKeyResponsibility(args.Key) {
		*reply = PutAppendReply{ErrWrongGroup}
		Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
		return
	}
	//if kv.Version != args.Version {
	//	Assert(kv.Version < args.Version, "")
	//	*reply = PutAppendReply{ErrOutdated}
	//	Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
	//	return
	//}
	op := &Op{PutType, args.Key, args.Value, nil, -1, nil, -1}
	if args.Op == "Put" {
		op = &Op{PutType, args.Key, args.Value, nil, -1, nil, -1}
	} else {
		op = &Op{AppendType, args.Key, args.Value, nil, -1, nil, -1}
	}

	cmd := kv.buildCmd(op, args.ClientId, args.SequenceId)
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		*reply = PutAppendReply{ErrWrongLeader}
	} else {
		output := kv.waitFor(index)

		//因为重新获得锁了
		Debug(dServer, "G%d-S%d PutAppend debug output = %+v", kv.gid, kv.me, output)
		if output.Err == ErrWrongLeader {
			*reply = PutAppendReply{ErrWrongLeader}
		} else if output.Err == ErrWrongGroup {
			*reply = PutAppendReply{ErrWrongGroup}
		} else if output.Err == OK {
			*reply = PutAppendReply{OK}
		} else {
			panic(1)
		}
	}
	Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,对应分片为%d,返回 %+v", kv.gid, kv.me, *args, key2shard(args.Key), *reply)
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) {
	Debug(dServer, "G%d-S%d 接收到ReceiveShard rpc,args=%+v", kv.gid, kv.me, *args)
	kv.mu.Lock()
	shard := args.Shard
	isLeader := kv.isLeader()
	version := kv.Version
	myconfig := kv.Config
	status := kv.copyShardStatus()
	kv.mu.Unlock()

	if !isLeader {
		*reply = ReceiveShardReply{ErrWrongLeader}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	if args.Version > version {
		*reply = ReceiveShardReply{ErrNotReady}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	if args.Version < version {
		*reply = ReceiveShardReply{ErrOutdated}
		//这里需要返回ok，因为存在一个case：主节点挂了，从节点变成主节点，从节点是OUT，进行重发，但是这个group的节点已经进入了下一个version。。
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	if myconfig.Shards[shard.Id] != kv.gid {
		*reply = ReceiveShardReply{ErrWrongGroup}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	//version相同了
	if status[shard.Id] != IN {
		Assert(status[shard.Id] == READY, "")
		*reply = ReceiveShardReply{OK}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	index, _ := kv.submitNewReceiveLog(shard, args.Version)

	kv.mu.Lock()
	output := kv.waitFor(index)
	kv.mu.Unlock()

	Debug(dServer, "G%d-S%d ReceiveShard debug output = %+v", kv.gid, kv.me, output)
	if output.Err == ErrWrongLeader {
		*reply = ReceiveShardReply{ErrWrongLeader}
	} else if output.Err == OK {
		*reply = ReceiveShardReply{OK}
		//可能会发生ErrWrongGroup
	} else if output.Err == ErrWrongGroup || output.Err == ErrNotReady {
		panic(1)
	} else {
		//可能2个请求一起到了，然后就导致第二个过时了，因为version++了
		*reply = ReceiveShardReply{ErrOutdated}
	}

	Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
}
