package shardkv

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.waitUtilInit()

	kv.mu.Lock()
	Debug(dServer, "G%d-S%d 接收到Get rpc,args=%+v,对应分片为%d", kv.gid, kv.me, *args, key2shard(args.Key))
	kv.getLatestConfig() //也需要等待，因为可能遇到没到当前最新config的情况，导致用别的config判断waitUntilReady。。
	kv.mu.Unlock()

	shard := key2shard(args.Key)
	ok := kv.waitUntilReady(shard) //因为会sleep
	Debug(dTrace, "G%d-S%d waitUntilReady 结束，当前version为%d,ready=%+v", kv.gid, kv.me, kv.Version, kv.Ready)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !ok {
		if !kv.isLeader() {
			*reply = GetReply{ErrWrongLeader, ""}
			Debug(dServer, "G%d-S%d Get rpc,返回 %+v", kv.gid, kv.me, *reply)
			return
		}
		if !kv.verifyKeyResponsibility(args.Key) {
			*reply = GetReply{ErrWrongGroup, ""}
			Debug(dServer, "G%d-S%d Get rpc,返回 %+v", kv.gid, kv.me, *reply)
			return
		}
	} else {
		isReady, ok := kv.Ready[shard]
		isLeader := kv.isLeader()
		isRespons := kv.verifyShardResponsibility(shard)
		_, has := kv.ShardMap[shard]
		Assert(ok && isReady && isLeader && isRespons && has, "")
	}

	op := &Op{GetType, args.Key, "", nil, -1, nil}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = GetReply{ErrWrongLeader, ""}
	} else {
		output := kv.waitFor(index)

		//因为重新获得锁了
		Debug(dServer, "G%d-S%d Get debug output = %+v", kv.gid, kv.me, output)
		if output.Err == ErrWrongLeader {
			*reply = GetReply{ErrWrongLeader, ""}
		} else if !kv.verifyKeyResponsibility(args.Key) {
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
	kv.waitUtilInit()

	kv.mu.Lock()
	Debug(dServer, "G%d-S%d 接收到PutAppend rpc,args=%+v,对应分片为%d", kv.gid, kv.me, *args, key2shard(args.Key))
	kv.getLatestConfig()
	kv.mu.Unlock()

	shard := key2shard(args.Key)
	ok := kv.waitUntilReady(shard)

	kv.mu.Lock()
	Debug(dTrace, "G%d-S%d PutAppend rpc,args=%+v： waitUntilReady 结束，当前version为%d,ready=%+v", kv.gid, kv.me, *args, kv.Version, kv.Ready)
	kv.mu.Unlock()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !ok {
		if !kv.isLeader() {
			*reply = PutAppendReply{ErrWrongLeader}
			Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
			return
		}
		if !kv.verifyKeyResponsibility(args.Key) {
			*reply = PutAppendReply{ErrWrongGroup}
			Debug(dServer, "G%d-S%d PutAppend rpc,args=%+v,返回 %+v", kv.gid, kv.me, *args, *reply)
			return
		}
	}
	op := &Op{PutType, args.Key, args.Value, nil, -1, nil}
	if args.Op == "Put" {
		op = &Op{PutType, args.Key, args.Value, nil, -1, nil}
	} else {
		op = &Op{AppendType, args.Key, args.Value, nil, -1, nil}
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
		} else if !kv.verifyKeyResponsibility(args.Key) {
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
	Debug(dServer, "G%d-S%d 接收到ReceiveShard rpc,before waitUtilInit,args=%+v", kv.gid, kv.me, *args)
	kv.waitUtilInit()

	Debug(dServer, "G%d-S%d 接收到ReceiveShard rpc,args=%+v", kv.gid, kv.me, *args)

	kv.getLatestConfig()
	//不拉取也行，有自动拉取的，因为收到的shard有version，在状态机中进行处理

	kv.mu.Lock()
	defer kv.mu.Unlock()

	isLeader := kv.isLeader()
	//version := kv.Version
	//kv.mu.Unlock()

	if !isLeader {
		//因为后面有assert，所以要提前判断是否是leader
		*reply = ReceiveShardReply{ErrWrongLeader}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	shard := args.Shard
	nextConfig := kv.QueryOrCached(shard.LastModifyVersion + 1)
	//nextNextConfig := kv.QueryOrCached(shard.LastModifyVersion + 2)
	Debug(dTrace, "G%d-S%d ReceiveShard rpc:get nextConfig,done!,config=%+v", kv.gid, kv.me, nextConfig)

	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	Assert(shard.LastModifyVersion < kv.Version, "")

	index := kv.processNextStep(shard, false)

	if index > 0 {
		isLeader := kv.isLeader()
		if !isLeader {
			*reply = ReceiveShardReply{ErrWrongLeader}
		} else {
			output := kv.waitFor(index)

			//因为重新获得锁了
			Debug(dServer, "G%d-S%d ReceiveShard debug output = %+v", kv.gid, kv.me, output)
			if output.Err == ErrWrongLeader {
				*reply = ReceiveShardReply{ErrWrongLeader}
			} else if output.Err == OK {
				*reply = ReceiveShardReply{OK}
			} else if output.Err == ErrRedirect {
				Debug(dTrace, "G%d-S%d WARN:拒绝：ReceiveShard rpc:进行 ErrRedirect，shard=%+v", kv.gid, kv.me, args.Shard)
				//redirect()
			} else {
				panic(1)
			}
		}
	} else {
		if index == -2 {
			//不是我，那就忽略
			*reply = ReceiveShardReply{ErrWrongGroup}
		} else {
			*reply = ReceiveShardReply{OK}
		}
	}

	Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
}
