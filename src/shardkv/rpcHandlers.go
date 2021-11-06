package shardkv

import "fmt"

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.waitUtilInit()

	kv.mu.Lock()
	Debug(dServer, "G%d-S%d 接收到Get rpc,args=%+v", kv.gid, kv.me, *args)
	//kv.getLatestConfig(false) //也需要等待，因为可能遇到没到当前最新config的情况，导致用别的config判断waitUntilReady。。
	kv.mu.Unlock()

	shard := key2shard(args.Key)
	ok := kv.waitUntilReady(shard) //因为会sleep
	Debug(dTrace, "G%d-S%d waitUntilReady 结束，当前version为%d", kv.gid, kv.me, kv.Version)

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
		} else {
			panic(1)
		}
	}
	Debug(dServer, "G%d-S%d Get rpc,返回 %+v", kv.gid, kv.me, *reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.waitUtilInit()

	kv.mu.Lock()
	Debug(dServer, "G%d-S%d 接收到PutAppend rpc,args=%+v", kv.gid, kv.me, *args)
	//kv.getLatestConfig(false)
	kv.mu.Unlock()

	shard := key2shard(args.Key)
	ok := kv.waitUntilReady(shard)
	Debug(dTrace, "G%d-S%d waitUntilReady 结束，当前version为%d", kv.gid, kv.me, kv.Version)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !ok {
		if !kv.isLeader() {
			*reply = PutAppendReply{ErrWrongLeader}
			Debug(dServer, "G%d-S%d PutAppend rpc,返回 %+v", kv.gid, kv.me, *reply)
			return
		}
		if !kv.verifyKeyResponsibility(args.Key) {
			*reply = PutAppendReply{ErrWrongGroup}
			Debug(dServer, "G%d-S%d PutAppend rpc,返回 %+v", kv.gid, kv.me, *reply)
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
	Debug(dServer, "G%d-S%d PutAppend rpc,返回 %+v", kv.gid, kv.me, *reply)
}

func (kv *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) {
	kv.waitUtilInit()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	Debug(dServer, "G%d-S%d 接收到ReceiveShard rpc,args=%+v", kv.gid, kv.me, *args)

	kv.getLatestConfig(false)

	if !kv.isLeader() {
		//因为后面有assert，所以要提前判断是否是leader
		*reply = ReceiveShardReply{ErrWrongLeader, kv.Version}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}
	if !kv.verifyShardResponsibility(args.Shard.Id) {
		*reply = ReceiveShardReply{ErrWrongGroup, kv.Version}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}
	if args.Version < kv.Version {
		*reply = ReceiveShardReply{ErrVersion, kv.Version}
		Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
		return
	}

	Assert(args.Version == kv.Version, fmt.Sprintf("args=%+v，my version=%d", *args, kv.Version)) //fixme !

	op := &Op{ReceiveShard, "", "", &args.Shard, args.Shard.Id, nil}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader { //这样会导致raft多次打印”我不是leader“的日志
		*reply = ReceiveShardReply{ErrWrongLeader, kv.Version}
	} else {
		output := kv.waitFor(index)

		//因为重新获得锁了
		Debug(dServer, "G%d-S%d ReceiveShard debug output = %+v", kv.gid, kv.me, output)
		if output.Err == ErrWrongLeader {
			*reply = ReceiveShardReply{ErrWrongLeader, kv.Version}
		} else if !kv.verifyShardResponsibility(args.Shard.Id) {
			*reply = ReceiveShardReply{ErrWrongGroup, kv.Version}
		} else if output.Err == OK {
			*reply = ReceiveShardReply{OK, kv.Version}
		} else if output.Err == ErrVersion {
			*reply = ReceiveShardReply{ErrVersion, kv.Version}
		} else {
			panic(1)
		}
	}

	Debug(dServer, "G%d-S%d ReceiveShard rpc,返回 %+v", kv.gid, kv.me, *reply)
}
