package shardkv

//todo 分片迁移的内容是空值
//todo 状态机对shard的迁移的时候，如果这个shard已经存在？
//todo 增加序列号，保证不重复
//todo 速度很慢
//todo 给shard附上最后修改的版本号，多次接收的话，选取最大的进行覆盖；因为并发变更，可能一次发送完了，然后执行，然后第二次又发送了
//todo 添加多个发送线程，否则太慢了
//todo 多次添加重复的change config命令。。
//todo 记录所有历史版本
//todo 历史版本不持久化
//todo 发送时不会清空发送队列
//todo 要保证版本一下一下变，接收方接收到之后，检查last版本，然后获得他的下一个版本，如果就是我那就行，否则就转发这样增加last号
//todo condInstallSnapshot 有bug

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "6.824/labgob"

//检查leader都在rpc中，状态机只负责维护状态
func (kv *ShardKV) applier() {
	Debug(dServer, "G%d-S%d applier线程启动成功", kv.gid, kv.me)
	for op := range kv.applyCh {
		kv.mu.Lock()
		if op.SnapshotValid {
			Assert(!op.CommandValid, "")
			//读取快照
			//if kv.rf.CondInstallSnapshot(op.SnapshotTerm, op.SnapshotIndex, op.Snapshot) {
			//	Debug(dServer, "G%d-S%d 装载快照,快照index=%d，我的lastApplied=%d", kv.gid, kv.me, op.SnapshotIndex, kv.lastApplied)
			//	kv.readSnapshotPersist(op.Snapshot)
			//	kv.lastApplied = op.SnapshotIndex
			//} else {
			//	Debug(dServer, "G%d-S%d CondInstallSnapshot返回不用装载快照，快照index=%d，lastApplied=%d", kv.gid, kv.me, op.SnapshotIndex, kv.lastApplied)
			//}
			//直接判断即可，不用他的接口
			if kv.lastApplied < op.SnapshotIndex {
				Debug(dServer, "G%d-S%d 装载快照,快照index=%d，我的lastApplied=%d", kv.gid, kv.me, op.SnapshotIndex, kv.lastApplied)
				kv.readSnapshotPersist(op.Snapshot)
				kv.lastApplied = op.SnapshotIndex
			} else {
				Debug(dServer, "G%d-S%d CondInstallSnapshot返回不用装载快照，快照index=%d，lastApplied=%d", kv.gid, kv.me, op.SnapshotIndex, kv.lastApplied)
			}
		} else {
			cmd := op.Command.(Command)
			Assert(op.CommandValid, "")
			Debug(dMachine, "G%d-S%d 状态机开始执行命令%+v,index=%d", kv.gid, kv.me, cmd, op.CommandIndex)
			if op.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			Assert(op.CommandIndex == kv.lastApplied+1, fmt.Sprintf("lastApplied=%d,op=%+v \n", kv.lastApplied, op))

			switch cmd.Op.Type {
			case PutType:
				kv.put(op.CommandIndex, cmd)
				break
			case AppendType:
				kv.append(op.CommandIndex, cmd)
				break
			case GetType:
				kv.get(op.CommandIndex, cmd)
				break
			case ReceiveShard:
				kv.receiveShard(op.CommandIndex, cmd)
				break
			case DeleteShard:
				kv.deleteShard(op.CommandIndex, cmd)
				break
			case ChangeConfig:
				kv.changeConfig(op.CommandIndex, cmd)
				break
			default:
				panic(1)
			}
			kv.lastApplied++
			//判断当前的字节数是否太大了
			size := kv.persister.RaftStateSize()
			if kv.maxraftstate > 0 && size >= kv.maxraftstate {
				Debug(dServer, "G%d-S%d 发现state size=%d，而max state size=%d,所以创建快照", kv.gid, kv.me, size, kv.maxraftstate)
				kv.rf.Snapshot(kv.lastApplied, kv.constructSnapshot())
			}
			Debug(dMachine, "G%d-S%d 状态机执行命令%+v结束，结果为%+v,更新lastApplied=%d", kv.gid, kv.me, cmd, kv.output[op.CommandIndex], kv.lastApplied)
		}
		kv.commitIndexCond.Broadcast() //装载快照的话，lastApplied也会变
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) changeConfig(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行changeConfig命令,index=%d,config=%+v,AlwaysMe=%+v",
		kv.gid, kv.me, CommandIndex, *command.Op.Config)
	//似乎没必要检验getLatestConfig，毕竟肯定要changeConfig
	if command.Op.Config.Num <= kv.Version {
		Debug(dMachine, "G%d-S%d 执行changeConfig命令，changeConfig失败，操作数的config=%d比我的=%d小",
			kv.gid, kv.me, command.Op.Config.Num, kv.Version)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "changeConfig失败，操作数的config比我的小"}
		return
	} else if kv.isLeader() {
		kv.output[CommandIndex] = &StateMachineOutput{OK, "我是leader，状态机跳过执行changeConfig命令！"}
		Debug(dMachine, "G%d-S%d 我是leader，状态机跳过执行changeConfig命令！", kv.gid, kv.me)
	} else if !kv.checkDuplicate(CommandIndex, command) {
		//只有在状态机执行状态转换的时候，才会发送shard，否则可能出现发送脏数据的问题 fixme
		kv.changeConfigUtil(*command.Op.Config)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "changeConfig成功！"}
	}
	kv.MachineVersion = raft.Max(kv.MachineVersion, command.Op.Config.Num)
	Debug(dMachine, "G%d-S%d 更新MachineVersion=%d", kv.gid, kv.me, kv.MachineVersion)
}

//注意：重启的时候会重新执行commit的，就导致会出现不负责之类的情况
func (kv *ShardKV) receiveShard(CommandIndex int, command Command) {
	shardId := command.Op.ShardId
	shard := CopyShard(*command.Op.Shard)
	_, exists := kv.ShardMap[shardId]

	Debug(dMachine, "G%d-S%d 执行receiveShard命令,index=%d,shard=%+v", kv.gid, kv.me, CommandIndex, *command.Op.Shard)
	kv.getLatestConfig() //fixme
	//必须检验，很可能多次接收
	if kv.checkDuplicate(CommandIndex, command) {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shardId=%d,重复receive！", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "重复receive！"}
		return
	}
	//校验我是否负责
	now := kv.QueryOrCached(shard.LastModifyVersion)
	if now.Shards[shardId] != kv.gid {
		panic(fmt.Sprintf("G%d-S%d 执行receiveShard命令,version=%d,shard=%+v，now=%+v,gid=%+v", kv.gid, kv.me,
			kv.Version, *command.Op.Shard, now, kv.gid))
	}

	//无脑接收这个shard
	if exists {
		myShardVersion := kv.ShardMap[shardId].LastModifyVersion
		theirShardVersion := command.Op.Shard.LastModifyVersion
		if myShardVersion < theirShardVersion { //不能有等于号
			//比我的新
			kv.ShardMap[shardId] = shard
			kv.output[CommandIndex] = &StateMachineOutput{OK, "我负责！+版本号比我大，更新！"}
		} else {
			kv.output[CommandIndex] = &StateMachineOutput{OK, "我负责！+版本号比我小于等于，abort！"} //必须是ok，引导发送发删除shard
		}
	} else {
		kv.ShardMap[shardId] = shard
		kv.output[CommandIndex] = &StateMachineOutput{OK, "我负责！+不存在，添加！"}
	}
	Assert(kv.Version >= shard.LastModifyVersion, "")
	if shard.LastModifyVersion == kv.Version {
		//下一个就是我，ok
		myShard := kv.ShardMap[shardId]
		kv.Ready[shardId] = true
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令接收到shard id=%d version=%d，我的version=%d，所以更新为ready！,ready=%+v,shard map=%+v",
			kv.gid, kv.me, shardId, myShard.LastModifyVersion, kv.Version, kv.Ready, kv.ShardMap)
	} else {
		//也可以不发送，会有线程自动扫描的。。
		kv.output[CommandIndex] = &StateMachineOutput{ErrRedirect,
			"可能出现在rpc handler中是+1，但是到这里就不是了。。因为是异步的，此时返回ErrRedirect"}
	}
	//可能出现在rpc handler中是+1，但是到这里就不是了。。因为是异步的，此时返回ErrRedirect

	Debug(dMachine, "G%d-S%d 执行receiveShard命令,ok,shardId=%d", kv.gid, kv.me, shardId)
}

func (kv *ShardKV) deleteShard(CommandIndex int, command Command) {
	shardId := command.Op.ShardId

	Debug(dMachine, "G%d-S%d 执行deleteShard命令,index=%d,shardId=%d,shard=%+v", kv.gid, kv.me, CommandIndex, shardId, command.Op.Shard)
	//检验了也没啥用，因为是自己提交的，clientId=-1
	if kv.checkDuplicate(CommandIndex, command) {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shardId=%d,重复delete！", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "重复delete！"}
		return
	}

	if kv.Config.Shards[shardId] == kv.gid && command.Op.Shard.LastModifyVersion == kv.Version {
		//我不负责就对了，因为是先改变config再删除的。。
		//即现在我负责的，不能删除，但是曾经我负责的，可以删除
		Debug(dMachine, "G%d-S%d 执行deleteShard命令，shardId=%d,这个shard我负责,而且last version=my version，不能删除，abort", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "我负责！，不能删除！"}
		return
	}

	if _, ok := kv.ShardMap[shardId]; !ok {
		//可能会发生重复删除
		Debug(dMachine, "G%d-S%d WARN：执行deleteShard命令，shardId=%d,这个shard不存在！", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "不存在!"}
		return
	}

	delete(kv.ShardMap, shardId)

	Debug(dMachine, "G%d-S%d 执行deleteShard命令,ok", kv.gid, kv.me)
	kv.output[CommandIndex] = &StateMachineOutput{OK, "delete!"}
}

func (kv *ShardKV) put(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行put命令,index=%d,cmd=%+v", kv.gid, kv.me, CommandIndex, command)
	if !kv.verifyKeyResponsibility(command.Op.Key) {
		Debug(dMachine, "G%d-S%d WARN：执行put命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid,
			kv.me, key2shard(command.Op.Key), kv.Config.Shards[key2shard(command.Op.Key)])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, ""}
		return
	}
	shard := key2shard(command.Op.Key)

	if !kv.isReady(shard) {
		if kv.isLeader() {
			Debug(dMachine, "G%d-S%d WARN：解决waitUntil bug的重试。。", kv.gid, kv.me)
			kv.output[CommandIndex] = &StateMachineOutput{ErrWrongLeader, "解决waitUntil bug的重试。。"}
		}
		return
	}

	state := kv.ShardMap[shard].State

	if !kv.checkDuplicate(CommandIndex, command) {
		state[command.Op.Key] = command.Op.Value
		kv.output[CommandIndex] = &StateMachineOutput{OK, command.Op.Value}
	}

}

func (kv *ShardKV) get(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行get命令,index=%d,cmd=%+v", kv.gid, kv.me, CommandIndex, command)

	if !kv.verifyKeyResponsibility(command.Op.Key) {
		Debug(dMachine, "G%d-S%d WARN：执行get命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid,
			kv.me, key2shard(command.Op.Key), kv.Config.Shards[key2shard(command.Op.Key)])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, ""}
		return
	}
	shard := key2shard(command.Op.Key)
	state := kv.ShardMap[shard].State
	if v, ok := state[command.Op.Key]; ok {
		Debug(dMachine, "G%d-S%d 执行get命令,value=%s", kv.gid, kv.me, v)
		kv.output[CommandIndex] = &StateMachineOutput{OK, v}
	} else {
		Debug(dMachine, "G%d-S%d 执行get命令,key不存在", kv.gid, kv.me, CommandIndex)
		kv.output[CommandIndex] = &StateMachineOutput{ErrNoKey, ""}
	}
}

func (kv *ShardKV) append(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行append命令,index=%d,cmd=%+v", kv.gid, kv.me, CommandIndex, command)

	if !kv.verifyKeyResponsibility(command.Op.Key) {
		Debug(dMachine, "G%d-S%d WARN：执行append命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid,
			kv.me, key2shard(command.Op.Key), kv.Config.Shards[key2shard(command.Op.Key)])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, ""}
		return
	}

	//判断这个key存不存在，因为waitUntilReady之后，异步可能改变map，delete了
	shard := key2shard(command.Op.Key)
	if !kv.isReady(shard) {
		if kv.isLeader() {
			Debug(dMachine, "G%d-S%d WARN：解决waitUntil bug的重试。。", kv.gid, kv.me)
			kv.output[CommandIndex] = &StateMachineOutput{ErrWrongLeader, "解决waitUntil bug的重试。。"}
		}
		return
	}

	state := kv.ShardMap[shard].State
	if !kv.checkDuplicate(CommandIndex, command) {
		if v, ok := state[command.Op.Key]; ok {
			Debug(dMachine, "G%d-S%d 执行append命令前,value=%s", kv.gid, kv.me, v)
			state[command.Op.Key] = v + command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, state[command.Op.Key]}
		} else {
			Debug(dMachine, "G%d-S%d 执行append命令,key不存在,自动创建", kv.gid, kv.me, CommandIndex)
			state[command.Op.Key] = command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, state[command.Op.Key]}
		}
	}
	Debug(dMachine, "G%d-S%d 执行append命令结束,value=%s", kv.gid, kv.me, state[command.Op.Key])

}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	Debug(dServer, "G%d-S%d 被kill", kv.gid, kv.me)
	//kv.sendShards2Channel() //?
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	//close(kv.migrationChan)
	//close(kv.applyCh)
	//Debug(dServer, "G%d-S%d 被kill结束", kv.gid, kv.me)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	InitLog()

	labgob.Register(Op{})
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.persister = persister

	kv.n = len(servers)

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mu = raft.NewReentrantLock()
	kv.commitIndexCond = sync.NewCond(kv.mu)

	kv.migrationChan = make(chan *Task, 100)

	kv.lastApplied = 0
	kv.output = make(map[int]*StateMachineOutput)
	kv.ShardMap = map[int]Shard{}
	kv.Version = -1
	kv.Ready = map[int]bool{}
	kv.QueryCache = map[int]shardctrler.Config{}
	kv.lastSentId = make([]int, NShards)
	raft.SetArrayValue(kv.lastSentId, -1)
	//启动时创建自己负责的
	//fixme 如果是迁移失败，重启之后呢？

	go kv.fetchConfigThread()
	for i := 0; i < 5; i++ {
		go kv.shardSenderThread()
	}
	go kv.applier()
	go func() {
		//Debug(dTrace, "G%d-S%d 启动sendShards2Channel 扫描线程", kv.gid, kv.me)
		for !kv.killed() {
			time.Sleep(time.Duration(200) * time.Millisecond)
			kv.sendShards2Channel()
		}
		//Debug(dTrace, "G%d-S%d 关闭sendShards2Channel 扫描线程", kv.gid, kv.me)
	}()

	go func() {
		for !kv.killed() {
			time.Sleep(time.Duration(100) * time.Millisecond) //每隔一段时间唤醒一次，防止，因为网络分区导致死锁，见md
			kv.commitIndexCond.Broadcast()
		}
	}()

	return kv
}
