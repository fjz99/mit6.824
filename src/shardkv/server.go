package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"sync"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "6.824/labgob"

//todo 打包发送，一次发送所有的OUT shard
//todo 尝试delete同步提交

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
			Debug(dMachine, "G%d-S%d 状态机开始执行命令%+v,index=%d,status=%+v", kv.gid, kv.me, cmd, op.CommandIndex, kv.ShardStatus)
			if op.CommandIndex <= kv.lastApplied {
				kv.checkSnapshot()
				kv.mu.Unlock()
				continue
			}
			//Assert(op.CommandIndex == kv.lastApplied+1, fmt.Sprintf("lastApplied=%d,op=%+v \n", kv.lastApplied, op))

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
			kv.checkSnapshot()
			Debug(dMachine, "G%d-S%d 状态机执行命令%+v结束，结果为%+v,更新lastApplied=%d,status=%+v", kv.gid, kv.me, cmd, kv.output[op.CommandIndex], kv.lastApplied, kv.ShardStatus)
		}
		kv.commitIndexCond.Broadcast() //装载快照的话，lastApplied也会变
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) changeConfig(CommandIndex int, command Command) {
	newConfig := command.Op.Config
	Debug(dMachine, "G%d-S%d 执行changeConfig命令,index=%d,newConfig=%+v,status=%+v", kv.gid, kv.me, CommandIndex, *newConfig, kv.ShardStatus)

	if newConfig.Num != kv.Version+1 {
		Debug(dMachine, "G%d-S%d 执行changeConfig命令，changeConfig失败，操作数的config=%d ！= 我的=%d +1",
			kv.gid, kv.me, newConfig.Num, kv.Version)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "changeConfig失败，操作数的config！= 我的+1"}
		return
	} else {
		//都加一了，肯定不重复。。
		kv.setNewStatus(kv.Config, *newConfig)
		kv.setNewConfig(*newConfig)
		if newConfig.Num == 1 {
			//初始化
			for i := 0; i < NShards; i++ {
				if newConfig.Shards[i] == kv.gid {
					kv.ShardMap[i] = Shard{Id: i, State: map[string]string{}, Session: map[int64]int{}}
					kv.ShardStatus[i] = READY
				}
			}
		}
		kv.output[CommandIndex] = &StateMachineOutput{OK, "changeConfig成功！"}
	}
}

//注意：重启的时候会重新执行commit的，就导致会出现不负责之类的情况
func (kv *ShardKV) receiveShard(CommandIndex int, command Command) {
	shardId := command.Op.ShardId
	shard := CopyShard(*command.Op.Shard)
	_, exists := kv.ShardMap[shardId]
	theirVersion := command.Op.TheirVersion

	Debug(dMachine, "G%d-S%d 执行receiveShard命令,index=%d,shard=%+v,status=%+v", kv.gid, kv.me, CommandIndex, *command.Op.Shard, kv.ShardStatus)

	//校验版本号可以简化问题
	if theirVersion < kv.Version {
		kv.output[CommandIndex] = &StateMachineOutput{ErrOutdated, "ErrOutdated！"}
		return
	} else if theirVersion > kv.Version {
		kv.output[CommandIndex] = &StateMachineOutput{ErrNotReady, "ErrNotReady！"}
		return
	}

	//必须检验，很可能多次接收 fixme ??
	if kv.checkDuplicate(CommandIndex, command) {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shardId=%d,重复receive！", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "重复receive！"}
		return
	}

	//校验我是否负责
	if !kv.verifyShardResponsibility(shardId) {
		panic(1)
	}
	//因为发送频率太高了，可能多次发送，导致当前状态其实是READY，所以不要覆盖
	if kv.ShardStatus[shardId] == READY {
		Assert(exists, "")
		kv.output[CommandIndex] = &StateMachineOutput{OK, "shard已经ready了"}
		return
	}

	Assert(kv.ShardStatus[shardId] == IN, "")

	//无脑接收这个shard
	kv.ShardMap[shardId] = shard
	kv.ShardStatus[shardId] = READY
	kv.output[CommandIndex] = &StateMachineOutput{OK, "shard接受成功"}

	Debug(dMachine, "G%d-S%d 执行receiveShard命令,ok,shardId=%d", kv.gid, kv.me, shardId)
}

func (kv *ShardKV) deleteShard(CommandIndex int, command Command) {
	shardId := command.Op.ShardId

	Debug(dMachine, "G%d-S%d 执行deleteShard命令,index=%d,shardId=%d,shard=%+v,status=%+v", kv.gid, kv.me, CommandIndex, shardId, command.Op.Shard, kv.ShardStatus)

	if command.Op.TheirVersion < kv.Version {
		kv.output[CommandIndex] = &StateMachineOutput{ErrOutdated, "ErrOutdated！"}
		Debug(dMachine, "G%d-S%d WARN:执行deleteShard命令,shard=%+v,status=%+v,version变化%d->%d，放弃delete", kv.gid,
			kv.me, command.Op.Shard, kv.ShardStatus, command.Op.TheirVersion, kv.Version)
		return
	}

	//检验了也没啥用，因为是自己提交的，clientId=-1
	if kv.checkDuplicate(CommandIndex, command) {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shardId=%d,重复delete！", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "重复delete！"}
		return
	}

	if kv.Config.Shards[shardId] == kv.gid {
		Debug(dMachine, "G%d-S%d 执行deleteShard命令，shardId=%d,这个shard我负责,不能删除，abort", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "我负责！，不能删除！"}
		panic(1)
		return
	}

	if _, ok := kv.ShardMap[shardId]; !ok {
		//可能会发生重复删除
		Debug(dMachine, "G%d-S%d WARN：执行deleteShard命令，shardId=%d,这个shard不存在！", kv.gid, kv.me, shardId)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "不存在!"}
		return
	}

	delete(kv.ShardMap, shardId)
	kv.ShardStatus[shardId] = NotMine

	Debug(dMachine, "G%d-S%d 执行deleteShard命令,ok,status=%+v", kv.gid, kv.me, kv.ShardStatus)
	kv.output[CommandIndex] = &StateMachineOutput{OK, "delete!"}
}

func (kv *ShardKV) put(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行put命令,index=%d,cmd=%+v,status=%+v", kv.gid, kv.me, CommandIndex, command, kv.ShardStatus)
	if !kv.verifyKeyResponsibility(command.Op.Key) {
		Debug(dMachine, "G%d-S%d WARN：执行put命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid,
			kv.me, key2shard(command.Op.Key), kv.Config.Shards[key2shard(command.Op.Key)])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, ""}
		return
	}
	shard := key2shard(command.Op.Key)

	//引导客户端重试
	if kv.ShardStatus[shard] != READY {
		Debug(dMachine, "G%d-S%d kv.ShardStatus[shard]=%+v != READY ", kv.gid, kv.me, kv.ShardStatus[shard])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, "kv.ShardStatus[shard] != READY"}
		return
	}

	state := kv.ShardMap[shard].State

	if !kv.checkDuplicate(CommandIndex, command) {
		state[command.Op.Key] = command.Op.Value
		kv.output[CommandIndex] = &StateMachineOutput{OK, command.Op.Value}
	}

}

func (kv *ShardKV) get(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行get命令,index=%d,cmd=%+v,status=%+v", kv.gid, kv.me, CommandIndex, command, kv.ShardStatus)

	if !kv.verifyKeyResponsibility(command.Op.Key) {
		Debug(dMachine, "G%d-S%d WARN：执行get命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid,
			kv.me, key2shard(command.Op.Key), kv.Config.Shards[key2shard(command.Op.Key)])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, ""}
		return
	}

	shard := key2shard(command.Op.Key)
	state := kv.ShardMap[shard].State

	if kv.ShardStatus[shard] != READY {
		Debug(dMachine, "G%d-S%d kv.ShardStatus[shard]=%+v != READY ", kv.gid, kv.me, kv.ShardStatus[shard])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, "kv.ShardStatus[shard] != READY"}
		return
	}

	if v, ok := state[command.Op.Key]; ok {
		Debug(dMachine, "G%d-S%d 执行get命令,value=%s", kv.gid, kv.me, v)
		kv.output[CommandIndex] = &StateMachineOutput{OK, v}
	} else {
		Debug(dMachine, "G%d-S%d 执行get命令,key不存在", kv.gid, kv.me, CommandIndex)
		kv.output[CommandIndex] = &StateMachineOutput{ErrNoKey, ""}
	}
}

func (kv *ShardKV) append(CommandIndex int, command Command) {
	Debug(dMachine, "G%d-S%d 执行append命令,index=%d,cmd=%+v,status=%+v", kv.gid, kv.me, CommandIndex, command, kv.ShardStatus)

	if !kv.verifyKeyResponsibility(command.Op.Key) {
		Debug(dMachine, "G%d-S%d WARN：执行append命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid,
			kv.me, key2shard(command.Op.Key), kv.Config.Shards[key2shard(command.Op.Key)])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, ""}
		return
	}

	//判断这个key存不存在，因为waitUntilReady之后，异步可能改变map，delete了
	shard := key2shard(command.Op.Key)
	if kv.ShardStatus[shard] != READY {
		Debug(dMachine, "G%d-S%d kv.ShardStatus[shard]=%+v != READY ", kv.gid, kv.me, kv.ShardStatus[shard])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, "kv.ShardStatus[shard] != READY"}
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

	kv.lastApplied = 0
	kv.output = make(map[int]*StateMachineOutput)
	kv.ShardMap = map[int]Shard{}
	kv.Version = 0
	kv.QueryCache = map[int]shardctrler.Config{}
	kv.ShardStatus = make([]string, NShards)
	for i := 0; i < NShards; i++ {
		kv.ShardStatus[i] = NotMine
	}

	go kv.PullConfigThread()
	//go kv.GCThread()
	go kv.SendShardThread()
	go kv.applier()

	go func() {
		for !kv.killed() {
			time.Sleep(time.Duration(100) * time.Millisecond) //每隔一段时间唤醒一次，防止，因为网络分区导致死锁，见md
			kv.commitIndexCond.Broadcast()
		}
	}()

	//go func() {
	//	for !kv.killed() {
	//		time.Sleep(time.Duration(50) * time.Millisecond)
	//		fmt.Println("size", kv.persister.RaftStateSize())
	//	}
	//}()

	return kv
}
