package shardkv

//todo put append的操作要校验是否负责分片，在wait中！
//todo 分片迁移的内容是空值
//todo 状态机对shard的迁移的时候，如果这个shard已经存在？
//todo 增加序列号，保证不重复
//todo 尝试rpc 处理器中不拉取最新的配置
//todo 状态机执行命令时，可能出现changeConfig后的命令因为还没迁移完成分片导致无法执行的问题，此时，状态机也要wait，等待到version改变？？或者shard就绪为止
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
			if kv.rf.CondInstallSnapshot(op.SnapshotTerm, op.SnapshotIndex, op.Snapshot) {
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
	Debug(dMachine, "G%d-S%d 执行changeConfig命令,index=%d,config=%+v", kv.gid, kv.me, CommandIndex, *command.Op.Config)
	//似乎没必要检验getLatestConfig，毕竟肯定要changeConfig
	if !kv.checkDuplicate(CommandIndex, command) {
		kv.changeConfigUtil(*command.Op.Config)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "changeConfig成功！"}
	}
}

//注意：重启的时候会重新执行commit的，就导致会出现不负责之类的情况
func (kv *ShardKV) receiveShard(CommandIndex int, command Command) {
	shard := command.Op.ShardId

	Debug(dMachine, "G%d-S%d 执行receiveShard命令,index=%d,shard=%+v", kv.gid, kv.me, CommandIndex, command.Op.Shard)

	//必须检验，很可能多次接收
	if kv.checkDuplicate(CommandIndex, command) {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shard=%d,重复receive！", kv.gid, kv.me, shard)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "重复receive！"}
		return
	}

	if kv.Config.Shards[shard] != kv.gid {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shard=%d,这个shard我不负责，负责的是%d", kv.gid, kv.me, shard,
			kv.Config.Shards[shard])
		kv.output[CommandIndex] = &StateMachineOutput{ErrWrongGroup, "我不负责！"}
		return
	}

	if _, ok := kv.ShardMap[shard]; ok {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shard=%d,,map=%+v,这个shard已经存在！", kv.gid, kv.me, shard, kv.ShardMap[shard])
		kv.output[CommandIndex] = &StateMachineOutput{OK, "已经存在!"}
		return
	}

	kv.ShardMap[shard] = CopyShard(*command.Op.Shard)
	//更新ready
	if v, ok := kv.Ready[shard]; ok && v {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shard=%d,ready已经为true了!！", kv.gid, kv.me, shard, kv.ShardMap[shard])
		kv.output[CommandIndex] = &StateMachineOutput{OK, "ready已经为true了!"}
		return
	}

	kv.Ready[shard] = true

	Debug(dMachine, "G%d-S%d 执行receiveShard命令,ok,shardId=%d", kv.gid, kv.me, shard)
	kv.output[CommandIndex] = &StateMachineOutput{OK, "receive!"}
}

func (kv *ShardKV) deleteShard(CommandIndex int, command Command) {
	shard := command.Op.ShardId

	Debug(dMachine, "G%d-S%d 执行deleteShard命令,index=%d,shard=%d", kv.gid, kv.me, CommandIndex, shard)
	//检验了也没啥用，因为是自己提交的，clientId=-1
	if kv.checkDuplicate(CommandIndex, command) {
		Debug(dMachine, "G%d-S%d WARN：执行receiveShard命令，shard=%d,重复delete！", kv.gid, kv.me, shard)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "重复delete！"}
		return
	}

	if kv.Config.Shards[shard] == kv.gid {
		//我不负责就对了，因为是先改变config再删除的。。
		Debug(dMachine, "G%d-S%d 执行deleteShard命令，shard=%d,这个shard我负责！！！", kv.gid, kv.me, shard)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "我负责！，不能删除！"}
		return
	}

	if _, ok := kv.ShardMap[shard]; !ok {
		Debug(dMachine, "G%d-S%d WARN：执行deleteShard命令，shard=%d,这个shard不存在！", kv.gid, kv.me, shard)
		kv.output[CommandIndex] = &StateMachineOutput{OK, "不存在!"}
		return
	}

	delete(kv.ShardMap, shard)

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

	shard := key2shard(command.Op.Key)
	state := kv.ShardMap[shard].State
	if !kv.checkDuplicate(CommandIndex, command) {
		if v, ok := state[command.Op.Key]; ok {
			Debug(dMachine, "G%d-S%d 执行append命令,value=%s", kv.gid, kv.me, v)
			state[command.Op.Key] = v + command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, state[command.Op.Key]}
		} else {
			Debug(dMachine, "G%d-S%d 执行append命令,key不存在,自动创建", kv.gid, kv.me, CommandIndex)
			state[command.Op.Key] = command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, state[command.Op.Key]}
		}
	}

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
	//启动时创建自己负责的
	//fixme 如果是迁移失败，重启之后呢？

	go kv.fetchConfigThread()
	go kv.shardSenderThread()
	go kv.applier()

	go func() {
		for !kv.killed() {
			time.Sleep(time.Duration(100) * time.Millisecond) //每隔一段时间唤醒一次，防止，因为网络分区导致死锁，见md
			kv.commitIndexCond.Broadcast()
		}
	}()

	return kv
}
