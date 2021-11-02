package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

//执行命令，维护状态机
//todo 快照
//检查leader都在rpc中，状态机只负责维护状态
func (kv *KVServer) applier() {
	Debug(dServer, "S%d applier线程启动成功", kv.me)
	for op := range kv.applyCh {
		kv.mu.Lock()
		if op.SnapshotValid {
			Assert(!op.CommandValid, "")
			//读取快照
			if kv.rf.CondInstallSnapshot(op.SnapshotTerm, op.SnapshotIndex, op.Snapshot) {
				Debug(dServer, "S%d 装载快照,快照index=%d，lastApplied=%d", kv.me, op.SnapshotIndex, kv.lastApplied)
				kv.readSnapshotPersist(op.Snapshot)
				kv.lastApplied = op.SnapshotIndex
			} else {
				Debug(dServer, "S%d CondInstallSnapshot返回不用装载快照，快照index=%d，lastApplied=%d", kv.me, op.SnapshotIndex, kv.lastApplied)
			}
		} else {
			cmd := op.Command.(Command)
			Assert(op.CommandValid, "")
			Debug(dMachine, "S%d 状态机开始执行命令%+v,index=%d", kv.me, cmd, op.CommandIndex)
			Assert(op.CommandIndex == kv.lastApplied+1, fmt.Sprintf("lastApplied=%d,op=%+v \n", kv.lastApplied, op)) //保证线性一致性

			switch cmd.Op.Type {
			case PutType:
				{
					kv.put(op.CommandIndex, cmd)
					break
				}
			case AppendType:
				{
					kv.append(op.CommandIndex, cmd)
					break
				}
			case GetType:
				{
					kv.get(op.CommandIndex, cmd)
					break
				}
			case RegisterType:
				{
					kv.register(op.CommandIndex, cmd)
					break
				}
			default:
				panic(1)
			}
			kv.lastApplied++
			kv.commitIndexCond.Broadcast()
			//判断当前的字节数是否太大了
			size := kv.persister.RaftStateSize()
			if kv.maxraftstate > 0 && size >= kv.maxraftstate {
				Debug(dServer, "S%d 发现state size=%d，而max state size=%d,所以创建快照", kv.me, size, kv.maxraftstate)
				kv.rf.Snapshot(kv.lastApplied, kv.constructSnapshot())
			}

			Debug(dMachine, "S%d 状态机执行命令%+v结束，结果为%+v,更新lastApplied=%d", kv.me, cmd, kv.output[op.CommandIndex], kv.lastApplied)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) put(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行put命令,index=%d,cmd=%+v", kv.me, CommandIndex, command)

	if !kv.checkDuplicate(CommandIndex, command) {
		kv.stateMachine[command.Op.Key] = command.Op.Value
		kv.output[CommandIndex] = &StateMachineOutput{OK, command.Op.Value}
	}

}

func (kv *KVServer) get(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行get命令,index=%d,cmd=%+v", kv.me, CommandIndex, command)

	if v, ok := kv.stateMachine[command.Op.Key]; ok {
		Debug(dMachine, "S%d 执行get命令,value=%s", kv.me, v)
		kv.output[CommandIndex] = &StateMachineOutput{OK, v}
	} else {
		Debug(dMachine, "S%d 执行get命令,key不存在", kv.me, CommandIndex)
		kv.output[CommandIndex] = &StateMachineOutput{ErrNoKey, ""}
	}
}

func (kv *KVServer) append(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行append命令,index=%d,cmd=%+v", kv.me, CommandIndex, command)

	if !kv.checkDuplicate(CommandIndex, command) {
		if v, ok := kv.stateMachine[command.Op.Key]; ok {
			Debug(dMachine, "S%d 执行append命令,value=%s", kv.me, v)
			kv.stateMachine[command.Op.Key] = v + command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, kv.stateMachine[command.Op.Key]}
		} else {
			Debug(dMachine, "S%d 执行append命令,key不存在,自动创建", kv.me, CommandIndex)
			kv.stateMachine[command.Op.Key] = command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, kv.stateMachine[command.Op.Key]}
		}
	}

}

func (kv *KVServer) register(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行register命令,index=%d,cmd=%+v", kv.me, CommandIndex, command)
	session := &Session{kv.sessionSeed, -1, Op{Type: RegisterType}}
	kv.session[session.ClientId] = session
	kv.output[CommandIndex] = &StateMachineOutput{OK, session.ClientId}
	Debug(dMachine, "S%d 执行register命令,分配sessionId=%d", kv.me, session.ClientId)
	kv.sessionSeed++
}

func (kv *KVServer) Kill() {
	Debug(dServer, "S%d 被kill", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	//不需要close chan，可以正常停止
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	InitLog()

	labgob.Register(Op{})
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.servers = servers
	kv.me = me
	kv.n = len(servers)
	kv.maxraftstate = maxraftstate //raft 状态的最大允许大小（以字节为单位）
	kv.mu = raft.NewReentrantLock()
	kv.commitIndexCond = sync.NewCond(kv.mu)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = 0
	kv.output = make(map[int]*StateMachineOutput)
	kv.stateMachine = map[string]string{}
	kv.sessionSeed = 0
	kv.session = map[int]*Session{}

	go kv.applier() //主线程

	go func() {
		for !kv.killed() {
			time.Sleep(time.Duration(100) * time.Millisecond) //每隔一段时间唤醒一次，防止，因为网络分区导致死锁，见md
			kv.commitIndexCond.Broadcast()
		}
	}()

	return kv
}
