package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
)

//todo 暂时先不用队列，直接使用lcoalIndex

//执行命令，维护状态机
//todo 暂时不考虑快照
//检查leader都在rpc中，状态机只负责维护状态
func (kv *KVServer) applier() {
	Debug(dServer, "S%d applier线程启动成功", kv.me)
	for op := range kv.applyCh {
		if op.SnapshotValid {
			Assert(!op.CommandValid, "")
		} else {
			cmd := op.Command.(Command)
			Assert(op.CommandValid, "")
			Assert(op.CommandIndex == kv.lastApplied+1, "") //保证线性一致性
			Debug(dMachine, "S%d 状态机开始执行命令%+v", kv.me, cmd)

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
			Debug(dMachine, "S%d 状态机开始执行命令%+v结束，结果为%+v", kv.me, cmd, kv.output[op.CommandIndex])
		}
	}
}

//todo
//todo 重复验证+保存最近的op
func (kv *KVServer) put(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行put命令,index=%d,cmd=%d", kv.me, CommandIndex, command)

	if !kv.checkDuplicate(CommandIndex, command) {
		kv.stateMachine[command.Op.Key] = command.Op.Value
		kv.output[CommandIndex] = &StateMachineOutput{OK, command.Op.Value}
	}

	kv.commitIndexCond.Broadcast()
}

func (kv *KVServer) get(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行get命令,index=%d,cmd=%d", kv.me, CommandIndex, command)

	if v, ok := kv.stateMachine[command.Op.Key]; ok {
		Debug(dMachine, "S%d 执行get命令,value=%s", kv.me, CommandIndex, v)
		kv.output[CommandIndex] = &StateMachineOutput{OK, v}
	} else {
		Debug(dMachine, "S%d 执行get命令,key不存在", kv.me, CommandIndex)
		kv.output[CommandIndex] = &StateMachineOutput{ErrNoKey, ""}
	}
	kv.commitIndexCond.Broadcast()
}

func (kv *KVServer) append(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行append命令,index=%d,cmd=%d", kv.me, CommandIndex, command)

	if !kv.checkDuplicate(CommandIndex, command) {
		if v, ok := kv.stateMachine[command.Op.Key]; ok {
			Debug(dMachine, "S%d 执行append命令,value=%s", kv.me, CommandIndex, v)
			kv.stateMachine[command.Op.Key] = v + command.Op.Value
			kv.output[CommandIndex] = &StateMachineOutput{OK, kv.stateMachine[command.Op.Key]}
		} else {
			Debug(dMachine, "S%d 执行append命令,key不存在", kv.me, CommandIndex)
			kv.output[CommandIndex] = &StateMachineOutput{ErrNoKey, ""}
		}
	}

	kv.commitIndexCond.Broadcast()
}

func (kv *KVServer) register(CommandIndex int, command Command) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Debug(dMachine, "S%d 执行register命令,index=%d,cmd=%d", kv.me, CommandIndex, command)
	session := &Session{kv.sessionId, -1, Op{Type: RegisterType}}
	kv.session[session.clientId] = session
	kv.output[CommandIndex] = &StateMachineOutput{OK, session.clientId}
	Debug(dMachine, "S%d 执行register命令,分配sessionId=%d", kv.me, session.clientId)
	kv.commitIndexCond.Broadcast()
	kv.sessionId++
}

func (kv *KVServer) Kill() {
	Debug(dServer, "被kill", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.applyCh) //否则applier线程无法停止。。
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	InitLog()

	labgob.Register(Op{})
	labgob.Register(Command{}) //todo ??

	kv := new(KVServer)
	kv.me = me
	kv.n = len(servers)
	kv.maxraftstate = maxraftstate
	kv.mu = raft.NewReentrantLock()
	kv.commitIndexCond = sync.NewCond(kv.mu)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastApplied = 0
	kv.output = make(map[int]*StateMachineOutput)
	kv.sessionId = 0
	kv.session = map[int]*Session{}

	go kv.applier() //主线程

	return kv
}
