package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	Debug(dServer, "S%d 被kill", sc.me)
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	InitLog()

	sc := new(ShardCtrler)
	sc.me = me
	sc.n = len(servers)

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0
	//for i := 0; i < NShards; i++ {
	//	sc.configs[0].Shards[i] = -1 //每个分片分配给哪个group
	//}

	labgob.Register(Op{})
	labgob.Register(Command{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.mu = raft.NewReentrantLock()
	sc.commitIndexCond = sync.NewCond(sc.mu)

	sc.lastApplied = 0
	sc.output = make(map[int]*StateMachineOutput)
	sc.sessionSeed = 0
	sc.session = map[int]int{}
	sc.LoadBalancer = MakeSimpleLoadBalancer(NShards)

	go sc.applier()

	go func() {
		for !sc.killed() {
			time.Sleep(time.Duration(100) * time.Millisecond) //每隔一段时间唤醒一次，防止，因为网络分区导致死锁，见md
			sc.commitIndexCond.Broadcast()
		}
	}()

	return sc
}

func (sc *ShardCtrler) applier() {
	Debug(dServer, "S%d applier线程启动成功", sc.me)
	for op := range sc.applyCh {
		sc.mu.Lock()
		if op.SnapshotValid {
			panic("")
		} else {
			cmd := op.Command.(Command)
			Assert(op.CommandValid, "")
			Debug(dMachine, "S%d 状态机开始执行命令%+v,index=%d", sc.me, cmd, op.CommandIndex)
			Assert(op.CommandIndex == sc.lastApplied+1, fmt.Sprintf("lastApplied=%d,op=%+v \n", sc.lastApplied, op)) //保证线性一致性

			switch cmd.Op.Type {
			case QUERY:
				sc.query(op.CommandIndex, cmd)
				break
			case LEAVE:
				sc.leave(op.CommandIndex, cmd)
				break
			case MOVE:
				sc.move(op.CommandIndex, cmd)
				break
			case JOIN:
				sc.join(op.CommandIndex, cmd)
				break
			case REGISTER:
				sc.register(op.CommandIndex, cmd)
				break
			default:
				panic(1)
			}
			sc.lastApplied++
			//判断当前的字节数是否太大了
			Debug(dMachine, "S%d 状态机执行命令%+v结束，结果为%+v,更新lastApplied=%d", sc.me, cmd, sc.output[op.CommandIndex], sc.lastApplied)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) query(index int, cmd Command) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	Debug(dMachine, "S%d 执行query命令,index=%d,cmd=%+v", sc.me, index, cmd)
	//Assert(cmd.Op.Num < len(sc.configs) && cmd.Op.Num >= 0, "")
	if cmd.Op.Num == -1 {
		sc.output[index] = &StateMachineOutput{OK, *sc.latestConfig()}
	} else if cmd.Op.Num >= len(sc.configs) {
		//还没这个配置呢,引导客户端重试"配置num太大了！，还没加载！"
		sc.output[index] = &StateMachineOutput{OK, Config{-1, [NShards]int{}, nil}}
	} else {
		sc.output[index] = &StateMachineOutput{OK, sc.configs[cmd.Op.Num]}
	}

	Debug(dMachine, "S%d 执行query命令,返回%+v", sc.me, sc.output[index])
}

func (sc *ShardCtrler) move(index int, cmd Command) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	Debug(dMachine, "S%d 执行move命令,index=%d,cmd=%+v", sc.me, index, cmd)
	//重复，不用设置值，output中是nil
	if sc.checkDuplicate(index, cmd) {
		return
	}

	op := cmd.Op
	Assert(op.Shard < NShards && op.Shard >= 0, "")
	latestConfig := sc.latestConfig()
	if _, ok := latestConfig.Groups[op.GID]; !ok {
		Debug(dMachine, "S%d 执行move命令,index=%d,cmd=%+v,GID不存在", sc.me, index, cmd)
		return
	}
	assign := sc.LoadBalancer.Move(op.GID, op.Shard)
	thisConfig := Config{Num: len(sc.configs)}
	groups := CopyMap(latestConfig.Groups)

	thisConfig.Groups = groups
	thisConfig.Shards = ChangeArray2FixedArray(assign)
	sc.configs = append(sc.configs, thisConfig)

	sc.output[index] = &StateMachineOutput{OK, thisConfig}
	Debug(dMachine, "S%d 执行move命令,添加%+v", sc.me, thisConfig)
}

func (sc *ShardCtrler) join(index int, cmd Command) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	Debug(dMachine, "S%d 执行join命令,index=%d,cmd=%+v", sc.me, index, cmd)

	//重复，不用设置值，output中是nil
	if sc.checkDuplicate(index, cmd) {
		return
	}

	latestConfig := sc.latestConfig()
	thisConfig := Config{Num: len(sc.configs)}
	groups := CopyMap(latestConfig.Groups)

	for k, v := range cmd.Op.Servers {
		sc.LoadBalancer.AddGroup(k)
		groups[k] = v
	}
	thisConfig.Groups = groups
	thisConfig.Shards = ChangeArray2FixedArray(sc.LoadBalancer.GetAssignArray())
	sc.configs = append(sc.configs, thisConfig)

	sc.output[index] = &StateMachineOutput{OK, thisConfig}
	Debug(dMachine, "S%d 执行join命令,添加%+v", sc.me, thisConfig)
}

func (sc *ShardCtrler) leave(index int, cmd Command) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	Debug(dMachine, "S%d 执行move命令,index=%d,cmd=%+v", sc.me, index, cmd)
	//重复，不用设置值，output中是nil
	if sc.checkDuplicate(index, cmd) {
		return
	}

	latestConfig := sc.latestConfig()
	thisConfig := Config{Num: len(sc.configs)}
	groups := CopyMap(latestConfig.Groups)

	for _, v := range cmd.Op.GIDs {
		if _, ok := latestConfig.Groups[v]; ok {
			sc.LoadBalancer.RemoveGroup(v)
			delete(groups, v)
		}
	}

	thisConfig.Groups = groups
	thisConfig.Shards = ChangeArray2FixedArray(sc.LoadBalancer.GetAssignArray())
	sc.configs = append(sc.configs, thisConfig)

	sc.output[index] = &StateMachineOutput{OK, thisConfig}

	Debug(dMachine, "S%d 执行join命令,添加%+v", sc.me, thisConfig)
}

func (sc *ShardCtrler) register(index int, cmd Command) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	Debug(dMachine, "S%d 执行register命令,index=%d,cmd=%+v", sc.me, index, cmd)
	session := sc.sessionSeed
	sc.session[session] = -1
	sc.output[index] = &StateMachineOutput{OK, session}
	Debug(dMachine, "S%d 执行register命令,分配sessionId=%d", sc.me, session)
	sc.sessionSeed++
}
