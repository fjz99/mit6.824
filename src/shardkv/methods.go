package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"time"
)

//等到状态机做完到index,返回对应的状态机运行结果
//外部必须提供锁，否则无法cond
//必须指定超时时间，然后每隔一段时间检查一次，因为可能本来是leader
func (kv *ShardKV) waitFor(index int) *StateMachineOutput {
	Debug(dServer, "G%d-S%d waitFor index=%d", kv.gid, kv.me, index)

	for kv.lastApplied < index {
		kv.commitIndexCond.Wait() //这里的wait无法释放所有的重入的lock，只会释放一层。。
		//Debug(dServer, "G%d-S%d waitFor 被唤醒，此时index=%d，等待的index=%d", sk.me, sk.lastApplied, index)
		id := kv.rf.GetLeaderId()
		if id != kv.me {
			//因为互斥性，此时可能被取代了
			Debug(dServer, "G%d-S%d waitFor 发现自己已经被取代，新的leader为S%d", kv.gid, kv.me, id)
			return &StateMachineOutput{ErrWrongLeader, nil}
		}
	}
	//不用自己校验是否负责shard，状态机会校验并且返回的
	output := kv.output[index]
	delete(kv.output, index)
	Debug(dServer, "G%d-S%d waitFor 返回index=%d,data=%+v", kv.gid, kv.me, index, *output)
	return output
}

func (kv *ShardKV) buildCmd(op *Op, id int64, seqId int) Command {
	return Command{*op, id, seqId, raft.GetNow()}
}

//这个方法不会检查是否负责这个分片
func (kv *ShardKV) checkDuplicate(CommandIndex int, command Command) bool {
	if command.ClientId == -1 {
		//-2 -3等，都为groupId，作为clientId
		return false
	}
	shard := key2shard(command.Op.Key)
	session := kv.ShardMap[shard].Session
	if s, ok := session[command.ClientId]; ok {
		if s >= command.SequenceId {
			//不执行,因为只有put，所以也不用返回。。
			Debug(dServer, "G%d-S%d 检查会话，seqId已存在 cmd=%+v,session=%v", kv.gid, kv.me, command, s)
			kv.output[CommandIndex] = &StateMachineOutput{OK, ""}
			return true
		} else {
			session[command.ClientId] = command.SequenceId
			return false
		}
	} else {
		//会话不存在,因为使用随机clientId，所以添加会话
		session[command.ClientId] = command.SequenceId
		Debug(dServer, "G%d-S%d 自动添加会话 session=%v", kv.gid, kv.me, session[command.ClientId])
		return false
	}
}

func (kv *ShardKV) constructSnapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(kv.ShardMap)
	encoder.Encode(kv.Config)
	encoder.Encode(kv.Version)
	encoder.Encode(kv.ResponsibleShards)
	encoder.Encode(kv.Ready)

	Debug(dServer, "G%d-S%d 创建快照完成，ShardMap=%+v,config=%+v,version=%+v,ResponsibleShards=%+v,ready=%+v",
		kv.gid, kv.me, kv.ShardMap, kv.Config, kv.Version, kv.ResponsibleShards, kv.Ready)
	return buf.Bytes()
}

func (kv *ShardKV) readSnapshotPersist(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ShardMap map[int]Shard
	var con shardctrler.Config
	var version int
	var res []int
	var ready map[int]bool

	if d.Decode(&ShardMap) != nil ||
		d.Decode(&con) != nil ||
		d.Decode(&version) != nil ||
		d.Decode(&res) != nil ||
		d.Decode(&ready) != nil {
		panic("decode err")
	} else {
		kv.ShardMap = ShardMap
		kv.Config = con
		kv.Version = version
		kv.ResponsibleShards = res
		kv.Ready = ready
	}
	Debug(dServer, "G%d-S%d 读取snapshot持久化数据成功，ShardMap=%+v,config=%+v,version=%+v,ResponsibleShards=%+v,ready=%+v",
		kv.gid, kv.me, kv.ShardMap, kv.Config, kv.Version, kv.ResponsibleShards, kv.Ready)
}

//修改配置
func (kv *ShardKV) changeConfigUtil(newConfig shardctrler.Config) {
	if newConfig.Num == kv.Version {
		Debug(dServer, "G%d-S%d changeConfigUtil version等同，为%d，abort", kv.gid, kv.me, newConfig.Num)
		return
	}
	Assert(newConfig.Num > kv.Version, "")
	kv.Config = newConfig
	kv.Version = newConfig.Num
	kv.ResponsibleShards = []int{}
	for index, GID := range kv.Config.Shards {
		if GID == kv.gid {
			kv.ResponsibleShards = append(kv.ResponsibleShards, index)
		}
	}

	if newConfig.Num == 1 {
		//第一次初始化
		//初始化ready map
		kv.Ready = map[int]bool{}
		for _, i := range kv.ResponsibleShards {
			kv.Ready[i] = true
			kv.ShardMap[i] = Shard{i, map[string]string{}, map[int64]int{}}
		}
	} else {
		//初始化ready map
		kv.Ready = map[int]bool{}
		for _, i := range kv.ResponsibleShards {
			if _, ok := kv.ShardMap[i]; ok {
				//负责的还存在
				kv.Ready[i] = true
			} else {
				kv.Ready[i] = false
			}
		}
	}
	Debug(dServer, "G%d-S%d 更新config为 Config=%+v,map=%+v,Ready=%+v", kv.gid, kv.me, newConfig, kv.ShardMap, kv.Ready)
	kv.sendShards2Channel()
}

//计算自己多了的日志，并发送,只计算，不更新配置
func (kv *ShardKV) sendShards2Channel() {
	if kv.rf.GetLeaderId() != kv.me {
		return
	}
	//Debug(dTrace, "G%d-S%d 调用sendShards2Channel kv.Config.Shards=%+v", kv.gid, kv.me, kv.Config.Shards)
	//Debug(dTrace, "G%d-S%d 调用sendShards2Channel kv.map=%+v", kv.gid, kv.me, kv.ShardMap)
	var unres []int //不负责的
	for index, GID := range kv.Config.Shards {
		if GID != kv.gid {
			unres = append(unres, index)
		}
	}

	var sends []int
	for _, i := range unres {
		if _, ok := kv.ShardMap[i]; ok {
			//不负责的还存在，有问题！
			sends = append(sends, i)
		}
	}
	//Debug(dTrace, "G%d-S%d 调用sendShards2Channel sends=%v", kv.gid, kv.me, sends)
	if len(sends) > 0 {
		kv.clearChan() //因为生成新任务了
	}
	for _, i := range sends {
		v := kv.ShardMap[i]
		target := kv.Config.Shards[i]
		kv.migrationChan <- &Task{&v, kv.Version, target}
		Debug(dServer, "G%d-S%d shard=%d不负责，发送给%d", kv.gid, kv.me, i, target)
	}
	//Debug(dTrace, "G%d-S%d 调用sendShards2Channel 结束", kv.gid, kv.me)
}

//等待分片准备好，返回false代表分片不归我管了或者我不是leader了，true为已经准备好
func (kv *ShardKV) waitUntilReady(shard int) bool {
	for {
		kv.mu.Lock()
		kv.getLatestConfig(false) //拉取最新的并且等待
		isReady, ok := kv.Ready[shard]
		isLeader := kv.isLeader()
		isRespons := kv.verifyShardResponsibility(shard)
		kv.mu.Unlock()
		if !ok || !isLeader || !isRespons {
			return false
		}
		if isReady {
			return true
		}
		time.Sleep(time.Duration(200) * time.Millisecond)
	}
}

func (kv *ShardKV) doRemoveShard(shard *Shard) {
	Debug(dServer, "G%d-S%d task发送成功，开始进行shard删除：%+v", kv.gid, kv.me, *shard)

	op := &Op{DeleteShard, "", "", shard, shard.Id, nil}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		//我自己不是leader了,放弃发送
		return
	} else {
		output := kv.waitFor(index)

		//因为重新获得锁了
		//永远都不用重试，因为如果提交失败，就说明我不是leader了，那就放弃提交
		//todo 会出现没有删除的情况吗
		Debug(dServer, "G%d-S%d doRemoveShard debug output = %+v", kv.gid, kv.me, output)
		Debug(dServer, "G%d-S%d task发送成功，shard删除结束，可能完成或失败：%+v", kv.gid, kv.me, shard)
	}
}

//发送线程
func (kv *ShardKV) shardSenderThread() {
	for task := range kv.migrationChan {
		kv.mu.Lock()
		Debug(dServer, "G%d-S%d 开始发送task：%+v", kv.gid, kv.me, *task)
	tryAgain:
		if kv.rf.GetLeaderId() != kv.me {
			Debug(dServer, "G%d-S%d 我不是leader，放弃发送task：%+v", kv.gid, kv.me, *task)
			kv.mu.Unlock()
			continue
		}
		responsibleForShard := kv.findWhoResponsibleForShard(task.Shard.Id)
		if responsibleForShard != task.Target {
			Debug(dServer, "G%d-S%d task的发送目标已被改变%d->%d，放弃发送task：%+v", kv.gid, kv.me, task.Target, responsibleForShard, *task)
			kv.mu.Unlock()
			continue
		}
		//todo ??
		if task.Version < kv.Version {
			Debug(dServer, "G%d-S%d task的version已被改变%d->%d，放弃发送task：%+v", kv.gid, kv.me, task.Version, kv.Version, *task)
			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()

		//不断尝试发送，发送的时候不能加锁！，否则可能导致互相发送，然后就死锁了，因为无法进入rpc handler！
		code := kv.sendShardTo(task)

		kv.mu.Lock()
		if code == 1 {
			//进行删除日志提交
			kv.doRemoveShard(task.Shard)
		} else if code == 2 {
			Debug(dServer, "G%d-S%d task ErrWrongGroup，放弃发送task：%+v", kv.gid, kv.me, *task)
			kv.mu.Unlock()
			continue
		} else {
			//code==3，重试
			goto tryAgain
		}

		Debug(dServer, "G%d-S%d 发送task完成：%+v", kv.gid, kv.me, *task)
		kv.mu.Unlock()
	}
}

//返回状态码1：ok，2：放弃发送，3：重试发送
//细粒度的锁，也会导致互斥性比较弱，导致方法内状态改变
func (kv *ShardKV) sendShardTo(task *Task) int {
	kv.mu.Lock()
	targetServers := make([]string, len(kv.Config.Groups[task.Target]))
	copy(targetServers, kv.Config.Groups[task.Target])
	version := kv.Version
	kv.mu.Unlock()

	for _, g := range targetServers {
		kv.mu.Lock()
		if kv.Version != version {
			Debug(dServer, "G%d-S%d 发送task=%+v的时候，发现version已经改变为%d->%d", kv.gid, kv.me, *task, version, kv.Version)
			kv.mu.Unlock()
			return 2
		}
		end := kv.make_end(g)
		//fixme !!!
		args := &ReceiveShardArgs{*task.Shard, task.Version, int64(kv.gid*(-1) - 1), 1}
		reply := &ReceiveShardReply{}
		kv.mu.Unlock()

		ok := end.Call("ShardKV.ReceiveShard", args, reply)

		//reply是安全的，不用加锁
		if ok {
			if reply.Err == ErrVersion {
				//接收方发现版本比我大,getLatestConfig中有wait，需要加锁
				kv.mu.Lock()
				kv.getLatestConfig(false)
				kv.mu.Unlock()

				return 2 //todo ??放弃还是重试？
			} else if reply.Err == ErrWrongLeader {
				continue
			} else if reply.Err == ErrWrongGroup {
				//接收方发现这个shard自己不负责
				return 2
			} else if reply.Err == OK {
				return 1
			}
		}
	}
	return 3 //因为一直wrong leader，可能正在选举
}

func (kv *ShardKV) getConfigFor(version int, sendShardIfVersionEquals bool) {
	if kv.rf.GetLeaderId() != kv.me {
		Debug(dServer, "G%d-S%d 我不是leader,不拉取配置，abort", kv.gid, kv.me)
		return
	}

	query := kv.mck.Query(version)
	//如果查找的version太大了，就会自动退出return
	if query.Num == -1 {
		Debug(dServer, "G%d-S%d version=%d太大，没找到配置", kv.gid, kv.me, version)
		return
	}
	Assert(query.Num >= kv.Version, "")
	if query.Num == kv.Version {
		//直接添加任务
		if sendShardIfVersionEquals {
			Debug(dServer, "G%d-S%d version等同，添加发送shard任务", kv.gid, kv.me)
			kv.sendShards2Channel()
		}
	} else {
		Debug(dServer, "G%d-S%d 广播配置更新日志config=%+v", kv.gid, kv.me, query)
		op := &Op{ChangeConfig, "", "", nil, -1, &query}
		cmd := kv.buildCmd(op, -1, -1)
		index, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			Debug(dServer, "G%d-S%d 广播配置更新日志:我不是leader，abort", kv.gid, kv.me)
		} else {
			output := kv.waitFor(index)

			//因为重新获得锁了
			Debug(dServer, "G%d-S%d Get debug output = %+v", kv.gid, kv.me, output)
			if output.Err == ErrWrongLeader {
				Debug(dServer, "G%d-S%d 广播配置更新日志:我不是leader，abort", kv.gid, kv.me)
			} else if output.Err == OK {
				Debug(dServer, "G%d-S%d 广播配置更新日志:配置日志广播完成", kv.gid, kv.me)
			} else {
				panic(1)
			}
		}
	}
	Debug(dServer, "G%d-S%d 拉取配置结束", kv.gid, kv.me)
}

//不加锁
func (kv *ShardKV) getLatestConfig(sendShardIfVersionEquals bool) {
	kv.getConfigFor(-1, sendShardIfVersionEquals)
}

//获得最新的config
//func (kv *ShardKV) getLatestConfig() {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//
//	query := kv.mck.Query(-1)
//	if query.Num == kv.Version {
//		return
//	}
//	kv.Config = query
//	kv.Version = query.Num
//	kv.ResponsibleShards = []int{}
//	var unres []int //不负责的
//	for index, GID := range kv.Config.Shards {
//		if GID == kv.gid {
//			kv.ResponsibleShards = append(kv.ResponsibleShards, index)
//		} else {
//			unres = append(unres, index)
//		}
//	}
//
//	//初始化ready map
//	kv.Ready = map[int]bool{}
//	for _, i := range kv.ResponsibleShards {
//		if _, ok := kv.ShardMap[i]; ok {
//			//负责的还存在
//			kv.Ready[i] = true
//		} else {
//			kv.Ready[i] = false
//		}
//	}
//	var sends []int
//	for _, i := range unres {
//		if _, ok := kv.ShardMap[i]; ok {
//			//不负责的还存在，有问题！
//			sends = append(sends, i)
//		}
//	}
//	if len(sends) > 0 {
//		kv.clearChan() //因为生成新任务了
//	}
//	for _, i := range sends {
//		v := kv.ShardMap[i]
//		target := kv.Config.Shards[i]
//		kv.migrationChan <- &v
//		Debug(dServer, "G%d-S%d shard=%d不负责，发送给%d", kv.gid, kv.me, i, target)
//	}
//	Debug(dServer, "G%d-S%d 更新config为 Config=%+v,map=%+v", kv.gid, kv.me, query, kv.ShardMap)
//}

//不会自动fetch
func (kv *ShardKV) verifyKeyResponsibility(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.Config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) verifyShardResponsibility(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.Config.Shards[shard] == kv.gid
}

//返回GID
//不会自动fetch
func (kv *ShardKV) findWhoResponsibleFor(key string) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.Config.Shards[key2shard(key)]
}

func (kv *ShardKV) findWhoResponsibleForShard(shard int) int {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.Config.Shards[shard]
}

func (kv *ShardKV) clearChan() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for i := 0; i < len(kv.migrationChan); i++ {
		_ = <-kv.migrationChan
	}
}

func (kv *ShardKV) fetchConfigThread() {
	time.Sleep(time.Duration(500) * time.Millisecond)
	for {
		kv.mu.Lock() //整体锁定好一些

		isLeader := kv.me == kv.rf.GetLeaderId()
		if isLeader {
			//拉取最新的配置
			if kv.Version == -1 {
				//应该初始化，拉取num=1的配置;直接手动提交
				Debug(dServer, "G%d-S%d 初始化线程：开始初始化", kv.gid, kv.me)
				kv.getConfigFor(1, false)
			} else {
				kv.getLatestConfig(true)
			}
		}

		kv.mu.Unlock()
		time.Sleep(time.Duration(800) * time.Millisecond)
	}
}

func (kv *ShardKV) waitUtilInit() {
	for {
		kv.mu.Lock()
		v := kv.Version
		kv.mu.Unlock()
		if v >= 1 {
			return
		}
		time.Sleep(time.Duration(200) * time.Millisecond)
	}
}

func (kv *ShardKV) isLeader() bool {
	return kv.me == kv.rf.GetLeaderId()
}
