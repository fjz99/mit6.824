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
	Debug(dTrace, "G%d-S%d checkDuplicate shard=%v,shardMap=%+v", kv.gid, kv.me, shard, kv.ShardMap)
	Assert(session != nil, "")
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
	encoder.Encode(kv.ShardStatus)

	Debug(dServer, "G%d-S%d 创建快照完成，ShardMap=%+v,config=%+v,version=%+v,ResponsibleShards=%+v,ShardStatus=%+v",
		kv.gid, kv.me, kv.ShardMap, kv.Config, kv.Version, kv.ResponsibleShards)
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
	var sts []string

	if d.Decode(&ShardMap) != nil ||
		d.Decode(&con) != nil ||
		d.Decode(&version) != nil ||
		d.Decode(&res) != nil ||
		d.Decode(&sts) != nil {
		panic("decode err")
	} else {
		kv.ShardMap = ShardMap
		kv.Config = con
		kv.Version = version
		kv.ResponsibleShards = res
		kv.ShardStatus = sts
	}
	Debug(dServer, "G%d-S%d 读取snapshot持久化数据成功，ShardMap=%+v,config=%+v,version=%+v,ResponsibleShards=%+v,ShardStatus=%+v",
		kv.gid, kv.me, kv.ShardMap, kv.Config, kv.Version, kv.ResponsibleShards, kv.ShardStatus)
}

func (kv *ShardKV) setNewConfig(newConfig shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.Config = newConfig
	kv.Version = newConfig.Num
	kv.ResponsibleShards = []int{}
	for index, GID := range kv.Config.Shards {
		if GID == kv.gid {
			kv.ResponsibleShards = append(kv.ResponsibleShards, index)
		}
	}
}

//等待分片准备好，客户端的版本号应该大于等于我的
func (kv *ShardKV) waitUntilReady(shard int, clientVersion int) bool {
	kv.mu.Lock()
	Assert(kv.Version <= clientVersion, "")
	kv.mu.Unlock()

	for !kv.killed() {

		kv.mu.Lock()
		isLeader := kv.isLeader()
		isRespons := kv.verifyShardResponsibility(shard)
		ver := kv.Version
		status := kv.ShardStatus
		kv.mu.Unlock()

		if !isLeader || clientVersion < ver {
			return false
		}
		if clientVersion == ver && status[shard] == READY {
			return true
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	return false
}

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

func (kv *ShardKV) waitUtilInit() {
	for !kv.killed() {
		kv.mu.Lock()
		v := kv.Version
		kv.mu.Unlock()
		if v >= 1 {
			return
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (kv *ShardKV) isLeader() bool {
	return kv.me == kv.rf.GetLeaderId()
}

func (kv *ShardKV) QueryOrCached(version int) shardctrler.Config {
	if version == -1 {
		query := kv.mck.Query(version)

		kv.mu.Lock()
		kv.QueryCache[query.Num] = query
		kv.mu.Unlock()

		if query.Num == -1 {
			Debug(dWarn, "G%d-S%d QueryOrCached query=%+v,num=-1", kv.gid, kv.me, query)
		}
		return query
	}
	kv.mu.Lock()
	if v, ok := kv.QueryCache[version]; ok {
		kv.mu.Unlock()
		if v.Num == -1 {
			Debug(dWarn, "G%d-S%d QueryOrCached query=%+v,num=-1", kv.gid, kv.me, v)
		}
		return v
	}
	kv.mu.Unlock()

	query := kv.mck.Query(version)

	kv.mu.Lock()
	kv.QueryCache[query.Num] = query
	kv.mu.Unlock()
	if query.Num == -1 {
		Debug(dWarn, "G%d-S%d QueryOrCached query=%+v,num=-1", kv.gid, kv.me, query)
	}
	return query
}

func (kv *ShardKV) submitNewReceiveLog(shard Shard, version int) (int, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := &Op{ReceiveShard, "", "", &shard, shard.Id, nil, version}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	return index, isLeader
}

func (kv *ShardKV) submitNewDeleteLog(shardId int) (int, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := &Op{DeleteShard, "", "", nil, shardId, nil, -1}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	return index, isLeader
}

func (kv *ShardKV) submitNewConfigLog(config shardctrler.Config) (int, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := &Op{ChangeConfig, "", "", nil, -1, &config, -1}
	cmd := kv.buildCmd(op, -1, -1)
	index, _, isLeader := kv.rf.Start(cmd)

	return index, isLeader
}

func (kv *ShardKV) isReady(shard int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.Config.Shards[shard] == kv.gid && kv.ShardStatus[shard] == READY
}

func (kv *ShardKV) setNewStatus(from, to shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for i := 0; i < NShards; i++ {
		if from.Shards[i] == kv.gid && to.Shards[i] == kv.gid {
			kv.ShardStatus[i] = READY
		} else if from.Shards[i] == kv.gid && to.Shards[i] != kv.gid {
			kv.ShardStatus[i] = OUT
		} else if from.Shards[i] != kv.gid && to.Shards[i] == kv.gid {
			kv.ShardStatus[i] = IN
		} else {
			//我一直不负责
			Assert(kv.ShardStatus[i] == GC || kv.ShardStatus[i] == NotMine, "")
		}
	}
	Debug(dTrace, "G%d-S%d 更新新的statusMap=%+v", kv.gid, kv.me, kv.ShardStatus)
}

//必须没有in和out的，即可fetch
func (kv *ShardKV) canFetchConfig() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	can := true
	for i := 0; i < NShards; i++ {
		if kv.ShardStatus[i] == IN || kv.ShardStatus[i] == OUT {
			can = false
			break
		}
	}
	return can && kv.isLeader()
}

func (kv *ShardKV) GCThread() {

	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() {
			kv.mu.Unlock()
			continue
		}

		for i := 0; i < NShards; i++ {
			if kv.ShardStatus[i] == GC {
				if _, ok := kv.ShardMap[i]; ok {
					kv.submitNewDeleteLog(i)
					Debug(dServer, "G%d-S%d GCThread GC shard=%d", kv.gid, kv.me, i)
				} else {
					panic("GC ERR")
				}
			}
		}
		Debug(dServer, "G%d-S%d GCThread,done", kv.gid, kv.me)
		kv.mu.Unlock()
		time.Sleep(GcInterval)
	}

}

// PullConfigThread 不用同步waitFor；即使rpc handler中错误的认为自己负责，那也会在状态机中返回不负责
//如果认为自己不负责，client自动重试
//TODO client + version
func (kv *ShardKV) PullConfigThread() {

	for !kv.killed() {
		kv.mu.Lock()
		version := kv.Version
		isLeader := kv.isLeader()
		kv.mu.Unlock()

		if !isLeader {
			continue
		}

		if !kv.canFetchConfig() {
			Debug(dServer, "G%d-S%d PullConfigThread,config没迁移完，无法进行下一次拉取,status=%+v", kv.gid, kv.me, kv.ShardStatus)
		} else {
			query := kv.QueryOrCached(version + 1) //初始化的时候是0，所以自动拉取1，在状态机中初始化即可！
			if query.Num != 0 {
				//有下一个配置
				kv.submitNewConfigLog(query) //todo 同步？
				//这里可能提交失败的，因为2 1网络分区
				Debug(dServer, "G%d-S%d PullConfigThread,fetch config=%+v", kv.gid, kv.me, query)
			} else {
				Debug(dServer, "G%d-S%d PullConfigThread,无下一个config，当前version=%d", kv.gid, kv.me, version)
			}
		}
		Debug(dServer, "G%d-S%d PullConfigThread,done", kv.gid, kv.me)
		kv.mu.Unlock()
		time.Sleep(FetchConfigInterval)
	}

}

func (kv *ShardKV) SendShardThread() {

	for !kv.killed() {
		kv.mu.Lock()
		if !kv.isLeader() {
			kv.mu.Unlock()
			continue
		}

		for i := 0; i < NShards; i++ {
			if kv.ShardStatus[i] == OUT {
				if _, ok := kv.ShardMap[i]; !ok {
					panic(1)
				}
				Debug(dServer, "G%d-S%d SendShardThread,send shard=%+v", kv.gid, kv.me, kv.ShardMap[i])
				go kv.doSendShard(kv.ShardMap[i])
			}
		}
		Debug(dServer, "G%d-S%d SendShardThread,done", kv.gid, kv.me)
		kv.mu.Unlock()
		time.Sleep(SendShardInterval)
	}

}

func (kv *ShardKV) doSendShard(shard Shard) {
	kv.mu.Lock()
	target := kv.Config.Shards[shard.Id]
	version := kv.Version
	targetServers := kv.Config.Groups[target]
	kv.mu.Unlock()

	for _, server := range targetServers {
		end := kv.make_end(server)
		args := &ReceiveShardArgs{shard, -1, -1, version}
		reply := &ReceiveShardReply{}

		ok := end.Call("ShardKV.ReceiveShard", args, reply)

		if !ok {
			continue
		}

		if reply.Err == OK {
			//迁移成功
			kv.mu.Lock()
			kv.ShardStatus[shard.Id] = GC
			kv.mu.Unlock()
			return
		} else if reply.Err == ErrWrongLeader {
			continue
		} else if reply.Err == ErrWrongGroup {
			panic(1)
		} else if reply.Err == ErrNotReady || reply.Err == ErrOutdated {
			return
			//等待下次发送
		}
	}

}
