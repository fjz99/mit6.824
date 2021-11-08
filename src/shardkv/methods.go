package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"6.824/shardctrler"
	"bytes"
	"fmt"
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
	Debug(dTrace, "G%d-S%d changeConfigUtil new:%+v,my:%+v", kv.gid, kv.me, newConfig, kv.Config)
	if newConfig.Num <= kv.Version {
		//调用gotLatestConfig的方法很多，所以很容易出现并发问题，即newConfig的版本比当前的版本小等。。
		Debug(dServer, "G%d-S%d changeConfigUtil version等同，为%d，abort", kv.gid, kv.me, newConfig.Num)
		return
	}
	//Assert(newConfig.Num > kv.Version, "")
	//checkAlwaysMe := kv.checkAlwaysMe(kv.Version, newConfig.Num) //获得改变的map
	lastConfig := kv.Config
	kv.setNewConfig(newConfig)

	if newConfig.Num == 1 {
		//第一次初始化
		//初始化ready map
		kv.Ready = map[int]bool{}
		for _, i := range kv.ResponsibleShards {
			kv.Ready[i] = true
			kv.ShardMap[i] = Shard{i, map[string]string{}, map[int64]int{}, kv.Version}
		}
	} else {
		//初始化ready map
		kv.Ready = map[int]bool{}
		for _, i := range kv.ResponsibleShards {
			if shard, ok := kv.ShardMap[i]; ok {
				//判断
				if shard.LastModifyVersion == kv.Version-1 {
					kv.Ready[i] = true
					kv.ShardMap[i] = Shard{shard.Id, shard.State, shard.Session, kv.Version}
					Debug(dServer, "G%d-S%d config %d->%d时，shard lastId自增 %+v", kv.gid, kv.me, lastConfig.Num, kv.Version, kv.ShardMap[i])
				} else {
					kv.Ready[i] = false
				}
			} else {
				kv.Ready[i] = false
			}
		}
	}
	Debug(dServer, "G%d-S%d 更新config为 Config=%+v,map=%+v,Ready=%+v", kv.gid, kv.me, newConfig, kv.ShardMap, kv.Ready)
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

//查找所有自己的shard，判断下一跳是否存在，如果存在，而且不是我，就发送
func (kv *ShardKV) sendShards2Channel() {
	kv.mu.Lock()
	isLeader := kv.isLeader()
	lens := len(kv.migrationChan)
	kv.mu.Unlock()
	if !isLeader || lens > 0 {
		return
	}

	kv.mu.Lock()
	Debug(dTrace, "G%d-S%d 调用sendShards2Channel kv.Config.Shards=%+v", kv.gid, kv.me, kv.Config.Shards)
	Debug(dTrace, "G%d-S%d 调用sendShards2Channel version=%d,kv.map=%+v", kv.gid, kv.me, kv.Version, kv.ShardMap)
	kv.mu.Unlock()

	for i := 0; i < NShards; i++ {
		kv.mu.Lock()
		b, ok := kv.ShardMap[i]
		copyShard := CopyShard(b)
		myVersion := kv.Version
		kv.mu.Unlock()
		lastVersion := kv.QueryOrCached(copyShard.LastModifyVersion)
		if ok {
			//这个版本我负责
			if copyShard.LastModifyVersion < myVersion {
				//version不是最新的
				query := kv.QueryOrCached(copyShard.LastModifyVersion + 1)
				//无脑发送，即使下一跳是我，在rpc处理器中，会自动处理的
				target := query.Shards[i]
				kv.mu.Lock()
				if target == kv.gid && lastVersion.Shards[i] == kv.gid {
					//两个都是我负责
					shardMap := kv.ShardMap[i]
					kv.ShardMap[i] = Shard{shardMap.Id, shardMap.State,
						shardMap.Session, shardMap.LastModifyVersion + 1}
					if shardMap.LastModifyVersion+1 == kv.Version {
						kv.Ready[i] = true
					}
					Debug(dServer, "G%d-S%d shard=%+v下一个就是我负责，直接last++", kv.gid, kv.me, copyShard)
					kv.mu.Unlock()
					continue
				}
				kv.migrationChan <- &Task{&copyShard, target} //直接发送也行，反正会转发。。
				Debug(dServer, "G%d-S%d shard=%+v不负责，发送给下一跳：%d", kv.gid, kv.me, copyShard, target)
				kv.mu.Unlock()
			}
		}
	}
	Debug(dTrace, "G%d-S%d 调用sendShards2Channel 结束", kv.gid, kv.me)
}

//等待分片准备好，返回false代表分片不归我管了或者我不是leader了，true为已经准备好
func (kv *ShardKV) waitUntilReady(shard int) bool {
	for !kv.killed() {
		kv.getLatestConfig() //拉取最新的并且等待

		kv.mu.Lock()
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
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
	return false
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
		Debug(dServer, "G%d-S%d 开始发送task：%+v，shard=%+v", kv.gid, kv.me, *task, *task.Shard)
	tryAgain:
		if kv.rf.GetLeaderId() != kv.me {
			Debug(dServer, "G%d-S%d 我不是leader，放弃发送task：%+v，shard=%+v", kv.gid, kv.me, *task, *task.Shard)
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
			Debug(dServer, "G%d-S%d task ErrWrongGroup，放弃发送task：%+v，shard=%+v", kv.gid, kv.me, *task, *task.Shard)
			kv.mu.Unlock()
			continue
		} else if code == 3 {
			//code==3，重试
			Debug(dServer, "G%d-S%d task code==3，重试：%+v，shard=%+v", kv.gid, kv.me, *task, *task.Shard)
			goto tryAgain
		} else {
			kv.mu.Unlock()
			continue
		}

		Debug(dServer, "G%d-S%d 发送task完成：%+v，shard=%+v", kv.gid, kv.me, *task, *task.Shard)
		kv.mu.Unlock()
	}
}

//返回状态码1：ok，2：放弃发送，3：重试发送,4被移除
//细粒度的锁，也会导致互斥性比较弱，导致方法内状态改变
func (kv *ShardKV) sendShardTo(task *Task) int {
	Debug(dTrace, "G%d-S%d sendShardTo：%+v，shard=%+v", kv.gid, kv.me, *task, *task.Shard)
	kv.mu.Lock()
	c := kv.QueryOrCached(task.Shard.LastModifyVersion + 1)
	target := c.Shards[task.Shard.Id]
	Assert(target == task.Target, "")
	targetServers := make([]string, len(c.Groups[task.Target]))
	copy(targetServers, c.Groups[task.Target])
	//for len(targetServers) fixme
	kv.mu.Unlock()
	index := 0
	Debug(dTrace, "G%d-S%d sendShardTo：targetServers=%+v", kv.gid, kv.me, targetServers)
	//fixme 拒绝发送，因为会有后面的人负责发送的
	if len(targetServers) == 0 {
		//kv.mu.Lock()
		//kv.ShardMap[task.Shard.Id] = Shard{task.Shard.Id, task.Shard.State, task.Shard.Session,
		//	task.Shard.LastModifyVersion + 1}
		//kv.mu.Unlock()
		Debug(dTrace, "G%d-S%d sendShardTo：target group=%d 已经被移除,所以自增LastModifyVersion", kv.gid, kv.me, task.Target)
		panic(1)
		return 4
	}

	living := 0
	for index < len(targetServers) {
		kv.mu.Lock()
		end := kv.make_end(targetServers[index])
		//fixme !!!
		args := &ReceiveShardArgs{*task.Shard, int64(kv.gid*(-1) - 1), 1}
		reply := &ReceiveShardReply{}
		kv.mu.Unlock()

		ok := end.Call("ShardKV.ReceiveShard", args, reply)
		//Debug(dTrace, "G%d-S%d fuck! sendShardTo：ok=%v,end=%v;%v", kv.gid, kv.me, ok, end, targetServers[index])

		//reply是安全的，不用加锁
		if ok {
			if reply.Err == ErrWrongLeader {
				index++
				living++
				continue
			} else if reply.Err == ErrWrongGroup {
				//接收方发现这个shard自己不负责
				return 2
			} else if reply.Err == OK {
				return 1
			}
		} else {
			//检查一下要发送的东西是不是被移除了
			kv.mu.Lock()
			if len(kv.Config.Groups[task.Target]) == 0 {
				kv.ShardMap[task.Shard.Id] = Shard{task.Shard.Id, task.Shard.State, task.Shard.Session,
					task.Shard.LastModifyVersion + 1}
				kv.mu.Unlock()
				Debug(dTrace, "G%d-S%d sendShardTo：target group=%d 已经被移除,所以自增LastModifyVersion", kv.gid, kv.me, task.Target)
				return 4
			}
			kv.mu.Unlock()
			Debug(dTrace, "G%d-S%d sendShardTo：ok=false,retry!", kv.gid, kv.me)
			index++ //因为存在shutdown server的可能，即让服务器关闭，此时返回ok=false，或者说，要考虑到宕机的情况，所以一个请求不通就发送另一个
			continue
		}
	}
	if living >= 1 {
		return 3 //因为一直wrong leader，可能正在选举
	} else {
		return 2 //根本没有能访问的，都是false。。
	}
}

func (kv *ShardKV) getConfigFor(version int) {
	Debug(dServer, "G%d-S%d 调用 getConfigFor version=%d", kv.gid, kv.me, version)

	kv.mu.Lock()
	isLeader := kv.isLeader()
	thisVersion := kv.Version
	kv.mu.Unlock() //fixme

	query := kv.QueryOrCached(version) //这里不要锁定！

	if !isLeader {
		Debug(dServer, "G%d-S%d 我不是leader,不拉取配置，abort", kv.gid, kv.me)
		return
	}

	if !isLeader {
		Debug(dServer, "G%d-S%d 我不是leader,不拉取配置，abort", kv.gid, kv.me)
		return
	}
	//如果查找的version太大了，就会自动退出return
	if query.Num == -1 {
		Debug(dServer, "G%d-S%d version=%d太大，没找到配置", kv.gid, kv.me, version)
		return
	}

	Assert(query.Num >= thisVersion, fmt.Sprintf("query:%+v,my version=%d", query, thisVersion))
	if query.Num == thisVersion {
		//直接添加任务
		//if sendShardIfVersionEquals {
		//	if len(kv.migrationChan) == 0 {
		//		Debug(dServer, "G%d-S%d version等同，添加发送shard任务", kv.gid, kv.me)
		//		kv.sendShards2Channel()
		//	}
		//}
	} else {
		//index := 0
		isLeader := false
		//保证changeConfig日志是一次一次提交的，否则就可能出现无法初始化的情况，因为上来就是num=3了之类的
		start := raft.Max(1, thisVersion+1)
		for i := start; i <= query.Num; i++ {
			thisConfig := kv.QueryOrCached(i)
			op := &Op{ChangeConfig, "", "", nil, -1, &thisConfig}
			cmd := kv.buildCmd(op, -1, -1)
			Debug(dServer, "G%d-S%d 广播配置更新日志config=%+v", kv.gid, kv.me, thisConfig)
			_, _, isLeader = kv.rf.Start(cmd)

			if !isLeader {
				Debug(dServer, "G%d-S%d 广播配置更新日志:我不是leader，abort", kv.gid, kv.me)
				return
			}

		}

		//因为日志已经提交了，所以直接修改为最新的，即可保证后续log中的entry一定是我负责的！
		//但是follower仍然是通过日志来修改config的
		//kv.mu.Lock()
		//if kv.Version >= thisConfig.Num {
		//	kv.mu.Unlock()
		//	continue
		//}
		//kv.mu.Unlock()

		thisConfig := kv.QueryOrCached(query.Num)
		kv.mu.Lock()                    //调用gotLatestConfig的方法很多，所以很容易出现并发问题
		kv.changeConfigUtil(thisConfig) //负责list也会修改
		//kv.sendShards2Channel()
		Debug(dServer, "G%d-S%d 拉取配置结束,最终修改为%+v", kv.gid, kv.me, kv.Config)
		kv.mu.Unlock()

		//kv.mu.Lock()
		//output := kv.waitFor(index)
		//kv.mu.Unlock()

		////因为重新获得锁了
		//Debug(dServer, "G%d-S%d GetConfigFor debug output = %+v", kv.gid, kv.me, output)
		//if output.Err == ErrWrongLeader {
		//	Debug(dServer, "G%d-S%d 广播配置更新日志:我不是leader，abort", kv.gid, kv.me)
		//} else if output.Err == OK {
		//	Debug(dServer, "G%d-S%d 广播配置更新日志:配置日志广播完成", kv.gid, kv.me)
		//} else {
		//	panic(1)
		//}
	}
	Debug(dServer, "G%d-S%d 拉取配置结束", kv.gid, kv.me)
}

//不加锁
func (kv *ShardKV) getLatestConfig() {
	kv.getConfigFor(-1)
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

func (kv *ShardKV) clearChan() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for i := 0; i < len(kv.migrationChan); i++ {
		_ = <-kv.migrationChan
	}
}

func (kv *ShardKV) fetchConfigThread() {
	time.Sleep(time.Duration(200) * time.Millisecond)
	for !kv.killed() {
		kv.mu.Lock()
		isLeader := kv.isLeader()
		version := kv.Version
		kv.mu.Unlock()

		if isLeader {
			//拉取最新的配置
			if version == -1 {
				//应该初始化，拉取num=1的配置;直接手动提交
				Debug(dServer, "G%d-S%d 初始化线程：开始初始化", kv.gid, kv.me)
				kv.getConfigFor(1)
				Debug(dServer, "G%d-S%d 初始化线程：初始化完成", kv.gid, kv.me)
			} else {
				kv.getLatestConfig()
			}
		}

		time.Sleep(time.Duration(500) * time.Millisecond)
	}
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

		return query
	}
	kv.mu.Lock()
	if v, ok := kv.QueryCache[version]; ok {
		kv.mu.Unlock()
		return v
	}
	kv.mu.Unlock()

	query := kv.mck.Query(version)

	kv.mu.Lock()
	kv.QueryCache[query.Num] = query
	kv.mu.Unlock()
	return query
}
