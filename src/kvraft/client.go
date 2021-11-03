package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

//todo 并发保护
const (
	REGISTER  string = "KVServer.ClientRegister"
	GET       string = "KVServer.ClientQuery"
	PutAppend string = "KVServer.ClientRequest"
)

const NoLeaderSleepTime = time.Duration(50) * time.Millisecond

type Clerk struct {
	servers     []*labrpc.ClientEnd
	mu          sync.Locker //保证初始化，毕竟是阻塞客户端。。其他操作都是阻塞的
	leaderIndex int
	clientId    int
	sequenceId  int
	n           int
	nowIndex    int
	fuckerId    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = -1
	ck.sequenceId = 0
	ck.leaderIndex = -1 //这个id是相对于这个数组的索引
	ck.mu = raft.NewReentrantLock()
	ck.n = len(ck.servers)
	ck.nowIndex = -1
	ck.fuckerId = nrand() % 5
	Debug(dClient, "初始化client %d成功", ck.fuckerId)
	//go ck.InitClient() //不要同步执行，因为测试用例里是同步调用这个方法的，否则会卡主
	return ck
}

func (ck *Clerk) NextServer() int {
	ck.nowIndex = (ck.nowIndex + 1) % ck.n
	return ck.nowIndex
}

// InitClient 注册client
func (ck *Clerk) InitClient() {

	Debug(dClient, "C%d 开始注册client", ck.fuckerId)
	Assert(ck.clientId == -1, "")
	for {
		args := &ClientRegisterArgs{}
		reply := &ClientRegisterReply{}
		server := -1
		if ck.leaderIndex != -1 {
			server = ck.leaderIndex
		} else {
			//随机选取一个
			server = ck.NextServer()
		}

		Debug(dClient, "C%d 开始注册client：开始处理，对%d请求", ck.fuckerId, server)
		ok := ck.servers[server].Call(REGISTER, args, reply)

		if ok {
			Debug(dClient, "C%d 开始注册client：请求返回%+v", ck.fuckerId, *reply)
			ck.leaderIndex = -1

			if reply.Status == OK {
				ck.leaderIndex = server
				ck.clientId = reply.ClientId
				break
			} else if reply.Status == ErrNoLeader {
				Debug(dClient, "C%d 开始注册client：NO LEADER！sleep 一段时间，等待选举完成", ck.fuckerId)
				time.Sleep(NoLeaderSleepTime) //等待选举完成
			} else if reply.Status == ErrWrongLeader {
				Debug(dClient, "C%d 注册client：不是leader，重试", ck.clientId)
			}
		} else {
			//异常的话就重试
			ck.leaderIndex = -1
			Debug(dClient, "C%d 开始注册client：rpc请求返回false", ck.fuckerId)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}
	Assert(ck.clientId != -1, "")
	Debug(dClient, "C%d 注册client成功，id=%d,切换为这个id", ck.fuckerId, ck.clientId)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	if ck.clientId == -1 {
		//同步初始化
		ck.InitClient()
	}

	Debug(dClient, "C%d 调用get，key=%s", ck.clientId, key)
	r := ""
	for {
		args := &ClientQueryArgs{Key: key}
		reply := &ClientQueryReply{}
		ok := false
		server := -1
		if ck.leaderIndex != -1 {
			server = ck.leaderIndex
		} else {
			//随机选取一个
			server = ck.NextServer()
		}

		Debug(dClient, "C%d 调用get，key=%s：对S%d发起请求", ck.clientId, key, server)
		ok = ck.servers[server].Call(GET, args, reply)

		if ok {
			Debug(dClient, "C%d 调用get，key=%s：请求返回%+v", ck.clientId, key, *reply)
			ck.leaderIndex = -1
			if reply.Status == OK {
				r = reply.Response
				ck.leaderIndex = server
				break
			} else if reply.Status == ErrNoKey {
				r = ""
				ck.leaderIndex = server
				break
			} else if reply.Status == ErrNoLeader {
				Debug(dClient, "C%d NO LEADER！sleep 一段时间，等待选举完成", ck.clientId)
				time.Sleep(NoLeaderSleepTime) //等待选举完成
			} else if reply.Status == ErrWrongLeader {
				Debug(dClient, "C%d 调用get：不是leader，重试", ck.clientId)
			}
		} else {
			//异常的话就重试
			ck.leaderIndex = -1
			Debug(dClient, "C%d 调用get，key=%s：请求返回false", ck.clientId, key)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}
	Debug(dClient, "C%d 调用get，key=%s,返回%s", ck.clientId, key, r)
	return r
}

func (ck *Clerk) randServer() int {
	return int(nrand()) % ck.n
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	if ck.clientId == -1 {
		//同步初始化
		ck.InitClient()
	}

	Debug(dClient, "C%d 调用PutAppend %s，key=%s,value='%s'", ck.clientId, op, key, value)
	seqId := ck.sequenceId
	ck.sequenceId++
	data := Op{}
	if op == "Put" {
		data = Op{PutType, key, value}
	} else {
		data = Op{AppendType, key, value}
	}

	for {
		args := &ClientRequestArgs{ClientId: ck.clientId, SequenceId: seqId, Op: data}
		reply := &ClientRequestReply{}
		ok := false
		server := -1
		if ck.leaderIndex != -1 {
			server = ck.leaderIndex
		} else {
			server = ck.NextServer()
			//随机选取一个
		}

		Debug(dClient, "C%d 调用PutAppend：%+v 对S%d发起请求", ck.clientId, *args, server)
		ok = ck.servers[server].Call(PutAppend, args, reply)

		if ok {
			Debug(dClient, "C%d 调用PutAppend：请求返回%+v", ck.clientId, *reply)
			ck.leaderIndex = -1
			if reply.Status == OK || reply.Status == ErrNoKey {
				//其实永远不会发生ErrNoKey。。
				ck.leaderIndex = server
				break
			} else if reply.Status == ErrNoLeader {
				Debug(dClient, "C%d NO LEADER！sleep 一段时间，等待选举完成", ck.clientId)
				time.Sleep(NoLeaderSleepTime) //等待选举完成
			} else if reply.Status == ErrWrongLeader {
				Debug(dClient, "C%d 调用PutAppend：不是leader，重试", ck.clientId)
			}

		} else {
			//异常的话就重试
			ck.leaderIndex = -1
			Debug(dClient, "C%d 调用PutAppend：请求返回false", ck.clientId)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}
	Debug(dClient, "C%d 调用PutAppend 返回", ck.clientId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {

	ck.PutAppend(key, value, "Append")
}
