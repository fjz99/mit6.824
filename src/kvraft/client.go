package kvraft

import (
	"6.824/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

const (
	REGISTER  string = "KVServer.ClientRegister"
	GET       string = "KVServer.ClientQuery"
	PutAppend string = "KVServer.ClientRequest"
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	leaderId   int
	clientId   int
	sequenceId int
	n          int
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
	ck.leaderId = -1
	ck.n = len(ck.servers)
	ck.InitClient()
	return ck
}

// InitClient 注册client
func (ck *Clerk) InitClient() {
	Debug(dClient, "开始初始化client")
	Assert(ck.clientId == -1, "")
	fmt.Println(ck.servers[0].Call(REGISTER, &ClientRegisterArgs{}, &ClientRegisterReply{}))
	fmt.Println(ck.servers[0].Call(GET, &ClientRegisterArgs{}, &ClientRegisterReply{}))
	fmt.Println(ck.servers[0].Call("KVServer.Get", &ClientRegisterArgs{}, &ClientRegisterReply{}))

	for {
		args := &ClientRegisterArgs{}
		reply := &ClientRegisterReply{}
		ok := false
		if ck.leaderId != -1 {
			Debug(dClient, "开始初始化client：FFFFFFFFF")
			ok = ck.servers[ck.leaderId].Call(REGISTER, args, reply)
		} else {
			//随机选取一个
			Debug(dClient, "开始初始化client：GGGGGGGGGGG %d %+v", ck.randServer(), ck.servers[ck.randServer()])
			ok = ck.servers[ck.randServer()].Call(REGISTER, args, reply)
		}
		if ok {
			Debug(dClient, "开始初始化client：请求返回%+v", *reply)
			ck.leaderId = reply.LeaderHint //只要有leader就一定会返回，如果没有就是-1
			if reply.Status == OK {
				ck.clientId = reply.ClientId
				break
			}
		} else {
			//异常的话就重试
			ck.leaderId = -1
			Debug(dClient, "开始初始化client：请求返回false")
		}
	}
	Assert(ck.clientId != -1, "")
	Debug(dClient, "初始化client成功，id=%d", ck.clientId)
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
	Debug(dClient, "clientId=%d 调用get，key=%s", ck.clientId, key)
	r := ""
	for {
		args := &ClientQueryArgs{Key: key}
		reply := &ClientQueryReply{}
		ok := false
		if ck.leaderId != -1 {
			ok = ck.servers[ck.leaderId].Call(GET, args, reply)
		} else {
			//随机选取一个
			ok = ck.servers[ck.randServer()].Call(GET, args, reply)
		}
		if ok {
			Debug(dClient, "clientId=%d 调用get，key=%s：请求返回%+v", ck.clientId, key, *reply)
			ck.leaderId = reply.LeaderHint //只要有leader就一定会返回，如果没有就是-1
			if reply.Status == OK {
				r = reply.Response
				break
			} else if reply.Status == ErrNoKey {
				r = ""
				break
			}
		} else {
			//异常的话就重试
			ck.leaderId = -1
			Debug(dClient, "clientId=%d 调用get，key=%s：请求返回false", ck.clientId, key)
		}
	}
	Debug(dClient, "clientId=%d 调用get，key=%s,返回%s", ck.clientId, key, r)
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
	Debug(dClient, "clientId=%d 调用PutAppend %s，key=%s,value=%s", ck.clientId, op, key, value)
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
		if ck.leaderId != -1 {
			server = ck.leaderId
		} else {
			server = ck.randServer()
			//随机选取一个
		}

		ok = ck.servers[server].Call(PutAppend, args, reply)

		if ok {
			Debug(dClient, "clientId=%d 调用PutAppend：请求返回%+v", ck.clientId, *reply)
			ck.leaderId = reply.LeaderHint //只要有leader就一定会返回，如果没有就是-1
			if reply.Status == OK || reply.Status == ErrNoKey {
				//append的情况下才会发生ErrNoKey
				break
			}
		} else {
			//异常的话就重试
			ck.leaderId = -1
			Debug(dClient, "clientId=%d 调用PutAppend：请求返回false", ck.clientId)
		}
	}
	Debug(dClient, "clientId=%d 调用PutAppend 返回", ck.clientId)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
