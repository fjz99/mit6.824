package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"crypto/rand"
	"math/big"
)
import "time"

type Clerk struct {
	servers     []*labrpc.ClientEnd
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
	ck.n = len(ck.servers)
	ck.nowIndex = -1
	ck.fuckerId = nrand() % 5
	Debug(dClient, "初始化client %d成功", ck.fuckerId)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	if ck.clientId == -1 {
		ck.InitClient()
	}

	args := &QueryArgs{}
	args.Num = num
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	if ck.clientId == -1 {
		ck.InitClient()
	}

	args := &JoinArgs{ClientId: ck.clientId, SeqId: ck.sequenceId}
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	if ck.clientId == -1 {
		ck.InitClient()
	}

	args := &LeaveArgs{ClientId: ck.clientId, SeqId: ck.sequenceId}
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	if ck.clientId == -1 {
		ck.InitClient()
	}

	args := &MoveArgs{ClientId: ck.clientId, SeqId: ck.sequenceId}
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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
		ok := ck.servers[server].Call("ShardCtrler.ClientRegister", args, reply)

		if ok {
			Debug(dClient, "C%d 开始注册client：请求返回%+v", ck.fuckerId, *reply)
			ck.leaderIndex = -1

			if reply.Err == OK {
				ck.leaderIndex = server
				ck.clientId = reply.ClientId
				break
			} else if reply.Err == ErrWrongLeader {
				Debug(dClient, "C%d 注册client：不是leader，重试", ck.clientId)
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			//异常的话就重试
			ck.leaderIndex = -1
			Debug(dClient, "C%d 开始注册client：rpc请求返回false", ck.fuckerId)
			time.Sleep(100 * time.Millisecond)
		}
	}
	Assert(ck.clientId != -1, "")
	Debug(dClient, "C%d 注册client成功，id=%d,切换为这个id", ck.fuckerId, ck.clientId)
}
