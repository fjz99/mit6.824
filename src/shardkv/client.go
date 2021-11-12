package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	clientId   int64
	sequenceId int
}

// MakeClerk
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end

	ck.clientId = nrand()
	ck.sequenceId = 0
	ck.config = ck.sm.Query(-1)
	Debug(dClient, "初始化client %d成功(随机clientId),config=%+v", ck.clientId, ck.config)
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	Debug(dClient, "C%d 调用get，key=%s", ck.clientId, key)
	for {
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Version = ck.config.Num
		Debug(dClient, "C%d 调用get，key=%s,选择group为 %d", ck.clientId, key, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					Debug(dClient, "C%d 调用get，key=%s,返回%s", ck.clientId, key, reply.Value)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					Debug(dClient, "C%d 调用get，key=%s,group WRONG", ck.clientId, key)
					break
				}
				if ok && reply.Err == ErrOutdated {
					//我的版本太低了,重试
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// PutAppend
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.SequenceId = ck.sequenceId
	ck.sequenceId++

	Debug(dClient, "C%d 调用PutAppend %s，key=%s,value='%s'", ck.clientId, op, key, value)
	for {
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Version = ck.config.Num
		Debug(dClient, "C%d 调用PutAppend %s，key=%s,value='%s',选择gid=%d", ck.clientId, op, key, value, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					Debug(dClient, "C%d 调用put append，ok", ck.clientId)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					Debug(dClient, "C%d 调用put append，key=%s,group WRONG", ck.clientId, key)
					break
				}
				if ok && reply.Err == ErrOutdated {
					//我的版本太低了,重试
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// InitClient 注册client
//clientId由分片协调者提供，group内部只会存储对应的last seqId
//其实自己产生随机数当客户端id就行了。。。
