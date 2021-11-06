package shardkv

import (
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	"sync"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrVersion     = "ErrVersion"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	ClientId   int64
	SequenceId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type ReceiveShardArgs struct {
	Shard      Shard
	Version    int   //逻辑时钟
	ClientId   int64 //会话id
	SequenceId int
}

type ReceiveShardReply struct {
	Err     Err
	Version int
}

type StateMachineOutput struct {
	Err  Err
	Data interface{}
}

type Command struct {
	Op         Op
	ClientId   int64 //会话id
	SequenceId int
	Timestamp  int64 //完成会话超时功能
}

type Op struct {
	Type    OpType
	Key     string
	Value   string              //get忽略；put就是value；append就是args
	Shard   *Shard              //接收到的shard
	ShardId int                 //删除的shard id
	Config  *shardctrler.Config //新添加的config
}

const (
	PutType      = "put"
	AppendType   = "append"
	GetType      = "get"
	ReceiveShard = "receive"
	DeleteShard  = "delete"
	ChangeConfig = "ChangeConfig"
)

type OpType string

type Shard struct {
	Id      int
	State   map[string]string
	Session map[int64]int //会话；也要快照
	//LastModifyVersion int
}

type ShardKV struct {
	mu              sync.Locker
	commitIndexCond *sync.Cond
	me              int
	rf              *raft.Raft
	applyCh         chan raft.ApplyMsg
	make_end        func(string) *labrpc.ClientEnd
	gid             int
	ctrlers         []*labrpc.ClientEnd
	maxraftstate    int // snapshot if log grows this big

	dead int32 // set by Kill()

	lastApplied int                         //因为有chan，不用也行，但是有的话可以构建从1开始的，忽略nil的id；不用快照
	output      map[int]*StateMachineOutput //对应index的输出；不需要快照，只要重新执行命令即可
	n           int
	persister   *raft.Persister

	//分片相关
	ShardMap          map[int]Shard      //snap
	Config            shardctrler.Config //snap
	ResponsibleShards []int              //snap
	Version           int                //当前的版本号,snap
	mck               *shardctrler.Clerk

	//分片转移
	migrationChan chan *Task
	Ready         map[int]bool //指的是分片是否准备好了,snap
}

type Task struct {
	Shard   *Shard
	Version int //任务提交的时候的版本号
	Target  int //发给谁
}
