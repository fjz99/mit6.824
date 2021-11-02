package kvraft

import (
	"6.824/labrpc"
	"6.824/raft"
	"sync"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"       //对于get而言，是错误，对于put、append，都不会触发这个
	ErrWrongLeader = "ErrWrongLeader" //会返回真实leader的位置
	ErrNoLeader    = "ErrNoLeader"
)

const (
	PutType      = "put"
	AppendType   = "append"
	GetType      = "get"
	RegisterType = "register"
)

type Status string
type OpType string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string //get忽略；put就是value；append就是args
}

type Command struct {
	Op         Op
	ClientId   int //会话id
	SequenceId int
	Timestamp  int64 //完成会话超时功能
}

type ClientRequestArgs struct {
	ClientId   int
	SequenceId int
	Op         Op
}

type ClientRequestReply struct {
	LeaderHint int //只要有leader就一定会返回，如果没有就是-1
	Status     Status
}

type ClientRegisterArgs struct {
}

type ClientRegisterReply struct {
	LeaderHint int //只要有leader就一定会返回，如果没有就是-1
	ClientId   int
	Status     Status
}

type ClientQueryArgs struct {
	Key string //即get的key
}

type ClientQueryReply struct {
	LeaderHint int //只要有leader就一定会返回，如果没有就是-1
	Status     Status
	Response   string
}

// Session 快照也需要保存这个
type Session struct {
	ClientId       int
	LastSequenceId int
	LastOp         Op
}

type StateMachineOutput struct {
	Status Status
	Data   interface{}
}

// KVServer 这些id推荐使用uint64
type KVServer struct {
	mu              sync.Locker
	commitIndexCond *sync.Cond

	me int
	n  int

	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	session     map[int]*Session //会话；也要快照
	sessionSeed int              //会话id生成器；也要快照

	stateMachine map[string]string           //状态机；也要快照
	lastApplied  int                         //因为有chan，不用也行，但是有的话可以构建从1开始的，忽略nil的id；不用快照
	output       map[int]*StateMachineOutput //对应index的输出；不需要快照，只要重新执行命令即可
	servers      []*labrpc.ClientEnd
	persister    *raft.Persister
}
