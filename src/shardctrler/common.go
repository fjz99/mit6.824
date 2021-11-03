package shardctrler

import (
	"6.824/raft"
	"sync"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards The number of shards.
//规定只有10个分片
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader" //会返回真实leader的位置
	ErrNoLeader    = "ErrNoLeader"
)

type Err string

// JoinArgs 添加新的group
type JoinArgs struct {
	// new GID -> servers mappings；non-zero replica group identifiers (GIDs) to lists of server names
	Servers map[int][]string
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

// LeaveArgs 删除group
type LeaveArgs struct {
	GIDs []int //gid
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

// MoveArgs 规定某个group负责某个shard
type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

// QueryArgs -1即为最新的配置
type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const (
	JOIN     = "join"
	LEAVE    = "leave"
	QUERY    = "query"
	MOVE     = "move"
	REGISTER = "register"
)

type OpType string

type ShardCtrler struct {
	mu              sync.Locker
	commitIndexCond *sync.Cond
	me              int
	n               int
	rf              *raft.Raft
	applyCh         chan raft.ApplyMsg
	closeChan       chan bool

	session     map[int]int //处理重复，保存seqId
	sessionSeed int

	configs []Config // indexed by config num,状态机

	lastApplied int                         //因为有chan，不用也行，但是有的话可以构建从1开始的，忽略nil的id；不用快照
	output      map[int]*StateMachineOutput //对应index的输出；不需要快照，只要重新执行命令即可
	dead        int32
}

type StateMachineOutput struct {
	Err  Err
	Data interface{}
}

type Command struct {
	Op         Op
	ClientId   int //会话id
	SequenceId int
	Timestamp  int64 //完成会话超时功能
}

type Op struct {
	Type  OpType
	Key   string
	Value string //get忽略；put就是value；append就是args
}
