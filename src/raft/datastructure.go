package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

type State int

const (
	LEADER    State = 1
	FOLLOWER  State = 2
	CANDIDATE State = 3
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC Reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	Log          []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	LeaderId     int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//发送线程的任务
type Task struct {
	RpcErrorCallback func(peerIndex int, rf *Raft, args interface{}, reply interface{})
	//Reply 用于传指针，实现reply的初始化，完成rpc
	//返回值为是否重试 参数都是指针，reply需要初始化，所以如果重试的话，需要在callback中初始化
	//这个callback会在超时或者rpc返回false时调用

	RpcSuccessCallback func(peerIndex int, rf *Raft, args interface{}, reply interface{}, task *Task)
	Args               interface{} //发送的内容
	Reply              interface{}
	RpcMethod          string //rpc 方法名
}

type Raft struct {
	mu                   sync.Locker // Lock to protect shared access to this peer's state
	broadCastCondition   *sync.Cond
	CommitIndexCondition *sync.Cond //监听commitId的变化
	logAppendCondition   *sync.Cond //监听日志的添加操作
	agreeCounter         int        //用于统计过半机制
	doneRPCs             int        //统计完成了多少rpc
	waitGroup            sync.WaitGroup

	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       //用于把commit的日志输出，从而实现测试。。
	n         int                 //总共几个节点

	state         State
	stateChanging chan *ChangedState

	term     int
	voteFor  int
	leaderId int

	commitIndex   int          //当前提交到的id
	lastApplied   int          //最后被应用到状态机的id
	nextIndex     []int        //leader使用初始化为 最大日志index的下一个id，用于回溯
	matchIndex    []int        //leader使用初始化为 -1
	senderChannel []chan *Task //为了并行发送心跳和日志提交，一个一个提交的话，是串行，非常慢，还存在超时重试的问题！在

	lastAccessTime   int64 //用于心跳检测
	electionInterval time.Duration
	rpcTimeout       time.Duration
	log              []LogEntry
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ChangedState struct {
	from State
	to   State
}
