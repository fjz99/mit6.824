1. 快照必须能够保证可以继续检测重复seqId 
对于普通的crash重启，状态机会自动重新执行log，commitId会重新增长，所以也能够执行register，来保存session。这也说明了，必须是由状态机来保存session
而假设使用了快照的话，register等log都会消失，所以需要保存的有：state map；所有的有效的会话；
lastApplied不用快照，apply chan中有index
2. 存在接受到了比自己的快照老的节点的AE rpc的情况，此时会导致smallIndex<-1，此时直接返回conflctINdex=snap index+1，conflict term=-1即可？？
因为提交的日志还是一样的，就是快照速度不同
至于为什么会出现接收到老快照，暂时未知
3. //Assert(op.CommandIndex == kv.lastApplied+1, fmt.Sprintf("lastApplied=%d,op=%+v \n", kv.lastApplied, op)) //保证线性一致性
//但是使用了快照之后，就不一定了，因为可能当前节点速度太慢，然后leader发送了快照，然后更新了快照，
//但是此时commitIndex对应的测试数据发出去了一部分,而且没消费完。。
//然后状态机安装了快照，但是读取到了raft接收到主节点快照之前发送的commit数据，就错误了
4. 关于状态机执行还是直接rpc handler执行的问题：
可以直接在rpc 处理器中验证序列号，成功才提交日志。也要有回滚功能，因为可能提交失败
或者通过状态机执行cmd

同样，普通的命令也有2种执行方式，状态机和rpc处理器（这种方式速度慢，而且是物理日志）