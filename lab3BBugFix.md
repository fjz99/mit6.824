1. 快照必须能够保证可以继续检测重复seqId 
对于普通的crash重启，状态机会自动重新执行log，commitId会重新增长，所以也能够执行register，来保存session。这也说明了，必须是由状态机来保存session
而假设使用了快照的话，register等log都会消失，所以需要保存的有：state map；所有的有效的会话；
lastApplied不用快照，apply chan中有index
2. 存在接受到了比自己的快照老的节点的AE rpc的情况，此时会导致smallIndex<-1，此时直接返回conflctINdex=snap index+1，conflict term=-1即可？？
因为提交的日志还是一样的，就是快照速度不同
至于为什么会出现接收到老快照，暂时未知