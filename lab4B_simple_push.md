1. 代码见simple push branch
2. persister.RaftStateSize() 11101, but maxraftstate 1000;因为每次get等都会写入日志，但是因为不是提交，所以没法快照，然后就超了。。
3. 存在一个case：主节点挂了，从节点变成主节点，从节点是OUT，进行重发，但是这个group的节点已经进入了下一个version。。此时就会永远无法进入下个version
所以ErrOutdated也需要认为是确认收到，因为他肯定曾经收到了，才version++了，才会收到ErrOutdated
见doSendShard方法
4. 修改shard ctrler，使其query rpc不需日志提交，这样可以加快速度，但是不可靠（存在1 2网络分区故障脑裂）
5. 不需要GC Thread，直接push成功后日志提交即可，但是可以异步提交，因为提交上之后，就有了日志。
如果使用GC线程，就会导致一种case：pull config thread认为gc无害，所以pull了，然后日志提交了，状态机执行了，会无脑设置之前不负责+现在负责的为in
即，导致删除日志缺失
但是这个之前负责的，可能还是GC状态，没有被删除；
对于从节点来说，不会发送shard，导致自己一直是out状态，然后就因为leader的删除日志缺失，而导致自己永远无法GC
也导致了触发assert。。
直接push成功后日志提交即可，没必要gc线程，但是可以存在GC状态，因为要异步提交日志
6. 状态机的执行速度有限，所以存在当前的version是v，但是log后面已经盖上了change config的可能，所以要多次校验。
7. 对于客户端而言，发送的get append put等，都不用附带版本号，因为只要校验可以执行就执行就行。状态机也是，不需要版本号。
8. 关于幂等性（即发送和接收shard，删除shard的幂等性）：
接收方：发送方会附带这个shard的版本号，接收方校验版本号，状态机也要校验一次，版本号相同的话，会防止多次接收导致覆盖
发送发：比较复杂，首先，如果接收到ERR OUTDATED的响应，也要提交日志，因为无法确定是否是丢包了，导致重发，而此时接收方已经进入下一个version了，所以必须提交日志
否则可能死锁。
其次，delete操作也要幂等，在对应版本内，幂等容易实现，如果发现没有这个shard，就不delete即可
但是因为状态机执行的滞后性，导致可能接收的时候版本没问题，但是提交的日志在一个change config后面，就导致执行这个delete的时候，发生assert panic（幸亏有assert。。）
解决办法是使用版本号，加一个版本号
9. waitUntilReady有一个问题，就是如果不考虑客户端的version的话。只考虑能READY就写的话也行，但是需要加一个超时时间，因为可能永远都不会ready。。因为分片配置的关系。。
加一个超时时间即可
10. 日志同步提交和异步提交的问题：
receive shard会同步提交，不怕网络分区（如果不是同步提交的话，2 1网络分区的 1的leader，会返回ok，此时发送方不发送了，OK了，2那一组会死锁）
日志会自己拉取，2 1分区的leader都会自己拉取，不怕网络分区
而对于删除而言，如果发生2 1网络分区，那么会有2个leader一起进行发送，因为重复收到会返回ok，所以可以异步提交
11. 关于快照和commitIndex的关系：
    直接根据快照index和lastAplied判断是否使用快照即可，但是注意，这种设计可能接收到比lastAplied小的commit log，因为可能先发送了log，但是网络卡顿，第二次发送了
    快照，快照反而先调用installSnapshot RPC，此时先执行了快照，就导致后续的commit log的index小于当前的lastAplied
12. 最后一个bug，raft state size太大，这是因为我的代码没有使用condInstallSnapshot，而这个condInstallSnapshot方法中会更新commitIndex，这就导致我即使安装了快照
    也不会更新commitIndex
    我有一段代码：
    if rf.commitIndex < rf.snapshotIndex {
    //快照还没有安装
    Debug(dTrace, "快照index=%d，commitIndex=%d，快照还没有安装，拒绝更新commitId", rf.me, rf.snapshotIndex, rf.commitIndex)
    return
    }
    这个是有意义的！
    CondInstallSnapshot难以设计，因为状态机的执行是有延迟的，如果单纯判断snap index大于commitIndex的话，状态机可能没执行到最后，而判断的却是最后这个index
    解决办法就是在rf层加一个lastApplied，因为发送的applyChan是同步的，所以就可以发送一次就增加一次lastApplied。
    这样就可以通过判断这个lastApplied，来确定是否安装快照了
    但是我的设计方式是直接加一个raft层的接口，setCommitIndex，由service层直接设置Commit index，注意service层的index是从1开始忽略nil的，需要做一层转换即可
13. 客户端确实不需要版本号，delete log也确实不需要同步提交