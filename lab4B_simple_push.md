1. 代码见simple push branch
2. persister.RaftStateSize() 11101, but maxraftstate 1000;因为每次get等都会写入日志，但是因为不是提交，所以没法快照，然后就超了。。
3. //存在一个case：主节点挂了，从节点变成主节点，从节点是OUT，进行重发，但是这个group的节点已经进入了下一个version。。此时就会永远无法进入下个version
    //所以ErrOutdated也需要认为是确认收到，因为他肯定曾经收到了，才version++了，才会收到ErrOutdated
见doSendShard方法
4. 修改shard ctrler，使其query rpc不需日志提交，这样可以加快速度，但是不可靠（存在1 2网络分区故障脑裂）
5. 不需要GC Thread，直接push成功后日志提交即可，但是可以异步提交，因为提交上之后，就有了日志。
如果使用GC线程，就会导致一种case：pull config thread认为gc无害，所以pull了，然后日志提交了，状态机执行了，会无脑设置之前不负责+现在负责的为in
即，导致删除日志缺失
但是这个之前负责的，可能还是GC状态，没有被删除；
对于从节点来说，不会发送shard，导致自己一直是out状态，然后就因为leader的删除日志缺失，而导致自己永远无法GC
也导致了触发assert。。
直接push成功后日志提交即可，没必要gc线程，但是可以存在GC状态，因为要异步提交日志，不会等待状态机执行，需要中间状态，但是pull的时候可以忽略GC状态，因为日志提交了
后面的日志一定在现在的日志的后面，就一定可以删除