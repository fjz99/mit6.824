1. 如果3个节点的超时时间为a b c且a=b>c此时，如果c挂了，会活锁，很长时间才能选举出一个leader
特别的，对于持久化后然后重启的情况，也有这个问题：
对于3个节点的集群而言，挂了重新start也会重新初始化超时时间，此时，就又可能出现2 1网络分区，而且2个同样的超时时间的问题，活锁，无法选举出leader
2. leader2 rejected Start() 即在测试的时候，一个节点，查找的时候被认为是leader，调用的时候又不是了，这个不一定是错误，有极低的概率发生恰好不是了
3. lab4B，challenge 2最后一个测试存在超时问题,似乎是死锁
4. 很奇怪的bug，lab3中测试的时候，如果使用-race就会通过，不加-race就会错。。
而speed test用-race就会错。。不用就会对。。
似乎是因为速度太慢。。
可以考虑使用随机clientId