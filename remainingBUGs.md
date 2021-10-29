1. raft.go 67行call，极低概率发生data race
2. 如果3个节点的超时时间为a b c且a=b>c此时，如果c挂了，会活锁，很长时间才能选举出一个leader
3. leader2 rejected Start() 即在测试的时候，一个节点，查找的时候被认为是leader，调用的时候又不是了，这个不一定是错误，有极低的概率发生恰好不是了