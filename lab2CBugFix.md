1. 日志最好有lru等缓冲区
2. voteFor=me并不能说明现在是leader！可能是follower！
3. log数组也需要持久化
4. 注意持久化的时候，是生成byte数组，所以要求序列化和反序列化的顺序相同，比如都是先term再voteFor等
5. 对于3个节点的集群而言，挂了重新start也会重新初始化超时时间，此时，就又可能出现2 1网络分区，而且2个同样的超时时间的问题，活锁，无法选举出leader e-112.log
6. go超时处理中，发生了协程无法退出的情况，导致积压了8000个协程，最后被kill，原因见
   https://geektutu.com/post/hpg-timeout-goroutine.html
即超时的时候，函数退出在after中，而协程无法退出，因为chan是同步队列，还没有接受方
   select default会在case阻塞的时候退出，所以不要用default，给chan设置容量即可，default的针对超时等待的目标线程的！
7. 发送线程不重试海星，重试的时候会复用task对象，有非常高的概率造成datarace，解决办法，在callback中手动添加task即可
8. 添加task的时候不要无脑清除，都是只保留最小的prevIndex的！（且这个任务的prevIndex+len（log）不能小于等于matchIndex，因为这样无意义）
9. 对于不可靠的网络而言，在发送队列里需要清除无意义的任务
每次加入发送队列时都找最有意义的
发送线程拒绝发送不符合state的
心跳只有在macthIndex=log len -1的时候+发送队列没数据的时候才会发送
回退nextIndex的时候保证最少是matchIndex+1
超时线程的chan大小设为1
不考虑lastSuccess，无脑batch
调整batchSize,fig8 unreliable会加快
发送log失败重试时，额外看看有没有新的log，此时顺便也加上
10. 回退过程只发生在leader刚被选举的时候，此时进行回退
11. 为了解耦，发送线程只负责发送，所以校验发送任务有意义的任务，就是start的了
对于一开始的nil而言，因为matchIndex=-1，此时不要从为matchIndex开始，因为大部分日志都是匹配的，所以要backward更好
而matchIndex！=-1的时候，就应该无脑从matchIndex开始，不允许空着！
所以对应start而言，如果matchIndex=-1，那么就提交一个最后的nil，回退的时候会无脑batch，不区分lastSuccess
如果matchIndex=-1，那就删除所有其他任务，并提交matchIndex+1开始的任务，如果此时没有任务发送，那ok，如果正好有任务发送，那么callback中会自动清空队列，并根据情况发送
12. verbose=1的日志输出会影响速度
