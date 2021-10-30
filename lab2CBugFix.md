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
8. 对于不可靠的网络而言，在发送队列里需要清除无意义的任务
在回调函数中完成自动重发送
对于fail的情况，需要重建Task对象，然后发送，注意此时可以顺便看看有没有新的log，就可以增大batch
对于success的情况，就在锁上阻塞，等待log增加，再次尝试构建log并发送
9. 回退过程只发生在leader刚被选举的时候，此时进行回退
10. verbose=1的日志输出会影响速度
11. 根据网上的代码，日志提交的时候，不需要等待commit，事实上是应用层面，客户端向服务端请求的时候，才返回这个，所以对于一致性模型而言，直接返回index即可，commit的内容通过chan给状态机
为啥这里等待commit就会导致速度很慢呢?因为日志是可以batch的。。这样的话batch就失效了
12. 能通过Figure8Unreliable2C测试的关键在于，减小rpc超时时间，增大batchSize
13. 
