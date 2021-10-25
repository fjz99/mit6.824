1. 只有守护线程才会自动停止，而心跳线程不会，所以即使主线程结束了，服务器还是在运行
2. 并行发送心跳和日志提交，一个一个提交的话，是串行，非常慢，还存在超时重试的问题！
3. 当接收到的term小的时候，不会重置计时器，重置计时器的3种情况
   if a) you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, 
you should not reset your timer); b) you are starting an election; or c) you grant a vote to another peer.
4. For example, if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you, 
you should first step down and adopt their term (thereby resetting votedFor), and then handle the RPC, 
which will result in you granting the vote!
即，先修改term，但是修改term就会导致voteFor清空，所以就可以再次投票
5. The tester requires that the leader send heartbeat RPCs no more than ten times per second.
即心跳最多1秒10次，100ms
6. 选举超时时间为随机数，250-400ms
7. 另一种实现，for + sleep
8. 须保证选举广播的rpc时间不能超过选举超时时间，即可保证上次选举的内容不会影响下一次的！
9. lab rpc，每个请求有0-27ms的延迟，有10%的概率丢包，100ms服务器没有响应，就认为服务器挂了，具体很复杂，最大延迟7s或2s
10. 因为rpc超时时间选择后，一定小于选举超时时间，所以就可以超时的时候仍然doneRPCs+1,就不要waitGroup了！
11. muteX是不可重入的锁！，必须外部加锁，util.go里面不会加锁！所以加锁的级别为callback和rpc handler
12. channel没有make会导致死锁
13. rpc请求响应字段需要大写。。
14. 