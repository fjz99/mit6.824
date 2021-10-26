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
6. 选举超时时间为随机数，250-500ms
7. 另一种实现，for + sleep
8. 须保证选举广播的rpc时间不能超过选举超时时间，即可保证上次选举的内容不会影响下一次的！
9. lab rpc，每个请求有0-27ms的延迟，有10%的概率丢包，100ms服务器没有响应，就认为服务器挂了，具体很复杂，最大延迟7s或2s
10. 因为rpc超时时间选择后，一定小于选举超时时间，所以就可以超时的时候仍然doneRPCs+1,就不要waitGroup了！
11. muteX是不可重入的锁！，必须外部加锁，util.go里面不会加锁！所以加锁的级别为callback和rpc handler
12. channel没有make会导致死锁
13. rpc请求响应字段需要大写。。
14. 降低rpc超时时间之后，就导致心跳经常超时，然后就导致测试时间很长，因为引起了再一次选举
15. 心跳只广播一次就行,不用一直广播，还会占用网络流量
16. 这里，假如选为leader之后，在init的地方for循环多次发送的话，因为外部加的是for循环整体的锁，而回调函数需要锁，所以阻塞了所有的回调函数，导致发送队列阻塞
17. 心跳间隔是100ms，如果rpc超时时间太长的话（超过心跳间隔时间），就会导致心跳失败，所以需要2个超时机制，
一个是单轮rpc的，超时重试，一个是总体的，防止选举直接互相关联，选举的是要取最小值250ms，或者手动控制超发次数，比如一次50ms，超发2次，刚好赶上心跳
或者设定总体超时时间，然后就会一直重试
18. follower状态下，先go broadcastVote了，然后进入candidate状态，select，此时收到异步的心跳rpc了，就会导致变成follower状态，但是却开始选举了
解决办法，借助互斥性，在加锁完成后，校验是否是candidate状态，只要加锁成功，再次校验超时，如果超时，就进行选举，后面的超时，对应，A-1.log
19. raft.go（git cd604c47） 420行，不要voteFor=-1！，因为可能出现原因是，发送线程异步执行，在一个leader存在时，正在发送心跳，
后来网络不好，收不到心跳，2个follower变成candidate，从而返回term+1，导致leader被降级，此时如果设置vote=-1的话，就会多次投票，因为，
简单说，被降级可能是因为受到投票导致的，此时不能设置vote=-1，如果是心跳导致的，就没事
20. 超时时间校验短一些，否则可能超过很久也没心跳，此时还很久才超时;但是因为测试需要，测试的时候，在初始化完之后，会等一会才测试，导致一旦减少超时检测间隔，
就会导致所有节点一起选举。此外，如果固定50ms检测一次的话，就会导致450和455同时进入candidate，会活锁
21. 现在看来，状态转换chan的作用未知，唯一的作用就是使主循环状态转换及时！
22. 选举超时时间随机数给一个间隔，保证不重复，否则出现诡异bug，见e-15.log，对应的是2个节点超时时间一样，而另一个小，成为了leader，此时网络分区了。。
那两个节点同时进入candidate，导致活锁，无法选出leader;如果偶尔重复了，就不算错误！
23. labA2的测试中，必须保证随机的超时时间不同，否则很容易活锁
24. e-136.log chan死锁问题
25. 只有一个节点的情况下，会死锁，因为wait，所以应该wait之前先判断是否过半
26. 重试投票请求的时候，是重新加到队列后面的，此时可以校验，如果队列有东西，说明一定是心跳，此时就不重试了
27. 日志提交的时候也是，如果要发心跳了，而此时队列里还有东西，就没必要发送心跳了，因为日志提交也有心跳的功能
28. 在给多个线程分发任务的时候，必须保证每个线程获得的任务也是内存重分配的。。
29. 存在超时问题，时限为10s，修改选举超时时间！
30. 最终结果为，选举超时200-350，rpc 80超时，均不重发，一次超时时间内，发一次心跳，一次投票请求就行！
31. Remember, however, that leader election may require multiple rounds in case of a split vote 
(which can happen if packets are lost or if candidates unluckily choose the same random backoff times). 
即，guide说了，不用重试投票请求！，只需要多轮投票就行！