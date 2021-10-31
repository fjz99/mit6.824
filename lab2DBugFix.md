1. 快照由主节点(的状态机)创建，因为快照保存的是节点的状态，然后install给其他人，所以这个东西错误也会重试，但是只发送一次
注意由此带来的index的改动
follower接收到快照的时候，会发送到chan中，从而状态机就可以获得这个快照，调用condInstall方法，通知raft切换到这个快照中
当然，在这之间，如果crash了，就很危险，因为可能log截断了，但是快照没持久化，或者快照持久化了，但是日志没截断，所以启动的时候需要检查一下，持久化的时候优先持久化快照
2. InstallSnapshot RPC的执行过程和其他的不一样，注意如果找不到前驱的话，就直接丢弃所有的log
3. snapshotIndex指的是快照的最后一个位置，当然初始值是-1
4. index-- //因为状态机是从1开始的。。
5. 既然leader和follower都可以创建快照，那么snapshot方法就不必广播快照了，只需在发送日志的时候根据情况发送快照即可
6. InstallSnapshot RPC的应用场景是follower太慢了
7. https://www.cnblogs.com/sun-lingyu/p/14591757.html
8. 因此，Frans Kaasoek教授在讲解2A/2B时，特意提到不要在向applyCh发送log entry时持有锁。在他的实现中，向applyCh发送是采用一个专门goroutine执行来确保串行的。
因为测试用例中每隔几个日志就会生成快照，此时会阻塞，等待快照生成完
9. 不用更新commitId，因为快照本身就是在commitId内的，由状态机生成的
10. 发送快照失败，自动重试的时候，需要使用最新的快照..
