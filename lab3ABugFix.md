1. get只读的操作有2种实现，1是写入日志，然后读，2是心跳过半，然后读，2性能好 2的方式需要改动raft，实现广播轮次功能
2. 因为存在失败的可能，如何判断？等待commitId超过index，如果超时就失败，此外，如果消费的index的log entry和插入的不同， 也算失败（因为可能提交失败，然后这个log被新的leader清除，也可以判断state？）
3. 使用日志index作为client标识符
4. 序列号带累计确特性，所以只需要保存最后一个和返回即可
5. 目前暂时不进行会话超时！
6. 这些id推荐使用uint64
7. 使用map保存对应index（状态机级别的index，从1开始）的输出
8. 关于并发： 对于lab2而言，start的并发调用（测试用例）是不要求有序的，比如并发调用1 2 3，实际为2 3 1也行，只要保证副本一致性就行
   对于lab3而言，客户端都是阻塞客户端，所以客户端内都是有序的，而且也不存在乱序问题，只有丢失和重复，所以直接保存序列号校验即可 而如果使用非阻塞客户端的话，就要复杂很多，需要使用队列+发送批次的第一个序列号，来保证有序性
   对于客户端并发而言，保证的是线性一致性，即读提交，是不可重复读。即客户端1的put方法返回ok之后，客户端2的get必须是最新值，但是如果客户端2的get发出的时候，客户端1的 put正在执行，那就说不准，可能是新值也可能是旧值。
9. 坑：这个没法通过重定向完成leader查找，只能通过轮询。。因为endpoint都是随机shuffle了
10. kv.commitIndexCond.Wait() //这里的wait无法释放所有的重入的lock，只会释放一层。。
11. 很诡异的锁问题，在client中，go导致的线程是异步的，导致无法决定它一定能获得锁，可能第一个锁被抢占了，解决办法：把client注册的判断放到put等方法中
12. 注意在waitFor返回之后，可能状态已经不是leader了，即此时的output中存放的并不是之前的那个请求结果，此时不应该读取对应index的输出，而应该返回errWrongLeader
同样地，waitFor方法应该有个超时时间，避免网络分区造成无法过半，导致这个请求一直无法执行，从而一直卡死在wait处，即使客户端的rpc超时返回了，服务器还会卡在这里。。
（即特殊一点，一个leader的某个index的日志因为网络分区无法提交，然后他被后来的leader截断日志了，此时假如没有新的commit的话，就会导致死锁。。）
可以考虑加一个线程，每隔一段时间就唤醒cond一次。。
13. matchIndex只会增长的问题：follower自己的matchIndex需要在term修改是重新设置为commitId！或者不用macthIndex来更新commitId；
可以在发送心跳的时候，pervIndex=leader存储的follower marchIndex，这样的话，follower根据这个和自己的commitId和leader的commitId来更新自己的commitId
正常的日志复制请求中，可以通过prevIndex+len of args log和自己的commitId和leader的commitId来更新自己的commitId
14. 一个节点认为自己是leader，但是网络分区了，此时我打代码会无限等待，直到分区结束为止
15. 快照必须能够保证可以继续检测重复seqId
对于普通的crash重启，状态机会自动重新执行log，commitId会重新增长，所以也能够执行register，来保存session。这也说明了，必须是由状态机来保存session

