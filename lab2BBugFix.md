1. 日志提交的时候，刚被选举为leader的节点会发送空entry，此时可能会和第一轮心跳冲突，不过无所谓
2. 可能一个节点选举完了，成了leader，然后发送心跳，此时如果选举太快的话，没有达到rpc超时时间，此时就存在错误改动doneRpcs的可能，比如选举完成后会发送空entry，就会
受影响，解决办法：选举成功后，发送心跳，然后，sleep掉超时时间。
3. 选举之间的计数干扰用rpc超时时间控制，选举和log的干扰（donerpcs，和cond对象）用sleep解决？log和log的干扰？
4. 判断日志提交成功的办法，设置一个超时时间，如果超时，那就提交失败，否则，不断检查matchIndex，如果过半超过目标index，就行，或者检查commitIndex
5. 具体实现思路，一个COnd对象，监听commitId的变化，start的时候，会给发送队列添加任务，然后超时等待在commitId上，每次被唤醒都检查这个id
发送任务返回时，会修改matchId和nextId，matchId修改后，直接循环检查过半问题，如果过半，就修改commitId并唤醒
发送任务循环发送时，直接取出所有发送队列的任务，因为发送循环发送的时候，一定可以按序同步完成，此时，心跳和其他的发送任务都可以忽略
6. 心跳也可以修改commitId，毕竟提交完之后，只剩下了心跳可以修改commitId
7. basicAgreement的测试，可能遇到还没来得及发送空entry就有一个agreement来了的情况。。此时index就不对了；见e-116.log
解决办法，判断前驱，必须是term=现在，否则就在cond等待commit,为了保证后续任务的有序性，需要添加一个缓冲队列。。
8. 机器莫名卡顿，导致bug。。因为超时依赖于物理时钟，一卡顿，时钟漂移，直接超时了。。
9. e-67是一个非常罕见且有意思的情况，1 2时钟漂移，同时超时，1选为leader，而1的commitIndex=2，但是源leader是3，但是源leader被废黜，但是1有所有的log（因为选为leader的条件）
但是1的commitId不是日志最后一个位置，即这是figure8的情况！此时，follower的commitId比leader大！
10. go test的IDEA环境下，随机数是静止的！所以不要用GoLand！，用命令行！
11. 同步初始化，初始化的日志提交是一起的，或者使用论文中的，判断提交的term
12. commit条件，term，半数,防止本term提交之前term的日志，即fig.8，这样的话，每次选为leader之后，只需提交一个空entry，因为空entry可能导index后退，
此时需要校验！防止本term提交之前term的日志，即fig.8
13. 日志策略，发送线程LOG2，选举VOTE，log提交COMMIT，快照SNAPSHOT，其他不重要日志（DEBUG）LOG1，必看LEADER，INFO必看，是通用信息；
14. leader降级为follower了的时候,清空发送队列,同样，重试的时候，sender线程也会判断当前状态，防止leader被降级后一直发送日志
即folower不能发送数据，验证一下，防止多次发送无意义的数据
15. 具体check test的流程：从chan中读取，读取一个之后，检查提交的前驱是否正确，
首先调用cfg.checkLogs（）
对于每个chan中的提交数据，检查找所有节点的command index下的command，假如存在command，command还不同，就错误："commit Index=%v server=%v %v != server=%v %v"
然后检查提交数据的前驱节点是否存在，不存在，就报错"server %v apply out of order %v"
one函数，会检查那个是leader，然后根据start返回的index，来检查有几个完成了提交
chan中发送的数据的index会放到测试程序log数组中的对应位置，用于测试
16. 我的nil没发送，所以导致out of order，而加了nil为了保证index从1开始。。
17. **所以说，nil无所谓，测试用例的index必须从1开始，并且发送nil，否则就错误**
此外，还要求nil不占index。。解决办法是用我的index-term，应为term从0开始，所以再-1
18. 很坑，leader节点可能是重新选举的，所以commitId不是满的，但是选举结束后会发送一个nil entry，此时，顺便把前面的也提交了
但是此时，需要在start方法中，顺便把前面的也发送到chan中，否则永远无法完成任务
19. 即使提交失败，返回值也是应该的index，即主节点日志添加的位置！，即，除非节点没有响应，否则都能提交成功。即使受到响应然后提交失败，就意味着没有过半！
20. 