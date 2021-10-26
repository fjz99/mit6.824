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