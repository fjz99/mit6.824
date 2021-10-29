1. 日志最好有lru等缓冲区
2. voteFor=me并不能说明现在是leader！可能是follower！
3. log数组也需要持久化
4. 注意持久化的时候，是生成byte数组，所以要求序列化和反序列化的顺序相同，比如都是先term再voteFor等