1. 特殊情况return了，但是没有释放锁，导致无限等待。可以考虑用defer，稳健。。
2. RPC必须提供err返回值，和2个参数，并且参数必须是指针，除非是普通变量
3. Var *不会分配内存，所以会段错误
4. struct的field也要大写。。
5. %#v表示输出类型
6. append函数只会在数组后面添加，如果make了一个数组的话，就会在最后添加，所以不要make！！！！
7. 而map必须make
8. 注意指针，尽量用指针，除了简单类型！！
9. labgob warning: Decoding into a non-default variable/field Id may not work
   即rpc的请求必须每个字段都是默认值，否则可能不会解析！
10. 输出的output中的文件路径不一定是有序的，因为hash的返回值+mod的结果不是有序的，所以需要排序，然后再加入对应mod的input列表中
11. go 的map是无序的，是hash的













