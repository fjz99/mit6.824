package shardctrler

import (
	"math/rand"
	"sort"
	"time"
)

const DefaultReplicas = 3 //每个group的节点个数
const IdStringLength = 18 //随机key的长度
type SortKeys []uint32

func (sk SortKeys) Len() int {
	return len(sk)
}

func (sk SortKeys) Less(i, j int) bool {
	return sk[i] < sk[j]
}

func (sk SortKeys) Swap(i, j int) {
	sk[i], sk[j] = sk[j], sk[i]
}

type Hash struct {
	SortKeys      SortKeys       //排序后的key，包括shard和group
	Shards        map[int]string //每个分片的随机key字符串，固定不变的
	ReverseShards map[string]int
	ShardNum      int              //分片个数
	GroupMap      map[int][]string //GID -> 随机key字符串
	ReverseGroup  map[string]int
	Hash2String   map[uint32]string
}

func MakeHashRing(shardNum int) *Hash {
	hash := Hash{}
	hash.ShardNum = shardNum
	hash.SortKeys = SortKeys{}
	hash.GroupMap = map[int][]string{}
	hash.Shards = map[int]string{}
	hash.ReverseGroup = map[string]int{}
	hash.ReverseShards = map[string]int{}
	hash.Hash2String = map[uint32]string{}
	rand.Seed(time.Now().UnixNano())
	//生成随机字符串
	for i := 0; i < shardNum; i++ {
		key := randomString(IdStringLength)
		hash.Shards[i] = key
		hash.ReverseShards[key] = i
		hash.SortKeys = append(hash.SortKeys, HashString(key))
		hash.Hash2String[HashString(key)] = key
	}
	sort.Sort(hash.SortKeys)
	return &hash
}

func (hs *Hash) AddGroup(GID int) []int {
	if _, ok := hs.GroupMap[GID]; ok {
		panic("GID 已存在")
	}

	for i := 0; i < DefaultReplicas; i++ {
		key := randomString(IdStringLength)
		hs.GroupMap[GID] = append(hs.GroupMap[GID], key)
		hs.ReverseGroup[key] = GID
		hs.Hash2String[HashString(key)] = key
		hs.SortKeys = append(hs.SortKeys, HashString(key))
	}

	return hs.getAssignArray()
}

func (hs *Hash) reAppendKeys() {
	hs.SortKeys = SortKeys{}
	for _, v := range hs.Shards {
		hs.SortKeys = append(hs.SortKeys, HashString(v))
	}
	for _, v := range hs.GroupMap {
		for _, p := range v {
			hs.SortKeys = append(hs.SortKeys, HashString(p))
		}
	}
	sort.Sort(hs.SortKeys)
}

func (hs *Hash) getAssignArray() []int {
	sort.Sort(hs.SortKeys)
	var arr []int
	for i := 0; i < hs.ShardNum; i++ {
		arr = append(arr, 0)
	}

	for k, v := range hs.Shards {
		pos := hs.findPos(v)
		key := hs.Hash2String[hs.SortKeys[pos]]
		if !hs.isShard(key) {
			GID := hs.ReverseGroup[key]
			arr[k] = GID
		}
	}
	return arr
}

func (hs *Hash) isShard(key string) bool {
	if _, ok := hs.ReverseShards[key]; ok {
		return true
	}
	if _, ok := hs.ReverseGroup[key]; ok {
		return false
	}
	panic(1)
}

func (hs *Hash) RemoveGroup(GID int) {

}

func (hs *Hash) Assign(GID int, shardId int) {

}

//查找gid对应的node id
func (hs *Hash) findPos(groupKey string) int {
	//查找func为true的最小index
	h := HashString(groupKey)
	index := 0
	for ; index < len(hs.SortKeys); index++ {
		if h == hs.SortKeys[index] {
			break
		}
	}
	if index == len(hs.SortKeys) {
		panic(1)
	}
	for i := 0; i < len(hs.SortKeys); i++ {
		index = (index + 1) % len(hs.SortKeys)
		if !hs.isShard(hs.Hash2String[hs.SortKeys[index]]) {
			return index
		}
	}
	panic(1)
}
