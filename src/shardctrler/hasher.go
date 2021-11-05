package shardctrler

import (
	"6.824/raft"
	"math/rand"
	"sort"
	"time"
)

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
	Shards        map[int]uint32 //每个分片的随机key字符串，固定不变的
	ReverseShards map[uint32]int
	ShardNum      int              //分片个数
	GroupMap      map[int][]uint32 //GID -> 随机key字符串哈希值
	ReverseGroup  map[uint32]int
	fixedArray    []int //表示规定某个shard为某个GID
}

func MakeHashRing(shardNum int) LoadBalancer {
	hash := Hash{}
	hash.ShardNum = shardNum
	hash.SortKeys = SortKeys{}
	hash.GroupMap = map[int][]uint32{}
	hash.Shards = map[int]uint32{}
	hash.ReverseGroup = map[uint32]int{}
	hash.ReverseShards = map[uint32]int{}
	hash.fixedArray = make([]int, shardNum)
	raft.SetArrayValue(hash.fixedArray, -1)
	rand.Seed(time.Now().UnixNano())
	//生成随机字符串
	for i := 0; i < shardNum; i++ {
		key := randomString(IdStringLength)
		hashString := HashString(key)
		hash.Shards[i] = hashString
		hash.ReverseShards[hashString] = i
		hash.SortKeys = append(hash.SortKeys, hashString)
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
		hashString := HashString(key)
		hs.GroupMap[GID] = append(hs.GroupMap[GID], hashString)
		hs.ReverseGroup[hashString] = GID
		hs.SortKeys = append(hs.SortKeys, hashString)
	}

	return hs.GetAssignArray()
}

func (hs *Hash) reAppendKeys() {
	hs.SortKeys = SortKeys{}
	for _, v := range hs.Shards {
		hs.SortKeys = append(hs.SortKeys, v)
	}
	for _, v := range hs.GroupMap {
		for _, p := range v {
			hs.SortKeys = append(hs.SortKeys, p)
		}
	}
	sort.Sort(hs.SortKeys)
}

func (hs *Hash) GetAssignArray() []int {
	sort.Sort(hs.SortKeys)
	var arr []int
	for i := 0; i < hs.ShardNum; i++ {
		arr = append(arr, 0)
	}

	for k, v := range hs.Shards {
		if hs.fixedArray[k] != -1 {
			arr[k] = hs.fixedArray[k]
			continue
		}
		pos := hs.findPos(v)
		key := hs.SortKeys[pos]
		if !hs.isShard(key) {
			GID := hs.ReverseGroup[key]
			arr[k] = GID
		}
	}
	return arr
}

func (hs *Hash) isShard(key uint32) bool {
	if _, ok := hs.ReverseShards[key]; ok {
		return true
	}
	if _, ok := hs.ReverseGroup[key]; ok {
		return false
	}
	panic(1)
}

func (hs *Hash) RemoveGroup(GID int) []int {
	for i := 0; i < hs.ShardNum; i++ {
		if hs.fixedArray[i] == GID {
			hs.fixedArray[i] = -1
		}
	}
	for _, p := range hs.GroupMap[GID] {
		delete(hs.ReverseGroup, p)
	}
	delete(hs.GroupMap, GID)
	hs.reAppendKeys()
	return hs.GetAssignArray()
}

func (hs *Hash) Move(GID int, shardId int) []int {
	hs.fixedArray[shardId] = GID
	return hs.GetAssignArray()
}

//查找gid对应的node id
func (hs *Hash) findPos(h uint32) int {
	//查找func为true的最小index
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
		if !hs.isShard(hs.SortKeys[index]) {
			return index
		}
	}
	panic(1)
}
