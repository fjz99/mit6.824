package shardctrler

import (
	"6.824/raft"
	"sort"
)

const DefaultReplicas = 50 //每个group的节点个数
const IdStringLength = 18  //随机key的长度

type LoadBalancer interface {
	RemoveGroup(GID int) []int
	Move(GID int, shardId int) []int
	GetAssignArray() []int
	AddGroup(GID int) []int
}

type SimpleLoadBalancer struct {
	GroupAssignMap map[int][]int
	shardNum       int
	GroupNum       int
	AssignArray    []int
}

func MakeSimpleLoadBalancer(shardNum int) LoadBalancer {
	lb := &SimpleLoadBalancer{}
	lb.shardNum = shardNum
	lb.GroupNum = 0
	lb.AssignArray = make([]int, shardNum)
	lb.GroupAssignMap = map[int][]int{}
	raft.SetArrayValue(lb.AssignArray, -1)
	return lb
}

func (lb *SimpleLoadBalancer) Move(GID int, shardId int) []int {
	if shardId < 0 || shardId >= lb.shardNum {
		return nil
	}
	if _, ok := lb.GroupAssignMap[GID]; !ok {
		panic("")
	}

	maxNum := lb.getMaxNum()
	arr := lb.GetAssignArray()
	targetId := arr[shardId]
	lb.GroupAssignMap[targetId] = DeleteArrayValue(lb.GroupAssignMap[targetId], shardId)
	lb.GroupAssignMap[GID] = append(lb.GroupAssignMap[GID], shardId)
	if len(lb.GroupAssignMap[GID]) > maxNum {
		//交换元素
		value := lb.GroupAssignMap[GID][0]
		lb.GroupAssignMap[GID] = DeleteArrayIndex(lb.GroupAssignMap[GID], 0)
		lb.GroupAssignMap[targetId] = append(lb.GroupAssignMap[targetId], value)
	}
	return lb.GetAssignArray()
}

func (lb *SimpleLoadBalancer) RemoveGroup(GID int) []int {
	if _, ok := lb.GroupAssignMap[GID]; !ok {
		panic("")
	}
	arr := lb.GroupAssignMap[GID]
	delete(lb.GroupAssignMap, GID)
	lb.GroupNum--
	lb.adjust(arr)
	return lb.GetAssignArray()
}

func (lb *SimpleLoadBalancer) GetAssignArray() []int {
	res := make([]int, lb.shardNum)
	for k, v := range lb.GroupAssignMap {
		for _, p := range v {
			res[p] = k
		}
	}
	return res
}

func (lb *SimpleLoadBalancer) AddGroup(GID int) []int {
	if _, ok := lb.GroupAssignMap[GID]; ok {
		panic("")
	}
	lb.GroupNum++
	lb.GroupAssignMap[GID] = []int{}
	if lb.GroupNum == 1 {
		freeShard := make([]int, lb.shardNum)
		for i := 0; i < lb.shardNum; i++ {
			freeShard[i] = i
		}
		lb.adjust(freeShard)
	} else {
		lb.adjust([]int{})
	}
	return lb.GetAssignArray()
}

func (lb *SimpleLoadBalancer) getMaxNum() int {
	newNum := -1 //最大值
	if lb.shardNum%lb.GroupNum == 0 {
		newNum = lb.shardNum / lb.GroupNum
	} else {
		newNum = lb.shardNum/lb.GroupNum + 1
	}
	return newNum
}

//对assign map进行平整
func (lb *SimpleLoadBalancer) adjust(freeShard []int) {
	if lb.GroupNum == 0 {
		return
	}
	newNum := lb.shardNum / lb.GroupNum
	numMap := map[int][]int{}
	var keys []int
	var nums []int
	for k, v := range lb.GroupAssignMap {
		if _, ok := numMap[len(v)]; !ok {
			nums = append(nums, len(v)) //保证不重复
		}
		keys = append(keys, k)
		numMap[len(v)] = append(numMap[len(v)], k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(nums))) //.......
	sort.Ints(keys)

	for _, p := range nums {
		sort.Ints(numMap[p])
		for _, v := range numMap[p] {
			arr := lb.GroupAssignMap[v]
			sort.Ints(freeShard)
			sort.Ints(arr)
			if len(arr) > newNum {
				free := len(arr) - newNum
				freeShard = append(freeShard, arr[:free]...)
				if free < len(lb.GroupAssignMap[v]) {
					lb.GroupAssignMap[v] = arr[free:]
				} else {
					lb.GroupAssignMap[v] = []int{}
				}
			}
			if len(arr) < newNum && len(freeShard) > 0 {
				free := newNum - len(arr)
				lb.GroupAssignMap[v] = append(lb.GroupAssignMap[v], freeShard[:free]...)
				if free < len(freeShard) {
					freeShard = freeShard[free:]
				} else {
					freeShard = []int{}
				}
			}
		}
	}
	sort.Ints(freeShard)
	index := 0
	//剩余的添加进去
	for _, k := range keys {
		//注意map的无序性
		if index >= len(freeShard) {
			break
		}
		lb.GroupAssignMap[k] = append(lb.GroupAssignMap[k], freeShard[index])
		index++
	}
	Assert(len(freeShard) == lb.shardNum%lb.GroupNum, "")
}
