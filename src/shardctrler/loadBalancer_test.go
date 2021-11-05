package shardctrler

import (
	"fmt"
	"testing"
)

func TestADD(t *testing.T) {
	lb := MakeSimpleLoadBalancer(10)
	fmt.Println(lb.GetAssignArray())
	fmt.Println(lb.AddGroup(1))
	fmt.Println(lb.AddGroup(2))
	fmt.Println(lb.AddGroup(3))
	fmt.Println(lb.AddGroup(4))
	fmt.Println(lb.AddGroup(5))
}

func TestRemove(t *testing.T) {
	lb := MakeSimpleLoadBalancer(10)
	fmt.Println(lb.GetAssignArray())
	fmt.Println(lb.AddGroup(1))
	fmt.Println(lb.AddGroup(2))
	fmt.Println(lb.AddGroup(3))
	fmt.Println(lb.RemoveGroup(3))
	fmt.Println(lb.RemoveGroup(1))
	fmt.Println(lb.AddGroup(1))
}

func TestMove(t *testing.T) {
	lb := MakeSimpleLoadBalancer(10)
	fmt.Println(lb.GetAssignArray())
	fmt.Println(lb.AddGroup(1))
	fmt.Println(lb.AddGroup(2))
	fmt.Println(lb.Move(2, 9))
	fmt.Println(lb.Move(2, 8))
	fmt.Println(lb.Move(2, 0))
	fmt.Println(lb.AddGroup(3))
	fmt.Println(lb.RemoveGroup(1))
}

func TestName(t *testing.T) {
	for i := 0; i < 2; i++ {
		println("iters=", i)
		lb := MakeSimpleLoadBalancer(10)
		fmt.Println(lb.GetAssignArray())
		fmt.Println(lb.AddGroup(1))
		fmt.Println(lb.AddGroup(2))
		fmt.Println(lb.AddGroup(3))
		fmt.Println(lb.RemoveGroup(3))
		fmt.Println(lb.RemoveGroup(2))
		for j := 0; j < 5; j++ {
			fmt.Println(lb.AddGroup(10 + j))
		}
		for j := 0; j < 5; j++ {
			fmt.Println(lb.RemoveGroup(10 + j))
		}
	}
}
