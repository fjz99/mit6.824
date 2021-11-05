package shardctrler

import (
	"fmt"
	"testing"
)

func TestHash1(t *testing.T) {
	ring := MakeHashRing(10)
	fmt.Println(ring.AddGroup(1))
	fmt.Println(ring.AddGroup(2))
	fmt.Println(ring.AddGroup(3))
}

func TestHash2(t *testing.T) {
	ring := MakeHashRing(10)
	fmt.Println(ring.AddGroup(1))
	fmt.Println(ring.AddGroup(2))
	fmt.Println(ring.Move(1, 0))
	fmt.Println(ring.Move(2, 1))
	fmt.Println(ring.AddGroup(3))
	fmt.Println(ring.RemoveGroup(1))
}

func TestHash3(t *testing.T) {
	ring := MakeHashRing(10)
	fmt.Println(ring.AddGroup(1))
	fmt.Println(ring.AddGroup(2))
	fmt.Println(ring.RemoveGroup(1))
	fmt.Println(ring.AddGroup(3))
	fmt.Println(ring.RemoveGroup(3))
	fmt.Println(ring.AddGroup(1))
}
