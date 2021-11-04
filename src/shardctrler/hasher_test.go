package shardctrler

import (
	"fmt"
	"testing"
)

func TestHash_AddGroup(t *testing.T) {
	ring := MakeHashRing(10)
	fmt.Println(ring.AddGroup(1))
}
