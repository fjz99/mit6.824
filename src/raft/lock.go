package raft

import "sync"

//实现可重入的锁

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

type ReentrantLock struct {
	mu        *sync.Mutex
	cond      *sync.Cond
	owner     int
	holdCount int
}

func NewReentrantLock() sync.Locker {
	rl := &ReentrantLock{}
	rl.mu = new(sync.Mutex)
	rl.cond = sync.NewCond(rl.mu)
	return rl
}

func GetGoroutineId() int {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("panic recover:panic info:%v\n", err)
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func (rl *ReentrantLock) Lock() {
	me := GetGoroutineId()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.owner == me {
		rl.holdCount++
		return
	}
	for rl.holdCount != 0 {
		rl.cond.Wait()
	}
	rl.owner = me
	rl.holdCount = 1
}

func (rl *ReentrantLock) Unlock() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.holdCount == 0 || rl.owner != GetGoroutineId() {
		panic("illegalMonitorStateError")
	}
	rl.holdCount--
	if rl.holdCount == 0 {
		rl.cond.Signal()
	}
}
