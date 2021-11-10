#!/usr/bin/env bash
#cd ../src/raft
#a=$(VERBOSE=1 go test -race -run Concurrent1 > e.log && tail -n 1 e.log | grep -o ok)
a=$(VERBOSE=1 go test -race -run Concurrent2 > ee.log && tail -n 1 ee.log | grep -o ok)
#VERBOSE=0 go test -race -run 2A | tee e.log PersistOneClient3A
#a=$(tail -n 1 e.log | grep -o ok) #输出到命令行
#echo xx$a Speed3A Speed3B
if [ "$a" != 'ok' ]; then
  echo "failed on $a"
  exit 1
fi
