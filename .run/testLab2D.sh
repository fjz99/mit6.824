#!/usr/bin/env bash
#cd ../src/raft
#a=$(VERBOSE=1 go test -race -run Backup2B > e.log && tail -n 1 e.log | grep -o ok)
a=$(VERBOSE=1 go test -race -run Rejoin2B > ed.log && tail -n 1 ed.log | grep -o ok)
#VERBOSE=0 go test -race -run 2A | tee e.log
#a=$(tail -n 1 e.log | grep -o ok) #输出到命令行
#echo xx$a
if [ "$a" != 'ok' ]; then
  echo "failed on $a"
  exit 1
fi
