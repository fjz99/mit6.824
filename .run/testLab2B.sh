#!/usr/bin/env bash
#cd ../src/raft
a=$(VERBOSE=1 go test -race -run RPCBytes2B > e.log && tail -n 1 e.log | grep -o ok)
#a=$(VERBOSE=1 go test -race -run ReElection2A > e.log && tail -n 1 e.log | grep -o ok)
if [ "$a" != 'ok' ]; then
  echo "failed on $a"
  exit 1
fi
