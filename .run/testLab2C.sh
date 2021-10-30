#!/usr/bin/env bash
#cd ../src/raft
a=$(VERBOSE=1 go test -race -run UnreliableAgree2C > ec.log && tail -n 1 ec.log | grep -o ok)
#a=$(VERBOSE=1 go test -race -run Figure8Unreliable2C > ec.log && tail -n 1 ec.log | grep -o ok)
if [ "$a" != 'ok' ]; then
  echo "failed on $a"
  exit 1
fi
