#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
cd ../src/shardkv
# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
chmod +x ../../.run/testLab4B.sh
rm -rf failedTests
mkdir failedTests
#!!!!!!!!!!!!!!!!!!!!1 10s
for i in $(seq 1 "$runs"); do
    timeout -k 2s 50s ../../.run/testLab4B.sh &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL "$i"
        cp ee.log failedTests/ee-"$i".log
    else
        echo '***' SUCCESS IN TRIAL "$i"
    fi
done

echo '*** DONE ***'
