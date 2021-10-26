#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT
cd ../src/raft
# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
chmod +x ../../.run/testLab2.sh
rm -rf failedTests
mkdir failedTests

for i in $(seq 1 "$runs"); do
    timeout -k 2s 10s ../../.run/testLab2B.sh &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL "$i"
        cp e.log failedTests/e-"$i".log
    else
        echo '***' SUCCESS IN TRIAL "$i"
    fi
done

echo '*** DONE ***'
