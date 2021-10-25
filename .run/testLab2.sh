VERBOSE=1 go test -race -run ReElection2A > e.log && tail -n 1 e.log | grep ok
