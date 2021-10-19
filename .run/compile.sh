cd /mnt/e/mit6.824/src || exit
go build -race -buildmode=plugin ../src/mrapps/wc.go
go build -race ../src/main/mrcoordinator.go
go build -race ../src/main/mrworker.go
echo "done"