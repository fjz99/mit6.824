cd /mnt/e/mit6.824/mapreduce/src || exit
go build -buildmode=plugin -o ./target/wc.so ./mrapps/wc.go
go run ./main/mrsequential.go ./target/wc.so ./main/pg*.txt

