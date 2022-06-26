#! /bin/zsh

rm wc.so
rm mr-%-%
rm mr-out*



(go run -race mrcoordinator.go pg-*.txt &)

sleep 1

go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrworker.go wc.so



