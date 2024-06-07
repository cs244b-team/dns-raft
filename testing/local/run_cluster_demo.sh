#! /bin/zsh -
cd cmd/server && go build .
cd ../client && go build .
cd ../..

rm -rf /tmp/*.logents /tmp/*.state
(trap - INT QUIT; go run cmd/server/main.go --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 0 --port 53) > /tmp/node0.log 2>&1  &
(trap - INT QUIT; go run cmd/server/main.go --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 1 --port 8054) > /tmp/node1.log 2>&1 &
go run cmd/server/main.go --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 2 --port 8055 > /tmp/node2.log 2>&1
wait
