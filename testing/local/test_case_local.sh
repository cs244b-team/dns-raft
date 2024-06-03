#!/bin/bash

killwait() {
    kill $1 2> /dev/null && wait $1 2> /dev/null
}

cleanup() {
  killwait $PID_NODE0
  killwait $PID_NODE1
  killwait $PID_NODE2
}

trap cleanup EXIT

# Remove specified files
rm -rf /tmp/*.logents /tmp/*.state /tmp*.log

# node 0
./cmd/server/server --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 0 --port 7000 > /tmp/node0.log 2>&1 &
PID_NODE0=$!

# node 1
./cmd/server/server --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 1 --port 7001 > /tmp/node1.log 2>&1 &
PID_NODE1=$!

# node 2
./cmd/server/server --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 2 --port 7002 > /tmp/node2.log 2>&1 &
PID_NODE2=$!

sleep 3

# Find the leader
LEADER_IP=""
LEADER_ID=""
for i in {0..2}; do
  if grep -q "converting to leader" /tmp/node${i}.log; then
    LEADER_IP="127.0.0.1:700${i}"
    LEADER_ID=${i}
    break
  fi
done

if [ -z "$LEADER_IP" ]; then
  echo "No leader found. Exiting..."
  exit 1
else
  echo "Leader is node-$LEADER_ID"
fi

# Issue first request through leader
./cmd/client/client -domain www.example.com. -server $LEADER_IP --eval > /dev/null 2>&1  &
TMP_CLIENT=$!
sleep 1
killwait $TMP_CLIENT

# Kill the leader node
echo "Killing leader node with ID $LEADER_ID"
if [ "$LEADER_ID" -eq 0 ]; then
  killwait $PID_NODE0
  ./cmd/server/server --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 0 --port 7000 &
  PID_NODE0=$!
elif [ "$LEADER_ID" -eq 1 ]; then
  killwait $PID_NODE1
  ./cmd/server/server --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 1 --port 7001 &
  PID_NODE1=$!
elif [ "$LEADER_ID" -eq 2 ]; then
  killwait $PID_NODE2
  ./cmd/server/server --node 127.0.0.1:7000 --node 127.0.0.1:8000 --node 127.0.0.1:9000 -id 2 --port 7002 &
  PID_NODE2=$!
fi

sleep 10

# Issue another request to that same node, who may or may not still be the leader
./cmd/client/client -domain www.example.com. -server $LEADER_IP --eval &
TMP_CLIENT=$!
sleep 2
killwait $TMP_CLIENT


sleep 5