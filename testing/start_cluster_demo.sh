#!/bin/zsh
set -e

# Kill cluster
for i in $(seq 0 4);
do
    sh ./testing/kill_node_demo.sh $i
done

# Start cluster
for i in $(seq 0 4);
do
    sh ./testing/start_node_demo.sh $i
done
