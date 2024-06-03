#!/bin/zsh
set -e

NODE_0_EXTERNAL_IP=34.168.204.145
NODE_1_EXTERNAL_IP=35.203.169.53
NODE_2_EXTERNAL_IP=35.203.184.108
NODE_3_EXTERNAL_IP=104.199.119.116
NODE_4_EXTERNAL_IP=35.233.157.154

NODES=(
    $NODE_0_EXTERNAL_IP
    $NODE_1_EXTERNAL_IP
    $NODE_2_EXTERNAL_IP
    $NODE_3_EXTERNAL_IP
    $NODE_4_EXTERNAL_IP
)

# Kill cluster
for i in $(seq 0 4);
do
    echo "Running command on node-$i"
    ssh -i ~/.ssh/id_ed25519_gh -o StrictHostKeyChecking=no user@${NODES[i]} $@
done
