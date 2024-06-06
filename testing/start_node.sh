#!/bin/zsh
set -e

NODE_0_INTERNAL_IP=10.138.0.2
NODE_1_INTERNAL_IP=10.138.0.3
NODE_2_INTERNAL_IP=10.138.0.4
NODE_3_INTERNAL_IP=10.138.0.5
NODE_4_INTERNAL_IP=10.138.0.6

INTERNAL_IPS=(
    $NODE_0_INTERNAL_IP
    $NODE_1_INTERNAL_IP
    $NODE_2_INTERNAL_IP
    $NODE_3_INTERNAL_IP
    $NODE_4_INTERNAL_IP
)

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

echo "Starting node-$1"

# Bind Raft to all interfaces on this node
INTERNAL_IPS[$1]=0.0.0.0

ssh -i ~/.ssh/id_ed25519_gh user@${NODES[$1]} \
"sudo rm -rf /tmp/*.logents /tmp/*.state /tmp/*.log && cd dns-raft/cmd/server && go build . && sudo -b LOG_LEVEL=INFO ./server --node ${INTERNAL_IPS[0]}:9000 --node ${INTERNAL_IPS[1]}:9000 --node ${INTERNAL_IPS[2]}:9000 --node ${INTERNAL_IPS[3]}:9000 --node ${INTERNAL_IPS[4]}:9000 -id ${1} --port 8053 > /tmp/node-${1}.log 2>&1"
