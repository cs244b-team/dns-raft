package main

import (
	"cs244b-team/dns-raft/common"
	"cs244b-team/dns-raft/raft"
	"flag"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"
)

func runLocalCluster() {
	cluster := []raft.Address{
		raft.NewAddress("localhost", 9000),
		raft.NewAddress("localhost", 9001),
		raft.NewAddress("localhost", 9002),
	}

	config := raft.DefaultConfig()

	// Create array of RaftNode objects
	nodes := make([]*raft.Node, len(cluster))
	for i := range cluster {
		nodes[i] = raft.NewNode(i, cluster, config)
	}

	// Connect to all nodes in the cluster
	for _, node := range nodes {
		node.ConnectToCluster()
	}

	// Start the RaftNodes
	wg := sync.WaitGroup{}
	for _, node := range nodes {
		wg.Add(1)
		go func(node *raft.Node) {
			node.Run()
			wg.Done()
		}(node)
	}

	// Wait for all nodes to finish
	wg.Wait()
}

type nodes []raft.Address

func (n *nodes) String() string {
	return fmt.Sprintf("%v", *n)
}

func (n *nodes) Set(address string) error {
	id, err := raft.AddressFromString(address)
	if err != nil {
		return err
	}
	*n = append(*n, id)
	return nil
}

func main() {
	common.InitLogger()

	cluster := nodes{}
	flag.Var(&cluster, "node", "ip:port of other nodes in the cluster")
	id := flag.Int("id", 0, "id of this node")
	flag.Parse()

	if len(cluster) == 0 {
		log.Warn("Cluster not specified, running local cluster")
		runLocalCluster()
	}

	// Create and run a single RaftNode
	config := raft.DefaultConfig()
	node := raft.NewNode(*id, cluster, config)

	// TODO: maybe set a timer to start the node after a delay, so that we can connect to all nodes successfully
	// and handle membership changes later?
	node.ConnectToCluster()
	node.Run()
}
