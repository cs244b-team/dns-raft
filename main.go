package main

import (
	"cs244b-team/dns-raft/raft"
	"flag"
	"fmt"
	"os"
	"sync"

	log "github.com/sirupsen/logrus"
)

type nodes []raft.NodeId

func (n *nodes) String() string {
	return fmt.Sprintf("%v", *n)
}

func (n *nodes) Set(value string) error {
	id, err := raft.NodeIdFromString(value)
	if err != nil {
		return err
	}
	*n = append(*n, id)
	return nil
}

func init_logging() {
	// Get environment variable for log level (LOG_LEVEL)
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}

	// Set log level
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("Invalid log level: %s", logLevel)
	}

	log.SetLevel(level)
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
}

func runLocalCluster() {
	cluster := nodes{
		raft.NodeId{Ip: "localhost", Port: 9000},
		raft.NodeId{Ip: "localhost", Port: 9001},
		raft.NodeId{Ip: "localhost", Port: 9002},
	}

	config := raft.DefaultConfig()

	// Create array of RaftNode objects
	nodes := make([]*raft.RaftNode, len(cluster))
	for i, node := range cluster {
		nodes[i] = raft.NewRaftNode(node, cluster, config)
	}

	// Connect to all nodes in the cluster
	for _, node := range nodes {
		node.ConnectToCluster()
	}

	// Start the RaftNodes
	wg := sync.WaitGroup{}
	for _, node := range nodes {
		wg.Add(1)
		go func(node *raft.RaftNode) {
			node.Run()
			wg.Done()
		}(node)
	}

	// Wait for all nodes to finish
	wg.Wait()
}

func main() {
	init_logging()

	// Parse command line arguments
	cluster := nodes{}
	flag.Var(&cluster, "node", "ip:port of other nodes in the cluster")
	flag.Parse()

	if len(cluster) == 0 {
		log.Warn("Cluster not specified, running local cluster")
		runLocalCluster()
		return
	}

	if flag.NArg() != 1 {
		log.Fatal("If cluster is specified, exactly one positional argument is required: ip:port of this node")
	}

	nodeId, err := raft.NodeIdFromString(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}

	// Create and run a single RaftNode
	node := raft.NewRaftNode(nodeId, cluster, raft.DefaultConfig())

	// TODO: maybe set a timer to start the node after a delay, so that we can connect to all nodes successfully
	// and handle membership changes later?
	node.ConnectToCluster()
	node.Run()
}
