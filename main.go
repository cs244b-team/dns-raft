package main

import (
	log "github.com/sirupsen/logrus"
)

// TODO ./raft --listen-port 9000 --config ...

func main() {
	log.SetLevel(log.DebugLevel)

	cluster := []NodeId{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

	config := DefaultConfig()

	node1 := NewRaftNode("127.0.0.1:9000", cluster, config)
	node2 := NewRaftNode("127.0.0.1:9001", cluster, config)
	node3 := NewRaftNode("127.0.0.1:9002", cluster, config)

	node1.connectToCluster()
	node2.connectToCluster()
	node3.connectToCluster()

	node2.setState(Candidate)

	go node1.run()
	go node2.run()
	node3.run()
}
