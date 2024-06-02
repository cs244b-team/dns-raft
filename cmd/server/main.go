package main

import (
	"cs244b-team/dns-raft/common"
	"cs244b-team/dns-raft/dns"
	"cs244b-team/dns-raft/raft"
	"encoding/gob"
	"flag"
	"fmt"
	"sync"

	dnslib "github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

func runLocalCluster(port int) {
	cluster := []raft.Address{
		raft.NewAddress("localhost", 9000),
		raft.NewAddress("localhost", 9001),
		raft.NewAddress("localhost", 9002),
	}

	config := raft.DefaultConfig()

	var server *dns.DDNSServer
	// Create array of RaftNode objects
	nodes := make([]*raft.Node, len(cluster))
	for i := range cluster {
		if i == 0 {
			server = dns.NewDDNSServer(port, i, cluster, config)
		} else {
			nodes[i] = raft.NewNode(i, cluster, config)
		}
	}

	// Connect to all nodes in the cluster
	for i, node := range nodes {
		if i == 0 {
			go server.Run()
		} else {
			node.ConnectToCluster()
		}
	}

	// Start the RaftNodes
	wg := sync.WaitGroup{}
	for i, node := range nodes {
		if i > 0 {
			wg.Add(1)
			go func(node *raft.Node) {
				node.Run()
				wg.Done()
			}(node)
		}
	}

	// Wait for all nodes to finish
	wg.Wait()
}

type cluster []raft.Address

func (n *cluster) String() string {
	return fmt.Sprintf("%v", *n)
}

func (n *cluster) Set(address string) error {
	id, err := raft.AddressFromString(address)
	if err != nil {
		return err
	}
	*n = append(*n, id)
	return nil
}

func main() {
	common.InitLogger()
	gob.Register(&dnslib.A{})
	gob.Register(&dnslib.AAAA{})
	gob.Register(&dnslib.NS{})
	gob.Register(&dnslib.CNAME{})
	gob.Register(&dnslib.TXT{})

	raftCluster := cluster{}
	flag.Var(&raftCluster, "node", "ip:port of other nodes in the cluster")
	raftNodeId := flag.Int("id", 0, "id of this node")
	dnsListenPort := flag.Int("port", 8053, "DNS server port")
	flag.Parse()

	if len(raftCluster) == 0 {
		log.Warn("Cluster not specified, running local cluster")
		runLocalCluster(*dnsListenPort)
	}

	raftConfig := raft.DefaultConfig()
	server := dns.NewDDNSServer(*dnsListenPort, *raftNodeId, raftCluster, raftConfig)

	server.Run()
}
