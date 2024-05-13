package main

// TODO ./raft --listen-port 9000 --config ...

func main() {
	cluster := []NodeId{"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}

	node1 := NewRaftNode("127.0.0.1:9000", cluster)
	node2 := NewRaftNode("127.0.0.1:9001", cluster)
	node3 := NewRaftNode("127.0.0.1:9002", cluster)

	node2.setState(Candidate)

	node1.run()
	node2.run()
	node3.run()
}
