package main

import (
	"fmt"
	"log"
	"net/rpc"
)

// TODO ./raft --listen-port 9000 --config ...

func main() {
	node := NewRaftNode("127.0.0.1:9000")

	// // go func() {
	// // 	for {
	// // 		conn, err := listener.Accept()
	// // 		if err != nil {
	// // 			log.Fatal("Accept error: ", err)
	// // 		}
	// // 		println("received connection")
	// // 		go rpc.ServeConn(conn)
	// // 	}
	// // }()

	go func() {
		println("other node started")
		otherNode, err := rpc.DialHTTP("tcp", "127.0.0.1:9000")
		println("created connection to server")
		if err != nil {
			log.Fatal("dialing:", err)
		}

		args := RequestVoteArgs{0, "127.0.0.1:9000", 0, 1}
		var reply RequestVoteResponse
		err = otherNode.Call("RaftNode.RequestVote", args, &reply)
		if err != nil {
			log.Fatal("RequestVote error:", err)
		}
		fmt.Printf("RequestVote: %d, %s = %+v", args.LastLogIndex, args.CandidateId, reply)
		otherNode.Close()
	}()

	// Run the node
	node.run()
}
