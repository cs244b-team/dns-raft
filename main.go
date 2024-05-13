package main

import (
	"log"
	"net"
	"net/rpc"
)

func main() {
	server := NewServer()
	rpc.Register(server)

	// Create a TCP listener
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Fatal("Accept error: ", err)
			}
			go rpc.ServeConn(conn)
		}
	}()

	// Run the server
	server.run()
}
