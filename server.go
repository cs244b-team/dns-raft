package main

import (
	// "net/rpc"
)

type ServerID int

type Server struct {
	// the id of this server
	serverID ServerID

	// the id of the leader
	leaderID ServerID

	// Ids of all servers in the cluster
	cluster []ServerID

	// TODO: how do we know the addresses?

	// the raft module of this server
	raftNode *RaftNode
}


func (s *Server) run() {
	for {
		switch s.raftNode.getState() {
		case Follower:
			s.runFollower()
		case Candidate:
			s.runCandidate()
		case Leader:
			s.runLeader()
		}
	}
}


func (s *Server) runFollower() {

}


func (s *Server) runCandidate() {

}

func (s *Server) runLeader() {

}
