package main

type ServerID int

type Server struct {
	// // the id of this server
	// serverID ServerID

	// // the id of the leader
	// leaderID ServerID

	// // Ids of all servers in the cluster
	// cluster []ServerID

	// // TODO: how do we know the addresses?

	// the raft module of this server
	raftNode *RaftNode
}

func NewServer() *Server {
	s := new(Server)
	s.raftNode = NewRaftNode()
	return s
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

// Respond to RPCs from candidates and leaders
// IF election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
func (s *Server) runFollower() {

}

func (s *Server) runCandidate() {

}

func (s *Server) runLeader() {

}

// Passthrough function to the Server's RaftNode RequestVote function
func (s *Server) RequestVote(args RequestVoteArgs, reply *RequestVoteResponse) error {
	println("RequestVote RPC received")
	*reply = s.raftNode.RequestVote(args)
	return nil
}

// Passthrough function to the Server's RaftNode AppendEntries function
func (s *Server) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesResponse) error {
	println("AppendEntries RPC received")
	*reply = s.raftNode.AppendEntries(args)
	return nil
}
