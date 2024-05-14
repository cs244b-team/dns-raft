package main

import (
	"net"
	"net/http"
	"net/rpc"

	log "github.com/sirupsen/logrus"
)

type Term int
type RaftState uint32
type NodeId string // ip:port, e.g. 127.0.0.1:9000

const (
	Follower RaftState = iota // default
	Candidate
	Leader
	Shutdown
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

type LogEntry struct {
	term Term
	// data generic?w 4y
}

type Config struct {
	// election timeout in milliseconds
	ElectionMin int
	ElectionMax int
}

func DefaultConfig() Config {
	return Config{
		ElectionMin: 150,
		ElectionMax: 300,
	}
}

type RaftNode struct {
	// Persistent state on all servers
	// TODO: make these persistent
	currentTerm Term             // Latest term server has seen (init to zero)
	votedFor    Optional[NodeId] // Candidate id that received vote in current term (invalid optional if none)
	log         []LogEntry       // Each entry contains command for state machine, and term when entry was received by leader (first index is 0!)

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed (initialized to 0)
	lastApplied int // Index of highest log entry applied to state machine (initialized to 0)
	state       RaftState

	// Volatile state on leaders
	nextIndex  map[NodeId]int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[NodeId]int // For each server, index of highest log entry known to be replicated on server

	// Server and cluster state
	serverId NodeId                 // the id of this server
	leaderId Optional[NodeId]       // the id of the leader
	cluster  []NodeId               // Ids of all servers in the cluster
	peers    map[NodeId]*rpc.Client // Save RPC clients for all peers

	// Configuration
	config Config
}

func NewRaftNode(serverId string, cluster []NodeId, config Config) *RaftNode {
	r := new(RaftNode)
	r.currentTerm = 0
	r.votedFor = None[NodeId]()
	r.log = make([]LogEntry, 0)
	r.commitIndex = 0
	r.lastApplied = 0
	r.state = Follower
	r.nextIndex = make(map[NodeId]int)
	r.matchIndex = make(map[NodeId]int)
	r.serverId = NodeId(serverId)
	r.leaderId = None[NodeId]()
	r.cluster = cluster
	r.peers = make(map[NodeId]*rpc.Client)
	r.config = config

	rpcServer := rpc.NewServer()

	// Register RPC handler and serve immediately
	rpcServer.Register(r)

	// TODO: handle different grpc paths to run multiple nodes on the same physical server
	rpcServer.HandleHTTP("/"+serverId, "/debug/"+serverId)

	// Create a TCP listener
	listener, err := net.Listen("tcp", serverId)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	go http.Serve(listener, nil)

	// TODO: other node startup events
	// go func() {
	// 	// start election timer
	//  // wait for timer to stop
	//  //
	// }()

	return r
}

func (node *RaftNode) connectToCluster() {
	for _, nodeId := range node.cluster {
		if nodeId != node.serverId {
			peerClient, err := rpc.DialHTTPPath("tcp", string(nodeId), "/"+string(nodeId))
			if err != nil {
				log.Error(err)
			} else {
				node.peers[nodeId] = peerClient
			}
		}
	}
}

// Helper functions for getting states, in case we want to implement persistence / atomic operations
func (node *RaftNode) getVotedFor() Optional[NodeId] {
	return node.votedFor
}

func (node *RaftNode) setVotedFor(votedFor NodeId) {
	node.votedFor.Set(votedFor)
}

func (node *RaftNode) getCurrentTerm() Term {
	return node.currentTerm
}

func (node *RaftNode) setCurrentTerm(currentTerm Term) {
	node.currentTerm = currentTerm
}

func (node *RaftNode) getState() RaftState {
	return node.state
}

func (node *RaftNode) setState(state RaftState) {
	node.state = state
}

// Retrieve the index of the last log entry (-1 if no entries)
func (node *RaftNode) LastLogIndex() int {
	return len(node.log) - 1
}

// Retrieve the term of the last log entry (-1 if no entries)
func (node *RaftNode) LastLogTerm() Term {
	lastIndex := node.LastLogIndex()
	if lastIndex < 0 {
		return -1
	}
	return node.log[lastIndex].term
}

// Determine if a candidate's log is at least as up-to-date as the receiver's log
func (node *RaftNode) isCandidateUpToDate(lastLogTerm Term, lastLogIndex int) bool {
	return lastLogTerm > node.LastLogTerm() || (lastLogTerm == node.LastLogTerm() && lastLogIndex >= node.LastLogIndex())
}

func (node *RaftNode) convertToFollower(term Term) {
	node.setCurrentTerm(term)
	node.setState(Follower)
}

func (node *RaftNode) convertToLeader() {
	node.setState(Leader)
}

// Main event loop for this RaftNode
func (node *RaftNode) run() {
	for {
		switch node.getState() {
		case Follower:
			node.runFollower()
		case Candidate:
			node.runCandidate()
		case Leader:
			node.runLeader()
		}
	}
}

// Respond to RPCs from candidates and leaders
// IF election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate

func (node *RaftNode) runFollower() {
	// https://github.com/hashicorp/raft/blob/main/raft.go#L156C16-L156C27
	// for ...
	// break when election timer expires

	// TODO (3): convert to candidate on election timeout without receiving appendentries or granting a vote
}

func (node *RaftNode) runCandidate() {
	log.Debugf("%s became candidate", string(node.serverId))

	// Call RequestVote on peers
	voteChannel := node.startElection()
	electionTimer := randomTimeout(node.config.ElectionMin, node.config.ElectionMax)

	voteReceived := 0
	for node.getState() == Candidate {
		select {
		// TODO: case election timer timeout, start new election
		// TODO: case receive AppendEntries, convert to follower
		case vote := <-voteChannel:
			log.Debug("Vote: ", vote)
			// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
			if vote.CurrentTerm > node.getCurrentTerm() {
				node.convertToFollower(vote.CurrentTerm)
				return
			}

			if vote.VoteGranted {
				voteReceived++
			}

			if voteReceived >= (len(node.cluster)+1)/2 {
				node.convertToLeader()
				return
			}
		case <-electionTimer:
			log.Info("Election timeout, starting new election")
			return
		}
	}
}

func (node *RaftNode) runLeader() {
	// TODO (2): send empty appendentries RPCs
}

func (node *RaftNode) startElection() <-chan RequestVoteResponse {
	// Create a vote response channel
	voteChannel := make(chan RequestVoteResponse, len(node.cluster))

	// Increment our current term
	node.setCurrentTerm(node.getCurrentTerm() + 1)

	// Vote for self
	node.votedFor = New[NodeId](node.serverId)
	voteChannel <- RequestVoteResponse{node.getCurrentTerm(), true}

	// Request votes from all peers in parallel
	args := RequestVoteArgs{node.currentTerm, node.serverId, node.LastLogIndex(), node.LastLogTerm()}
	for _, peerClient := range node.peers {
		go func(c *rpc.Client) {
			var reply RequestVoteResponse
			err := c.Call("RaftNode.RequestVote", args, &reply)
			if err != nil {
				log.Error("RequestVote error:", err)
			}
			voteChannel <- reply
		}(peerClient)
	}

	return voteChannel
}

// RPC logic below

type RequestVoteArgs struct {
	CandidateTerm Term
	CandidateId   NodeId
	LastLogIndex  int
	LastLogTerm   Term
}

type RequestVoteResponse struct {
	CurrentTerm Term
	VoteGranted bool
}

// Invoked by candidates to gather votes, not called directly by this RaftNode (Section 5.2)
func (node *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteResponse) error {
	log.Debug("RequestVote RPC called")
	response := RequestVoteResponse{CurrentTerm: node.getCurrentTerm(), VoteGranted: false}

	// Reply false if term < currentTerm (Section 5.1)
	if args.CandidateTerm < node.getCurrentTerm() {
		*reply = response
		return nil
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (Section 5.4.1)

	if (!node.getVotedFor().HasValue() || node.getVotedFor().Value() == args.CandidateId) && node.isCandidateUpToDate(args.LastLogTerm, args.LastLogIndex) {
		node.setVotedFor(args.CandidateId)
		response.VoteGranted = true

		// TODO: Section 5.2, indicate that during this timeout period, we granted a vote
		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
	if args.CandidateTerm > node.getCurrentTerm() {
		node.convertToFollower(args.CandidateTerm)
	}

	*reply = response
	return nil
}

type AppendEntriesArgs struct {
	LeaderTerm   Term
	LeaderId     NodeId     // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commit index
}

type AppendEntriesResponse struct {
	CurrentTerm Term
	Success     bool
}

// Invoked by leader to replicate log entries (Section 5.3); also used as a heartbeat
func (node *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesResponse) error {
	log.Debug("AppendEntries RPC called")

	// TODO (1) ...

	*reply = AppendEntriesResponse{CurrentTerm: node.getCurrentTerm(), Success: false}
	return nil
}
