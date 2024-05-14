package main

import (
	"net"
	"net/http"
	"net/rpc"
	"time"

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
	Term Term
	// data generic?w 4y
}

type Config struct {
	// Election timeout in milliseconds
	ElectionTimeoutMin int
	ElectionTimeoutMax int

	// How often a Leader should send empty Append Entry heartbeats
	HeartbeatInterval int
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin: 1500,
		ElectionTimeoutMax: 3000,
		HeartbeatInterval: 750,
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

	// For followers to know that they are receiving RPCs from other nodes
	lastContact time.Time

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

	r.lastContact = time.Now()
	r.config = config

	// Register RPC handler and serve immediately
	rpcServer := rpc.NewServer()
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

func (node *RaftNode) getLeaderId() Optional[NodeId] {
	return node.leaderId
}

func (node *RaftNode) setLeaderId(leaderId NodeId) {
	node.leaderId.Set(leaderId)
}

func (node *RaftNode) getCommitIndex() int {
	return node.commitIndex
}

func (node *RaftNode) setCommitIndex(commitIndex int) {
	node.commitIndex = commitIndex
}


func (node *RaftNode) getLastContact() time.Time {
	return node.lastContact
}

func (node *RaftNode) setLastContact(lastContact time.Time) {
	node.lastContact = lastContact
}

// Determine if a candidate's log is at least as up-to-date as the receiver's log
func (node *RaftNode) isCandidateUpToDate(lastLogTerm Term, lastLogIndex int) bool {
	return lastLogTerm > node.LastLogTerm() || (lastLogTerm == node.LastLogTerm() && lastLogIndex >= node.LastLogIndex())
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
	return node.log[lastIndex].Term
}

func (node *RaftNode) convertToFollower(term Term) {
	node.setCurrentTerm(term)
	node.setState(Follower)
}

func (node *RaftNode) convertToLeader() {
	log.Debugf("%s converting to leader", string(node.serverId))
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

func (node *RaftNode) runFollower() {
	electionTimer, electionTime := randomTimeout(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	for node.getState() == Follower {
		<-electionTimer
		if time.Since(node.getLastContact()) > electionTime {
			node.setState(Candidate)
			log.Debugf("%s converting to candidate", string(node.serverId))
			return
		}
		electionTimer, electionTime = randomTimeout(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	}
}

func (node *RaftNode) runCandidate() {
	log.Debugf("%s became candidate", string(node.serverId))

	// Call RequestVote on peers
	voteChannel := node.startElection()
	electionChannel, _ := randomTimeout(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)

	voteReceived := 0
	for node.getState() == Candidate {
		select {
		case vote := <-voteChannel:
			// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
			if vote.CurrentTerm > node.getCurrentTerm() {
				node.convertToFollower(vote.CurrentTerm)
				return
			}

			if vote.VoteGranted {
				voteReceived++
			}

			// TODO: what if the election timer stops while we are tallying the last vote?
			if voteReceived >= (len(node.cluster)+1)/2 {
				node.convertToLeader()
				return
			}
		case <-electionChannel:
			log.Info("Election timeout, starting new election")
			return
		}
	}
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

func (node *RaftNode) runLeader() {
	heartbeatTicker := time.NewTicker(time.Duration(node.config.HeartbeatInterval) * time.Millisecond)
	defer heartbeatTicker.Stop()
	for {
		args := AppendEntriesArgs{
			node.getCurrentTerm(),
			node.serverId,
			node.LastLogIndex(),
			node.LastLogTerm(),
			make([]LogEntry, 0),
			node.getCommitIndex(),
		}
		log.Debugf("%s sending heartbeats", string(node.serverId))
		for _, peerClient := range node.peers {
			go func(c *rpc.Client) {
				var reply AppendEntriesResponse
				err := c.Call("RaftNode.AppendEntries", args, &reply)
				if err != nil {
					log.Error("AppendEntries error:", err)
				}
			}(peerClient)
		}
		<-heartbeatTicker.C
	}
	// TODO
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

		// TODO: synch
		node.setLastContact(time.Now())
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
	if args.CandidateTerm > node.getCurrentTerm() {
		node.convertToFollower(args.CandidateTerm)
		response.CurrentTerm = node.getCurrentTerm()
	}

	*reply = response
	return nil
}

type AppendEntriesArgs struct {
	LeaderTerm   Term
	LeaderId     NodeId     // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  Term       // Term of prevLogIndex entry
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

	// TODO: Section 5.2, indicate that during this timeout period, we granted a vote
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate

	// TODO (1) ...

	response := AppendEntriesResponse{CurrentTerm: node.getCurrentTerm(), Success: false}

	// 1. Reply false if term < currentTerm (Section 5.1)
	if args.LeaderTerm < node.getCurrentTerm() {
		*reply = response
		return nil
	}

	// Section 5.2, convert to follower if term > currentTerm
	if args.LeaderTerm > node.getCurrentTerm() {
		node.convertToFollower(args.LeaderTerm)
		response.CurrentTerm = node.getCurrentTerm()
	}

	// Save leader id
	node.setLeaderId(args.LeaderId)
	log.Debugf("%s received a heartbeat from %s", string(node.serverId), string(args.LeaderId))

	// TODO: 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (Section 5.3)

	// TODO 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (Section 5.3)

	// TODO 4. Append any new entries not already in the log
	// if len(args.Entries) > 0 {}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > 0 && args.LeaderCommit > node.getCommitIndex() {
		commitIdx := min(args.LeaderCommit, node.LastLogIndex())
		node.setCommitIndex(commitIdx)

		// TODO: apply logs

	}

	// TODO: synch
	node.setLastContact(time.Now())
	response.Success = true
	*reply = response
	return nil
}
