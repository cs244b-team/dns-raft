package raft

import (
	"fmt"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type RaftState uint32

const (
	Follower RaftState = iota // default
	Candidate
	Leader
	Shutdown
)

type Term int

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
		ElectionTimeoutMin: 150,
		ElectionTimeoutMax: 300,
		HeartbeatInterval:  75,
	}
}

type NodeId struct {
	Ip   string
	Port uint16
}

func (n NodeId) String() string {
	return n.Ip + ":" + fmt.Sprintf("%d", n.Port)
}

func NodeIdFromString(s string) (NodeId, error) {
	ip := strings.Split(s, ":")[0]
	port := strings.Split(s, ":")[1]

	port_uint, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return NodeId{}, err
	}

	return NodeId{Ip: ip, Port: uint16(port_uint)}, nil
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
	serverId    NodeId                 // the id of this server
	leaderId    Optional[NodeId]       // the id of the leader
	cluster     []NodeId               // Ids of all servers in the cluster
	peerClients map[NodeId]*rpc.Client // Save RPC clients for all peers

	// For followers to know that they are receiving RPCs from other nodes
	lastContact time.Time

	// Configuration for timeouts and heartbeats
	config Config
}

func NewRaftNode(serverId NodeId, cluster []NodeId, config Config) *RaftNode {
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
	r.peerClients = make(map[NodeId]*rpc.Client)
	r.lastContact = time.Now()
	r.config = config

	// Register RPC handler and serve immediately
	r.startRpcServer()

	return r
}

func (node *RaftNode) ConnectToCluster() {
	for _, nodeId := range node.cluster {
		if nodeId != node.serverId {
			client, err := rpc.DialHTTPPath("tcp", rpcServerAddress(nodeId), rpcServerPath(nodeId))
			if err != nil {
				log.Fatalf(
					"%s failed to connect to %s, reason: %s",
					node.serverId.String(),
					nodeId.String(),
					err,
				)
			} else {
				node.peerClients[nodeId] = client
				log.Debugf("%s connected to %s", node.serverId.String(), nodeId.String())
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
	return lastLogTerm > node.lastLogTerm() || (lastLogTerm == node.lastLogTerm() && lastLogIndex >= node.lastLogIndex())
}

// Retrieve the index of the last log entry (-1 if no entries)
func (node *RaftNode) lastLogIndex() int {
	return len(node.log) - 1
}

// Retrieve the term of the last log entry (-1 if no entries)
func (node *RaftNode) lastLogTerm() Term {
	lastIndex := node.lastLogIndex()
	if lastIndex < 0 {
		return -1
	}
	return node.log[lastIndex].Term
}

// Main event loop for this RaftNode
func (node *RaftNode) Run() {
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
	// electionTimer, electionTime := randomTimeout(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	electionTimer := NewRandomTimer(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	for node.getState() == Follower {
		<-electionTimer.C
		if time.Since(node.getLastContact()) > electionTimer.timeout {
			node.convertToCandidate()
			return
		}
		electionTimer = NewRandomTimer(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	}
}

func (node *RaftNode) runCandidate() {
	// Call RequestVote on peers
	voteChannel := node.startElection()
	electionTimer := NewRandomTimer(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)

	votesReceived := 0
	for node.getState() == Candidate {
		select {
		case vote := <-voteChannel:
			// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
			if vote.CurrentTerm > node.getCurrentTerm() {
				log.Debugf(
					"%s received vote with higher term %d, converting to follower",
					node.serverId.String(),
					vote.CurrentTerm,
				)
				node.convertToFollower(vote.CurrentTerm)
				return
			}

			if vote.VoteGranted {
				votesReceived++
			}

			// TODO: what if the election timer stops while we are tallying the last vote?
			if votesReceived >= (len(node.cluster)+1)/2 {
				log.Infof(
					"%s received majority of votes (%d/%d), converting to leader",
					node.serverId.String(),
					votesReceived,
					len(node.cluster),
				)
				node.convertToLeader()
				return
			}
		case <-electionTimer.C:
			log.Debugf(
				"%s election timer expired after %d ms",
				node.serverId.String(),
				electionTimer.timeout,
			)
			return
		}
	}
}

func (node *RaftNode) startElection() <-chan RequestVoteResponse {
	log.Debugf("%s is starting election", node.serverId.String())

	// Create a vote response channel
	voteChannel := make(chan RequestVoteResponse, len(node.cluster))

	// Increment our current term
	node.setCurrentTerm(node.getCurrentTerm() + 1)

	// Vote for self
	node.votedFor = New(node.serverId)
	voteChannel <- RequestVoteResponse{node.getCurrentTerm(), true}

	// Request votes from all peers in parallel
	args := RequestVoteArgs{node.currentTerm, node.serverId, node.lastLogIndex(), node.lastLogTerm()}
	for id, client := range node.peerClients {
		go func(id NodeId, c *rpc.Client) {
			var reply RequestVoteResponse
			log.Debugf("%s requesting vote from %s", node.serverId.String(), id.String())
			err := c.Call("RaftNode.RequestVote", args, &reply)
			if err != nil {
				log.Errorf("%s experienced RequestVote error: %s", node.serverId.String(), err)
			}
			voteChannel <- reply
		}(id, client)
	}

	return voteChannel
}

func (node *RaftNode) runLeader() {
	heartbeatTicker := time.NewTicker(
		time.Duration(node.config.HeartbeatInterval) * time.Millisecond,
	)
	defer heartbeatTicker.Stop()
	for {
		log.Debugf("%s sending heartbeat", node.serverId.String())
		args := node.makeAppendEntries()
		for _, client := range node.peerClients {
			go func(c *rpc.Client) {
				var reply AppendEntriesResponse
				err := c.Call("RaftNode.AppendEntries", args, &reply)
				if err != nil {
					log.Errorf(
						"%s experienced AppendEntries error: %s",
						node.serverId.String(),
						err,
					)
				}
			}(client)
		}
		<-heartbeatTicker.C
	}
	// TODO
}

func (node *RaftNode) makeAppendEntries() AppendEntriesArgs {
	return AppendEntriesArgs{
		node.getCurrentTerm(),
		node.serverId,
		node.lastLogIndex(),
		node.lastLogTerm(),
		make([]LogEntry, 0),
		node.getCommitIndex(),
	}
}

// State conversion functions
func (node *RaftNode) convertToFollower(term Term) {
	log.Infof("%s converting to follower", node.serverId.String())
	node.setCurrentTerm(term)
	node.setState(Follower)
}

func (node *RaftNode) convertToCandidate() {
	log.Infof("%s converting to candidate", node.serverId.String())
	node.setState(Candidate)
}

func (node *RaftNode) convertToLeader() {
	log.Infof("%s converting to leader", node.serverId.String())
	node.setState(Leader)
}
