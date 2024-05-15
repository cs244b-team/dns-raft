package raft

import (
	"time"

	log "github.com/sirupsen/logrus"
)

type NodeState uint32

const (
	Follower NodeState = iota // default
	Candidate
	Leader
	Shutdown
)

type LogEntry struct {
	Term int
	// data generic?w 4y
}

type Node struct {
	// Persistent state on all servers
	// TODO: make these persistent
	currentTerm int           // Latest term server has seen (init to zero)
	votedFor    Optional[int] // Candidate id that received vote in current term (invalid optional if none)
	log         []LogEntry    // Each entry contains command for state machine, and term when entry was received by leader (first index is 0!)

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed (initialized to 0)
	lastApplied int // Index of highest log entry applied to state machine (initialized to 0)
	state       NodeState

	// Volatile state on leaders
	nextIndex  map[int]int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int // For each server, index of highest log entry known to be replicated on server

	// Server and cluster state
	serverId int           // the id of this server
	leaderId Optional[int] // the id of the leader
	cluster  []Address     // List of all node addresses in the cluster
	peers    []*Peer       // Peer information and RPC clients

	// For followers to know that they are receiving RPCs from other nodes
	lastContact time.Time

	// Configuration for timeouts and heartbeats
	config Config
}

func NewNode(serverId int, cluster []Address, config Config) *Node {
	r := new(Node)

	r.currentTerm = 0
	r.votedFor = None[int]()
	r.log = make([]LogEntry, 0)
	r.commitIndex = 0
	r.lastApplied = 0
	r.state = Follower
	r.nextIndex = make(map[int]int)
	r.matchIndex = make(map[int]int)
	r.serverId = int(serverId)
	r.leaderId = None[int]()
	r.cluster = cluster
	r.peers = make([]*Peer, 0)
	r.lastContact = time.Now()
	r.config = config

	// Create list of peers
	for i, address := range r.cluster {
		if i != serverId {
			r.peers = append(r.peers, NewPeer(i, address))
		}
	}

	// Register RPC handler and serve immediately
	r.startRpcServer()

	return r
}

func (node *Node) ConnectToCluster() {
	for _, peer := range node.peers {
		err := peer.Connect()
		if err != nil {
			log.Fatalf("node-%d failed to connect to node-%d, reason: %s", node.serverId, peer.id, err)
		} else {
			log.Debugf("node-%d connected to node-%d", node.serverId, peer.id)
		}
	}
}

// Helper functions for getting states, in case we want to implement persistence / atomic operations
func (node *Node) getVotedFor() Optional[int] {
	return node.votedFor
}

func (node *Node) setVotedFor(votedFor int) {
	node.votedFor.Set(votedFor)
}

func (node *Node) getCurrentTerm() int {
	return node.currentTerm
}

func (node *Node) setCurrentTerm(currentTerm int) {
	node.currentTerm = currentTerm
}

func (node *Node) getState() NodeState {
	return node.state
}

func (node *Node) setState(state NodeState) {
	node.state = state
}

func (node *Node) getLeaderId() Optional[int] {
	return node.leaderId
}

func (node *Node) setLeaderId(leaderId int) {
	node.leaderId.Set(leaderId)
}

func (node *Node) getCommitIndex() int {
	return node.commitIndex
}

func (node *Node) setCommitIndex(commitIndex int) {
	node.commitIndex = commitIndex
}

func (node *Node) getLastContact() time.Time {
	return node.lastContact
}

func (node *Node) setLastContact(lastContact time.Time) {
	node.lastContact = lastContact
}

// Determine if a candidate's log is at least as up-to-date as the receiver's log
func (node *Node) isCandidateUpToDate(lastLogTerm int, lastLogIndex int) bool {
	return lastLogTerm > node.lastLogTerm() || (lastLogTerm == node.lastLogTerm() && lastLogIndex >= node.lastLogIndex())
}

// Retrieve the index of the last log entry (-1 if no entries)
func (node *Node) lastLogIndex() int {
	return len(node.log) - 1
}

// Retrieve the term of the last log entry (-1 if no entries)
func (node *Node) lastLogTerm() int {
	lastIndex := node.lastLogIndex()
	if lastIndex < 0 {
		return -1
	}
	return node.log[lastIndex].Term
}

// Main event loop for this RaftNode
func (node *Node) Run() {
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

func (node *Node) runFollower() {
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

func (node *Node) runCandidate() {
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
					"node-%d received vote with higher term %d, converting to follower",
					node.serverId,
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
					"node-%d received majority of votes (%d/%d), converting to leader",
					node.serverId,
					votesReceived,
					len(node.cluster),
				)
				node.convertToLeader()
				return
			}
		case <-electionTimer.C:
			log.Debugf(
				"node-%d election timer expired after %d ms",
				node.serverId,
				electionTimer.timeout/time.Millisecond,
			)
			return
		}
	}
}

func (node *Node) startElection() <-chan RequestVoteResponse {
	log.Debugf("node-%d is starting election", node.serverId)

	// Create a vote response channel
	voteChannel := make(chan RequestVoteResponse, len(node.cluster))

	// Increment our current term
	node.setCurrentTerm(node.getCurrentTerm() + 1)

	// Vote for self
	node.votedFor = Some(node.serverId)
	voteChannel <- RequestVoteResponse{node.getCurrentTerm(), true}

	// Request votes from all peers in parallel
	args := RequestVoteArgs{node.currentTerm, node.serverId, node.lastLogIndex(), node.lastLogTerm()}
	for _, peer := range node.peers {
		go func(p *Peer) {
			log.Debugf("node-%d requesting vote from node-%d", node.serverId, p.id)
			reply, err := p.RequestVote(args)
			if err != nil {
				log.Errorf("node-%d experienced RequestVote error: %s", node.serverId, err)
			}
			voteChannel <- reply
		}(peer)
	}

	return voteChannel
}

func (node *Node) runLeader() {
	heartbeatTicker := time.NewTicker(
		time.Duration(node.config.HeartbeatInterval) * time.Millisecond,
	)
	defer heartbeatTicker.Stop()
	for {
		args := AppendEntriesArgs{
			node.getCurrentTerm(),
			node.serverId,
			node.lastLogIndex(),
			node.lastLogTerm(),
			make([]LogEntry, 0),
			node.getCommitIndex(),
		}

		log.Debugf("node-%d sending heartbeats", node.serverId)
		for _, peer := range node.peers {
			go func(p *Peer) {
				_, err := p.AppendEntries(args)
				if err != nil {
					log.Errorf("node-%d experienced AppendEntries error: %s", node.serverId, err)
				}
			}(peer)
		}
		<-heartbeatTicker.C
	}
	// TODO
}

// State conversion functions
func (node *Node) convertToFollower(term int) {
	log.Infof("node-%d converting to follower", node.serverId)
	node.setCurrentTerm(term)
	node.setState(Follower)
}

func (node *Node) convertToCandidate() {
	log.Infof("node-%d converting to candidate", node.serverId)
	node.setState(Candidate)
}

func (node *Node) convertToLeader() {
	log.Infof("node-%d converting to leader", node.serverId)
	node.setState(Leader)
}
