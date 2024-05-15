package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type NodeStatus uint32

const (
	Follower NodeStatus = iota // default
	Candidate
	Leader
	Shutdown
)

type Node struct {
	// Persistent state on all servers
	storage *StableStorage
	log     []*LogEntry
	// log     *StableLog

	lastLogLock sync.Mutex

	// Volatile state on all servers
	commitIndex atomic.Int32 // Index of highest log entry known to be committed (initialized to 0)
	lastApplied atomic.Int32 // Index of highest log entry applied to state machine (initialized to 0)
	status      NodeStatus

	// Volatile state on leaders
	nextIndex  map[int]int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int // For each server, index of highest log entry known to be replicated on server

	// Server and cluster state
	serverId int          // the id of this server
	leaderId atomic.Int32 // the id of the leader, or -1 if unknown
	cluster  []Address    // List of all node addresses in the cluster
	peers    []*Peer      // Peer information and RPC clients

	// For followers to know that they are receiving RPCs from other nodes
	lastContact     time.Time
	lastContactLock sync.Mutex

	// Configuration for timeouts and heartbeats
	config Config
}

func NewNode(serverId int, cluster []Address, config Config) *Node {
	r := new(Node)

	stateFile := fmt.Sprintf("/tmp/raft-node-%d.state", serverId)
	r.storage = NewStableStorage(stateFile)
	r.storage.Reset()

	r.log = make([]*LogEntry, 0)

	r.commitIndex.Store(0)
	r.lastApplied.Store(0)
	r.status = Follower

	r.nextIndex = make(map[int]int)
	r.matchIndex = make(map[int]int)

	r.serverId = int(serverId)
	r.leaderId.Store(-1)
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
func (node *Node) getCurrentTerm() int {
	currentTerm, err := node.storage.GetCurrentTerm()
	if err != nil {
		log.Errorf("node-%d failed to get currentTerm: %s", node.serverId, err)
	}
	return currentTerm
}

func (node *Node) setCurrentTerm(currentTerm int) {
	err := node.storage.SetCurrentTerm(currentTerm)
	if err != nil {
		log.Errorf("node-%d failed to set currentTerm: %s", node.serverId, err)
	}
}

func (node *Node) getVotedFor() Optional[int] {
	votedFor, err := node.storage.GetVotedFor()
	if err != nil {
		log.Errorf("node-%d failed to get votedFor: %s", node.serverId, err)
	}
	return votedFor
}

func (node *Node) setVotedFor(votedFor int) {
	err := node.storage.SetVotedFor(votedFor)
	if err != nil {
		log.Errorf("node-%d failed to set votedFor: %s", node.serverId, err)
	}
}

func (node *Node) getStatus() NodeStatus {
	return NodeStatus(atomic.LoadUint32((*uint32)(&node.status)))
}

func (node *Node) setStatus(status NodeStatus) {
	statusAddr := (*uint32)(&node.status)
	atomic.StoreUint32(statusAddr, uint32(status))
}

func (node *Node) getLeaderId() int {
	return int(node.leaderId.Load())
}

func (node *Node) setLeaderId(leaderId int) {
	node.leaderId.Store(int32(leaderId))
}

func (node *Node) getCommitIndex() int {
	return int(node.commitIndex.Load())
}

func (node *Node) setCommitIndex(commitIndex int) {
	node.commitIndex.Store(int32(commitIndex))
}

func (node *Node) getLastApplied() int {
	return int(node.lastApplied.Load())
}

func (node *Node) setLastApplied(lastApplied int) {
	node.lastApplied.Store(int32(lastApplied))
}

func (node *Node) getLastContact() time.Time {
	node.lastContactLock.Lock()
	defer node.lastContactLock.Unlock()
	return node.lastContact
}

func (node *Node) setLastContact(lastContact time.Time) {
	node.lastContactLock.Lock()
	defer node.lastContactLock.Unlock()
	node.lastContact = lastContact
}

// Determine if a candidate's log is at least as up-to-date as the receiver's log
func (node *Node) isCandidateUpToDate(lastLogTerm int, lastLogIndex int) bool {
	idx, term := node.lastLogIndexAndTerm()
	return lastLogTerm > term || (lastLogTerm == term && lastLogIndex >= idx)
}

// Retrieve the index and the term of the last log entry (-1 if no entries)
func (node *Node) lastLogIndexAndTerm() (int, int) {
	node.lastLogLock.Lock()
	defer node.lastLogLock.Unlock()

	lastIndex := len(node.log) - 1
	if lastIndex < 0 {
		return -1, -1
	}

	return lastIndex, node.log[lastIndex].Term
}

// Main event loop for this RaftNode
func (node *Node) Run() {
	for {
		switch node.getStatus() {
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
	electionTimer := NewRandomTimer(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	for node.getStatus() == Follower {
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
	for node.getStatus() == Candidate {
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
	node.setVotedFor(node.serverId)
	voteChannel <- RequestVoteResponse{node.getCurrentTerm(), true}

	// Request votes from all peers in parallel
	idx, term := node.lastLogIndexAndTerm()
	args := RequestVoteArgs{node.getCurrentTerm(), node.serverId, idx, term}
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
		idx, term := node.lastLogIndexAndTerm()
		args := AppendEntriesArgs{
			node.getCurrentTerm(),
			node.serverId,
			idx,
			term,
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
	node.setStatus(Follower)
}

func (node *Node) convertToCandidate() {
	log.Infof("node-%d converting to candidate", node.serverId)
	node.setStatus(Candidate)
}

func (node *Node) convertToLeader() {
	log.Infof("node-%d converting to leader", node.serverId)
	node.setStatus(Leader)
}
