package raft

import (
	"context"
	"sync"
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
	// storage *StableStorage
	currentTerm int
	votedFor    Optional[int]
	log         []LogEntry
	// log     *StableLog

	mu sync.Mutex // Lock to protect this node's states

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed (initialized to 0)
	lastApplied int // Index of highest log entry applied to state machine (initialized to 0)
	status      NodeStatus

	// Volatile state on leaders
	nextIndex  map[int]int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int // For each server, index of highest log entry known to be replicated on server

	// Server and cluster state
	serverId int       // the id of this server
	leaderId int       // the id of the leader, or -1 if unknown
	cluster  []Address // List of all node addresses in the cluster
	peers    []*Peer   // Peer information and RPC clients

	// For followers to know that they are receiving RPCs from other nodes
	lastContact time.Time

	// Configuration for timeouts and heartbeats
	config Config
}

func NewNode(serverId int, cluster []Address, config Config) *Node {
	r := new(Node)

	// stateFile := fmt.Sprintf("/tmp/raft-node-%d.state", serverId)
	// r.storage = NewStableStorage(stateFile)
	// r.storage.Reset()

	r.currentTerm = 0
	r.votedFor = None[int]()

	r.log = make([]LogEntry, 0)

	r.commitIndex = 0
	r.lastApplied = 0
	r.status = Follower

	r.nextIndex = make(map[int]int)
	r.matchIndex = make(map[int]int)

	r.serverId = int(serverId)
	r.leaderId = -1
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

// // Helper functions for getting states, in case we want to implement persistence / atomic operations
// func (node *Node) getCurrentTerm() int {
// 	currentTerm, err := node.storage.GetCurrentTerm()
// 	if err != nil {
// 		log.Errorf("node-%d failed to get currentTerm: %s", node.serverId, err)
// 	}
// 	return currentTerm
// }

// func (node *Node) setCurrentTerm(currentTerm int) {
// 	err := node.storage.SetCurrentTerm(currentTerm)
// 	if err != nil {
// 		log.Errorf("node-%d failed to set currentTerm: %s", node.serverId, err)
// 	}
// }

// func (node *Node) getVotedFor() Optional[int] {
// 	votedFor, err := node.storage.GetVotedFor()
// 	if err != nil {
// 		log.Errorf("node-%d failed to get votedFor: %s", node.serverId, err)
// 	}
// 	return votedFor
// }

// func (node *Node) setVotedFor(votedFor int) {
// 	err := node.storage.SetVotedFor(votedFor)
// 	if err != nil {
// 		log.Errorf("node-%d failed to set votedFor: %s", node.serverId, err)
// 	}
// }

func (node *Node) getCurrentTerm() int {
	return node.currentTerm
}

func (node *Node) setCurrentTerm(currentTerm int) {
	node.currentTerm = currentTerm
}

func (node *Node) getVotedFor() Optional[int] {
	return node.votedFor
}

func (node *Node) setVotedFor(votedFor int) {
	node.votedFor.Set(votedFor)
}

func (node *Node) getStatus() NodeStatus {
	return node.status
}

func (node *Node) setStatus(status NodeStatus) {
	node.status = status
}

func (node *Node) getLeaderId() int {
	return node.leaderId
}

func (node *Node) setLeaderId(leaderId int) {
	node.leaderId = leaderId
}

func (node *Node) getCommitIndex() int {
	return node.commitIndex
}

func (node *Node) setCommitIndex(commitIndex int) {
	node.commitIndex = commitIndex
}

func (node *Node) getLastApplied() int {
	return node.lastApplied
}

func (node *Node) setLastApplied(lastApplied int) {
	node.lastApplied = lastApplied
}

func (node *Node) getLastContact() time.Time {
	return node.lastContact
}

func (node *Node) setLastContact(lastContact time.Time) {
	node.lastContact = lastContact
}

// Determine if a candidate's log is at least as up-to-date as the receiver's log
func (node *Node) isCandidateUpToDate(lastLogTerm int, lastLogIndex int) bool {
	idx, term := node.lastLogIndexAndTerm()
	return lastLogTerm > term || (lastLogTerm == term && lastLogIndex >= idx)
}

// Retrieve the index and the term of the last log entry (-1 if no entries)
func (node *Node) lastLogIndexAndTerm() (int, int) {
	lastIndex := len(node.log) - 1
	if lastIndex < 0 {
		return -1, -1
	}

	return lastIndex, node.log[lastIndex].Term
}

func (node *Node) prevLogIndexAndTerm(peerId int) (int, int) {
	nextIndex := node.nextIndex[peerId]
	if nextIndex == 0 {
		return -1, -1
	}

	return nextIndex - 1, node.log[nextIndex-1].Term
}

// Main event loop for this RaftNode
func (node *Node) Run() {
	for {
		node.mu.Lock()
		status := node.getStatus()
		node.mu.Unlock()

		switch status {
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
	for {
		node.mu.Lock()
		if node.getStatus() != Follower {
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()

		<-electionTimer.C
		if time.Since(node.getLastContact()) > electionTimer.timeout {
			node.mu.Lock()
			if node.getStatus() != Follower {
				node.mu.Unlock()
				return
			}

			node.convertToCandidate()
			node.mu.Unlock()
			return
		}
		electionTimer = NewRandomTimer(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)
	}
}

func (node *Node) runCandidate() {
	// Check if the node is in the candidate state before starting the election
	node.mu.Lock()
	status := node.getStatus()
	if status != Candidate {
		node.mu.Unlock()
		return
	}

	// Call RequestVote on peers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	voteChannel := node.startElection(ctx)
	node.mu.Unlock()

	electionTimer := NewRandomTimer(node.config.ElectionTimeoutMin, node.config.ElectionTimeoutMax)

	votesReceived := 0
	for {
		node.mu.Lock()
		if node.getStatus() != Candidate {
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()

		select {
		case vote := <-voteChannel:
			node.mu.Lock()
			if node.getStatus() != Candidate {
				node.mu.Unlock()
				return
			}

			// If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
			if vote.CurrentTerm > node.getCurrentTerm() {
				log.Debugf(
					"node-%d received vote with higher term %d, converting to follower",
					node.serverId,
					vote.CurrentTerm,
				)
				node.convertToFollower(vote.CurrentTerm)
				node.mu.Unlock()
				return
			}

			if vote.VoteGranted {
				votesReceived++
			}

			if votesReceived >= (len(node.cluster)+1)/2 {
				log.Infof(
					"node-%d received majority of votes (%d/%d), converting to leader",
					node.serverId,
					votesReceived,
					len(node.cluster),
				)
				node.convertToLeader()
				node.mu.Unlock()
				return
			}
			node.mu.Unlock()

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

// Start an election and return a channel to receive vote responses
// must be called with the lock held
func (node *Node) startElection(ctx context.Context) <-chan RequestVoteResponse {
	log.Debugf("node-%d is starting election", node.serverId)

	// Create a vote response channel
	voteChannel := make(chan RequestVoteResponse, len(node.cluster))

	// Increment our current term
	node.setCurrentTerm(node.getCurrentTerm() + 1)

	// Vote for self
	node.setVotedFor(node.serverId)
	// TODO: deduplicate votes
	voteChannel <- RequestVoteResponse{node.getCurrentTerm(), true}

	// Request votes from all peers in parallel
	idx, term := node.lastLogIndexAndTerm()
	args := RequestVoteArgs{node.getCurrentTerm(), node.serverId, idx, term}

	callPeers(node, "Node.RequestVote", args, node.config.RPCRetryInterval, ctx, voteChannel)

	return voteChannel
}

func (node *Node) runLeader() {
	heartbeatTicker := time.NewTicker(
		time.Duration(node.config.HeartbeatInterval) * time.Millisecond,
	)
	// Initialize nextIndex
	node.mu.Lock()
	for peerId := range node.nextIndex {
		node.nextIndex[peerId] = len(node.log)
	}
	node.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer heartbeatTicker.Stop()
	defer cancel()
	var logIndicesToVotes map[int]int
	for {
		node.mu.Lock()
		node.sendAppendEntries(ctx, logIndicesToVotes)
		node.mu.Unlock()
		<-heartbeatTicker.C
	}
	// TODO
}

// send AppendEntries RPCs to all peers when heartbeats timeout or receive client requests
// must be called with the lock held
func (node *Node) sendAppendEntries(ctx context.Context, logIndicesToVotes map[int]int) {
	if node.getStatus() != Leader {
		return
	}

	log.Debugf("node-%d sending heartbeats", node.serverId)
	for _, peer := range node.peers {
		go func(p *Peer) {
			node.mu.Lock()
			idx, term := node.prevLogIndexAndTerm(p.id)
			entries := make([]LogEntry, 0)
			if idx > -1 && idx < len(node.log)-1 {
				// Credit to https://arorashu.github.io/posts/raft.html for giving an idea on how to separate heartbeats from updating followers
				entries = node.log[idx+1 : idx+2]
			}
			args := AppendEntriesArgs{
				node.getCurrentTerm(),
				node.serverId,
				idx,
				term,
				entries,
				node.getCommitIndex(),
			}
			node.mu.Unlock()
			reply, err := callRPCNoRetry[AppendEntriesResponse](p, "Node.AppendEntries", args, ctx)
			if err != nil {
				log.Errorf("node-%d experienced AppendEntries error: %s", node.serverId, err)
				return
			}
			unpackedReply := reply.Value()
			node.mu.Lock()
			defer node.mu.Unlock()
			if unpackedReply.CurrentTerm > node.getCurrentTerm() {
				log.Debugf(
					"node-%d received AppendEntries response with higher term %d, converting to follower",
					node.serverId,
					unpackedReply.CurrentTerm,
				)
				node.convertToFollower(unpackedReply.CurrentTerm)
				return
			}
			// TODO: Was trying to implement the part where you retry append entries if logs are out of order. This
			// does not seem to be the right spot to do so.
			if !unpackedReply.Success {
				node.nextIndex[p.id] -= 1
			} else if idx < len(node.log)-1 {
				// We can consider the nextIndex to have been accepted by the peer
				logIndicesToVotes[idx+1]++
				if logIndicesToVotes[idx+1] >= (len(node.cluster)+1)/2 && node.log[idx+1].Term == node.getCurrentTerm() {
					// TODO: commit idx+1!
				}
				node.nextIndex[p.id] += 1
				// TODO: de-duplicate votes
			}
			// TODO: handle AppendEntries response
		}(peer)
	}
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
