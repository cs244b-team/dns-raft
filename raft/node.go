package raft

import (
	"context"
	"cs244b-team/dns-raft/common"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	wal "github.com/tidwall/wal"
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

	log         []LogEntry
	logEntryWAL *wal.Log
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

	kv_store map[string]any
}

func NewNode(serverId int, cluster []Address, config Config) *Node {
	r := new(Node)

	filename := fmt.Sprintf("/tmp/raft-node-%d.state", serverId)
	r.storage = NewStableStorage(filename)

	logEntryFilename := fmt.Sprintf("/tmp/raft-node-%d.logents", serverId)
	walLog, err := wal.Open(logEntryFilename, nil)
	if err != nil {
		log.Fatalf("failed to open wal storage file: %s", err)
	}
	r.logEntryWAL = walLog

	r.log = make([]LogEntry, 0)

	// Load saved log entries
	firstSavedIndex, err := r.logEntryWAL.FirstIndex()
	if err != nil {
		log.Fatalf("failed to get wal first index: %s", err)
	}
	lastSavedIndex, err := r.logEntryWAL.LastIndex()
	if err != nil {
		log.Fatalf("failed to get wal last index: %s", err)
	}
	if lastSavedIndex > 0 {
		for i := firstSavedIndex; i <= lastSavedIndex; i++ {
			data, err := r.logEntryWAL.Read(i)
			if err != nil {
				log.Fatalf("failed to load wal entry: %s", err)
			}
			entry, err := common.DecodeFromBytes[LogEntry](data)
			if err != nil {
				log.Fatalf("failed to decode wal entry to bytes: %s", err)
			}
			r.log = append(r.log, entry)
		}
	}

	r.commitIndex = -1
	r.lastApplied = -1
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

// Helper functions for getting states, in case we want to implement persistence / atomic operations
func (node *Node) getCurrentTerm() int {
	currentTerm := node.storage.GetCurrentTerm()
	return currentTerm
}

func (node *Node) setCurrentTerm(currentTerm int) {
	err := node.storage.SetCurrentTerm(currentTerm)
	if err != nil {
		log.Errorf("node-%d failed to set currentTerm: %s", node.serverId, err)
	}
}

func (node *Node) getVotedFor() Optional[int] {
	votedFor := node.storage.GetVotedFor()
	return votedFor
}

func (node *Node) setVotedFor(votedFor int) {
	err := node.storage.SetVotedFor(votedFor)
	if err != nil {
		log.Errorf("node-%d failed to set votedFor: %s", node.serverId, err)
	}
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

		node.mu.Lock()
		if time.Since(node.getLastContact()) > electionTimer.timeout {
			log.Infof("node-%d election timer expired with no contact from leader or candidate", node.serverId)
			if node.getStatus() == Follower {
				node.convertToCandidate()
			}
			node.mu.Unlock()
			return
		}
		node.mu.Unlock()
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

	votesReceived := map[int]bool{}
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
				votesReceived[vote.ServerId] = true
			}

			if len(votesReceived) >= (len(node.cluster)+1)/2 {
				log.Infof(
					"node-%d received majority of votes (%d/%d), converting to leader",
					node.serverId,
					len(votesReceived),
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
	voteChannel <- RequestVoteResponse{node.getCurrentTerm(), true, node.serverId}

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
	logIndicesToVotes := make(IndexVoteCounter)
	for {
		node.mu.Lock()
		if node.getStatus() != Leader {
			node.mu.Unlock()
			return
		}

		node.sendAppendEntries(ctx, logIndicesToVotes)
		node.mu.Unlock()
		<-heartbeatTicker.C
	}
	// TODO
}

func (node *Node) persistentEntryToLog(entry LogEntry, logIndex uint64, truncateBack bool) error {
	bytes, err := common.EncodeToBytes(entry)
	if err != nil {
		return err
	}
	if truncateBack {
		if err = node.logEntryWAL.TruncateBack(logIndex); err != nil {
			return err
		}
	}
	if err = node.logEntryWAL.Write(logIndex, bytes); err != nil {
		return err
	}
	return nil
}

// send AppendEntries RPCs to all peers when heartbeats timeout or receive client requests
// must be called with the lock held
func (node *Node) sendAppendEntries(ctx context.Context, logIndicesToVotes IndexVoteCounter) {
	if node.getStatus() != Leader {
		return
	}

	log.Debugf("node-%d sending heartbeats", node.serverId)
	for _, peer := range node.peers {
		go func(p *Peer) {
			node.mu.Lock()
			idx, term := node.prevLogIndexAndTerm(p.id)
			nextIdx := idx + 1
			entries := make([]LogEntry, 0)
			if nextIdx < len(node.log) {
				// Credit to https://arorashu.github.io/posts/raft.html for giving an idea on how to separate heartbeats from updating followers
				entries = node.log[nextIdx : nextIdx+1]
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
			} else if nextIdx < len(node.log) {
				// We can consider the nextIndex to have been accepted by the peer
				logIndicesToVotes.AddVote(nextIdx, unpackedReply.ServerId)
				if logIndicesToVotes.CountVotes(nextIdx) >= (len(node.cluster)+1)/2 && node.log[nextIdx].Term == node.getCurrentTerm() {
					commitIdx := min(node.commitIndex, nextIdx)
					node.setCommitIndex(commitIdx)
					node.applyLogCommands()
				}
				node.nextIndex[p.id] += 1
			}
		}(peer)
	}
}

func applyCommand(entry LogEntry) {
	// TODO: apply command string to update kv_store
}

func (node *Node) applyLogCommands() {
	for idx := node.lastApplied + 1; idx <= node.commitIndex; idx++ {
		applyCommand(node.log[idx])
	}
	node.lastApplied = node.commitIndex
}

// State conversion functions
func (node *Node) convertToFollower(term int) {
	log.Infof("node-%d converting to follower", node.serverId)
	node.setCurrentTerm(term)
	node.setStatus(Follower)

	// Since we always convert to follower when we receive a higher term, our votedFor should be reset
	node.setVotedFor(-1)
}

func (node *Node) convertToCandidate() {
	log.Infof("node-%d converting to candidate", node.serverId)
	node.setStatus(Candidate)
}

func (node *Node) convertToLeader() {
	log.Infof("node-%d converting to leader", node.serverId)
	node.setStatus(Leader)
}
