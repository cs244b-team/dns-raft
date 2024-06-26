package raft

import (
	"context"
	"cs244b-team/dns-raft/common"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	wal "github.com/tidwall/wal"
)

type NodeStatus uint8

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
	serverId int               // the id of this server
	leaderId int               // the id of the leader, or -1 if unknown
	cluster  []Address         // List of all node addresses in the cluster
	peers    []*Peer           // Peer information and RPC clients
	updates  map[int]chan bool // TODO

	// For followers to know that they are receiving RPCs from other nodes
	lastContact time.Time

	// Configuration for timeouts and heartbeats
	config Config

	// DNS record store
	kv_store map[string][]dns.RR
}

func (node *Node) ResetWAL() {
	logEntryFilename := fmt.Sprintf("/tmp/raft-node-%d.logents", node.serverId)
	if err := os.Remove(logEntryFilename); err != nil {
		log.Fatalf("failed to remove wal storage file: %s", err)
	}
	walLog, err := wal.Open(logEntryFilename, nil)
	if err != nil {
		log.Fatalf("failed to re-open wal storage file when resetting: %s", err)
	}
	node.logEntryWAL = walLog
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

	r.updates = make(map[int]chan bool)
	r.kv_store = make(map[string][]dns.RR)

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

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		r.mu.Lock()
		log.Warnf("Killing node %d", r.serverId)
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()

	// Register RPC handler and serve immediately
	r.startRpcServer()

	return r
}

func (node *Node) getLeaderPeer() *Peer {
	for _, peer := range node.peers {
		if peer.id == node.leaderId {
			return peer
		}
	}
	return nil
}

func (node *Node) ConnectToCluster() {
	var retryPeers []*Peer
	for _, peer := range node.peers {
		err := peer.Connect()
		if err != nil {
			log.Warnf("node-%d failed to connect to node-%d, reason: %s", node.serverId, peer.id, err)
			retryPeers = append(retryPeers, peer)
		} else {
			log.Debugf("node-%d connected to node-%d", node.serverId, peer.id)
		}
	}

	for _, peer := range retryPeers {
		failedCounter := 0
		err := peer.Connect()
		for err != nil && failedCounter < 10 {
			log.Warnf("node-%d failed to connect on RETRY to node-%d, reason: %s", node.serverId, peer.id, err)
			failedCounter++
			time.Sleep(1 * time.Second)
			err = peer.Connect()
		}
		if err != nil {
			log.Debugf("node-%d connected to node-%d", node.serverId, peer.id)
		} else if failedCounter >= 10 {
			log.Warnf("node-%d failed to connect to node-%d after 10 RETRIES, reason: %s", node.serverId, peer.id, err)
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

func (node *Node) GetValue(key string) ([]dns.RR, bool) {
	records, ok := node.kv_store[key]
	if !ok {
		return nil, false
	}
	return records, true
}

func (node *Node) setValue(key string, record dns.RR) {
	_, ok := node.kv_store[key]
	if !ok {
		node.kv_store[key] = make([]dns.RR, 0)
	}

	// Quick and dirty way to not duplicate record in store
	for _, storeRecord := range node.kv_store[key] {
		if dns.IsDuplicate(record, storeRecord) {
			log.Warnf("node-%d ignoring duplicate record %s", node.serverId, record.String())
			return
		}
	}

	node.kv_store[key] = append(node.kv_store[key], record)
}

func (node *Node) removeValue(key string) {
	delete(node.kv_store, key)
}

func (node *Node) UpdateValue(key string, cmdType CommandType, value Optional[dns.RR]) error {
	node.mu.Lock()
	if node.getStatus() == Leader {
		entry := LogEntry{node.getCurrentTerm(), Command{cmdType, key, value}}
		index := len(node.log) + 1
		err := node.persistLogEntry(entry, uint64(index), false)
		if err != nil {
			node.mu.Unlock()
			return err
		}
		node.log = append(node.log, entry)

		updateChannel := make(chan bool, 1)
		node.updates[index] = updateChannel

		ctx := context.WithoutCancel(context.Background())
		node.sendAppendEntries(ctx)

		node.mu.Unlock()

		// Wait for updateTimeout amount of time,
		updateTimer := time.After(time.Duration(node.config.UpdateTimeout) * time.Millisecond)
		select {
		case <-updateTimer:
			return errors.New(fmt.Sprintf("Log entry %d update timeout", index))
		case <-updateChannel:
			return nil
		}
	} else if node.getStatus() == Follower {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		args := ForwardToLeaderArgs{Key: key, CmdType: cmdType, Value: value}
		node.mu.Unlock()
		_, err := callRPCOnLeader[ForwardToLeaderResponse](node, "Node.ForwardToLeader", args, node.config.ForwardToLeaderRetryInterval, ctx)
		return err
	} else {
		node.mu.Unlock()
		return errors.New(fmt.Sprintf("Update request for key %s, value %v was sent to non-follower, non-leader node (with status %v)", key, value, node.getStatus()))
	}
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
	nextIndex, ok := node.nextIndex[peerId]
	if !ok {
		log.Fatalf("node-%d attempted to retrieve prevLogIndexAndTerm with a peerId %d that wasn't set", node.serverId, peerId)
	}
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

	for _, peer := range node.peers {
		node.nextIndex[peer.id] = len(node.log)
		log.Debugf("node-%d setting peer-%d nextIndex to %d", node.serverId, peer.id, len(node.log))
	}
	node.mu.Unlock()

	// Blank entry on leader startup
	node.UpdateValue("", Blank, None[dns.RR]())

	ctx, cancel := context.WithCancel(context.Background())
	defer heartbeatTicker.Stop()
	defer cancel()
	for {
		node.mu.Lock()
		if node.getStatus() != Leader {
			node.mu.Unlock()
			return
		}

		node.sendAppendEntries(ctx)
		node.mu.Unlock()
		<-heartbeatTicker.C
	}
	// TODO No-op commit (client interaction, section 8 of paper)
}

func (node *Node) persistLogEntry(entry LogEntry, logIndex uint64, truncateBack bool) error {
	if logIndex <= 0 {
		log.Fatalf("node-%d tried to write to an invalid index (%d) in our 1-indexed persistent log", node.serverId, logIndex)
	}

	bytes, err := common.EncodeToBytes(entry)
	if err != nil {
		return err
	}

	lastSavedIndex, err := node.logEntryWAL.LastIndex()
	if err != nil {
		return fmt.Errorf("node-%d failed to get wal last index: %s", node.serverId, err)
	}

	// If log is non-empty, we may need to truncate
	if truncateBack {
		// We only need to truncate the log if logIndex is within the current log
		if logIndex <= lastSavedIndex {
			if logIndex > 1 {
				if err = node.logEntryWAL.TruncateBack(logIndex - 1); err != nil {
					return fmt.Errorf("node-%d logEntryWAL.TruncateBack failed with logIndex: %d", node.serverId, logIndex)
				}
			} else {
				// We want to insert into logIndex 1, so we must clear the entire log
				node.ResetWAL()
			}
		}
	} else if logIndex <= lastSavedIndex {
		log.Fatalf("node-%d logEntryWAL received a request to write w/o truncateBack to logIndex %d when the log has been persisted until %d", node.serverId, logIndex, lastSavedIndex)
	}

	// By now, we have truncated the log, as needed. We should append the log entry now.
	if logIndex > lastSavedIndex+1 {
		// We want to write past the lastSavedIndex, which leaves a hole in our log
		return fmt.Errorf("node-%d attempted to persist log with a hole at index %d when log goes to index %d", node.serverId, logIndex, lastSavedIndex)
	}

	if err = node.logEntryWAL.Write(logIndex, bytes); err != nil {
		return fmt.Errorf("node-%d logEntryWAL.Write failed with logIndex: %d with err %v", node.serverId, logIndex, err)
	}

	return nil
}

func (node *Node) persistLogBatch(entries []LogEntry, startingLogIndex uint64, truncateBack bool) error {
	if startingLogIndex <= 0 {
		log.Fatalf("node-%d tried to write to an invalid index (%d) in our 1-indexed persistent log", node.serverId, startingLogIndex)
	}

	batch := new(wal.Batch)
	for i, entry := range entries {
		bytes, err := common.EncodeToBytes(entry)
		if err != nil {
			return err
		}
		batch.Write(startingLogIndex+uint64(i), bytes)
	}

	lastSavedIndex, err := node.logEntryWAL.LastIndex()
	if err != nil {
		return fmt.Errorf("node-%d failed to get wal last index: %s", node.serverId, err)
	}

	// If log is non-empty, we may need to truncate
	if truncateBack {
		// We only need to truncate the log if logIndex is within the current log
		if startingLogIndex <= lastSavedIndex {
			if startingLogIndex > 1 {
				if err = node.logEntryWAL.TruncateBack(startingLogIndex - 1); err != nil {
					return fmt.Errorf("node-%d batched logEntryWAL.TruncateBack failed with logIndex: %d", node.serverId, startingLogIndex)
				}
			} else {
				// We want to insert into logIndex 1, so we must clear the entire log
				node.ResetWAL()
			}
		}
	} else if startingLogIndex <= lastSavedIndex {
		log.Fatalf("node-%d logEntryWAL received a request to batch write w/o truncateBack to logIndex %d when the log has been persisted until %d", node.serverId, startingLogIndex, lastSavedIndex)
	}

	// By now, we have truncated the log, as needed. We should append the log entry now.
	if startingLogIndex > lastSavedIndex+1 {
		// We want to write past the lastSavedIndex, which leaves a hole in our log
		return fmt.Errorf("node-%d attempted to persist log with a hole at index %d when log goes to index %d", node.serverId, startingLogIndex, lastSavedIndex)
	}

	if err = node.logEntryWAL.WriteBatch(batch); err != nil {
		return fmt.Errorf("node-%d logEntryWAL.Write failed with logIndex: %d with err %v", node.serverId, startingLogIndex, err)
	}

	return nil
}

// send AppendEntries RPCs to all peers when heartbeats timeout or receive client requests
// must be called with the lock held
func (node *Node) sendAppendEntries(ctx context.Context) {
	if node.getStatus() != Leader {
		return
	}

	log.Debugf("node-%d sending heartbeats", node.serverId)
	peer_args := make([]AppendEntriesArgs, len(node.peers))
	for i, peer := range node.peers {
		idx, term := node.prevLogIndexAndTerm(peer.id)
		nextIdx := idx + 1
		if nextIdx == 0 {
			log.Warnf("prevIDX will be -1 with log length of %d", node.serverId)
		}
		entries := make([]LogEntry, 0)
		if nextIdx < len(node.log) {
			// Credit to https://arorashu.github.io/posts/raft.html for giving an idea on how to separate heartbeats from updating followers
			entries = node.log[nextIdx:min(nextIdx+node.config.MaximumBatchSize, len(node.log))]
		}
		args := AppendEntriesArgs{
			node.getCurrentTerm(),
			node.serverId,
			idx,
			term,
			entries,
			node.getCommitIndex(),
		}
		peer_args[i] = args
	}
	for i, peer := range node.peers {
		go func(p *Peer, i int) {
			args := peer_args[i]
			nextIdx := args.PrevLogIndex + 1
			reply, err := callRPCNoRetry[AppendEntriesResponse](p, "Node.AppendEntries", args, ctx)
			if err != nil {
				log.Errorf("node-%d experienced AppendEntries error: %s", p.id, err)
				return
			}
			unpackedReply := reply.Value()
			node.mu.Lock()
			defer node.mu.Unlock()
			if unpackedReply.CurrentTerm > node.getCurrentTerm() {
				if unpackedReply.Success {
					log.Fatal("Expected a node of higher term to reject the appendentry")
				}
				log.Debugf(
					"node-%d received AppendEntries response with higher term %d, converting to follower",
					node.serverId,
					unpackedReply.CurrentTerm,
				)
				node.convertToFollower(unpackedReply.CurrentTerm)
				return
			}

			if !unpackedReply.Success {
				node.nextIndex[p.id] = nextIdx - 1
			} else if nextIdx < len(node.log) {
				// We can consider the nextIndex to have been accepted by the peer
				node.matchIndex[p.id] = args.PrevLogIndex + len(args.Entries)
				// The leader always has a log match for nextIdx...args.PrevLogIndex + len(args.Entries)
				for currIdx := args.PrevLogIndex + len(args.Entries); currIdx >= nextIdx; currIdx-- {
					if (node.countLogMatches(currIdx)+1) >= (len(node.cluster)+1)/2 && node.log[currIdx].Term == node.getCurrentTerm() && currIdx > node.commitIndex {
						node.setCommitIndex(currIdx)
						node.applyLogCommands()
						break
					}
				}
				node.nextIndex[p.id] = args.PrevLogIndex + len(args.Entries) + 1
			}
		}(peer, i)
	}
}

func (node *Node) countLogMatches(logIndex int) int {
	// Make sure the lock has been acquired before calling!
	numMatches := 0
	for _, peer := range node.peers {
		if node.matchIndex[peer.id] >= logIndex {
			numMatches++
		}
	}
	return numMatches
}

func (node *Node) applyCommand(entry LogEntry) {
	if entry.Cmd.Type == Remove {
		node.removeValue(entry.Cmd.Key)
	} else if entry.Cmd.Type == Update {
		if !entry.Cmd.Value.HasValue() {
			log.Warnf("node-%d update for %s expected value to be set, but none was found", node.serverId, entry.Cmd.Key)
		}
		node.setValue(entry.Cmd.Key, entry.Cmd.Value.Value())
	} else if entry.Cmd.Type == Blank {
		log.Debugf("node-%d skipping blank log entry", node.serverId)
	} else {
		log.Fatalf("node-%d attempted to apply an usupported Raft command: %v", node.serverId, entry.Cmd.Type)
	}
}

// Lock is held by sendAppendEntries or AppendEntries RPC, so no lock is needed here
func (node *Node) applyLogCommands() {
	for idx := node.lastApplied + 1; idx <= node.commitIndex; idx++ {
		// log.Infof("node-%d applying command at log index: %v", node.serverId, idx, node.log[idx])
		node.applyCommand(node.log[idx])

		// If this leader node has any clients blocked waiting for their update to be committed, signal them
		if node.getStatus() == Leader {
			updateChannel, ok := node.updates[idx+1]
			if !ok {
				log.Debugf("node-%d (leader) attempted to apply log command, but client update channel is missing at index: %d", node.serverId, idx+1)
			} else {
				updateChannel <- true
				close(updateChannel)
			}
			// TODO: remove update logindex: channel mapping
		}
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
	// Also, reset the leaderId
	node.setLeaderId(-1)
}

func (node *Node) convertToCandidate() {
	log.Infof("node-%d converting to candidate", node.serverId)
	node.setLeaderId(-1) // Reset the leaderId since we are in an election
	node.setStatus(Candidate)
}

func (node *Node) convertToLeader() {
	log.Infof("node-%d converting to leader", node.serverId)
	node.setLeaderId(node.serverId) // Record that we are the leader
	node.setStatus(Leader)
}
