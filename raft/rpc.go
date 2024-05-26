package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

func rpcServerPath(id int) string {
	return fmt.Sprintf("/raft/%d", id)
}

func rpcServerDebugPath(id int) string {
	return fmt.Sprintf("/debug/raft/%d", id)
}

func (node *Node) startRpcServer() {
	server := rpc.NewServer()
	server.Register(node)
	server.HandleHTTP(rpcServerPath(node.serverId), rpcServerDebugPath(node.serverId))

	address := node.cluster[node.serverId].String()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	go http.Serve(listener, nil)
	log.Infof("node-%d RPC server started on %s", node.serverId, address)
}

type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId   int
	LastLogIndex  int
	LastLogTerm   int
}

type RequestVoteResponse struct {
	CurrentTerm int
	VoteGranted bool
	ServerId    int
}

// Invoked by candidates to gather votes, not called directly by this RaftNode (Section 5.2)
func (node *Node) RequestVote(args RequestVoteArgs, reply *RequestVoteResponse) error {
	log.Debugf("node-%d received vote request from node-%d", node.serverId, args.CandidateId)

	node.mu.Lock()
	defer node.mu.Unlock()

	response := RequestVoteResponse{CurrentTerm: node.getCurrentTerm(), VoteGranted: false, ServerId: node.serverId}

	// Reply false if term < currentTerm (Section 5.1)
	if args.CandidateTerm < node.getCurrentTerm() {
		log.Debugf(
			"node-%d rejecting vote request from node-%d because candidate term %d < current term %d",
			node.serverId,
			args.CandidateId,
			args.CandidateTerm,
			node.getCurrentTerm(),
		)
		*reply = response
		return nil
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
	if args.CandidateTerm > node.getCurrentTerm() {
		log.Debugf(
			"node-%d converting to follower because candidate term %d > current term %d",
			node.serverId,
			args.CandidateTerm,
			node.getCurrentTerm(),
		)
		node.convertToFollower(args.CandidateTerm)
		response.CurrentTerm = node.getCurrentTerm()
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (Section 5.4.1)
	if (!node.getVotedFor().HasValue() || node.getVotedFor().Value() == args.CandidateId) && node.isCandidateUpToDate(args.LastLogTerm, args.LastLogIndex) {
		log.Debugf("node-%d granting vote to node-%d", node.serverId, args.CandidateId)

		node.setVotedFor(args.CandidateId)
		response.VoteGranted = true

		node.setLastContact(time.Now())
	}

	*reply = response
	return nil
}

type AppendEntriesArgs struct {
	LeaderTerm   int
	LeaderId     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // Leader's commit index
}

type AppendEntriesResponse struct {
	CurrentTerm int
	Success     bool
	ServerId    int
}

// Invoked by leader to replicate log entries (Section 5.3); also used as a heartbeat
func (node *Node) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesResponse) error {
	log.Debugf("node-%d received append entries request from node-%d", node.serverId, args.LeaderId)

	node.mu.Lock()
	defer node.mu.Unlock()

	// TODO: Section 5.2, indicate that during this timeout period, we granted a vote
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate

	// TODO (1) ...

	response := AppendEntriesResponse{CurrentTerm: node.getCurrentTerm(), Success: false, ServerId: node.serverId}

	// 1. Reply false if term < currentTerm (Section 5.1)
	if args.LeaderTerm < node.getCurrentTerm() {
		log.Debugf(
			"node-%d rejecting AppendEntries request from node-%d because leader term %d < current term %d",
			node.serverId,
			args.LeaderId,
			args.LeaderTerm,
			node.getCurrentTerm(),
		)
		*reply = response
		return nil
	}

	// Section 5.2, convert to follower if term > currentTerm
	if args.LeaderTerm > node.getCurrentTerm() {
		log.Debugf(
			"node-%d converting to follower because leader term %d > current term %d",
			node.serverId,
			args.LeaderTerm,
			node.getCurrentTerm(),
		)
		node.convertToFollower(args.LeaderTerm)
		response.CurrentTerm = node.getCurrentTerm()
	}

	// Save leader id
	node.setLeaderId(args.LeaderId)

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (Section 5.3)
	if args.PrevLogIndex != -1 && args.PrevLogIndex >= len(node.log) || (args.PrevLogIndex != -1 && node.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		*reply = response
		return nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (Section 5.3)
	startInsertingAtIdx := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		if startInsertingAtIdx+i < len(node.log) && node.log[startInsertingAtIdx+i].Term != entry.Term {
			node.log = node.log[:startInsertingAtIdx+i]
			args.Entries = args.Entries[i:] // the prior entries are already in the log
			break
		}
	}

	// 4. Append any new entries not already in the log
	node.log = append(node.log, args.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > 0 && args.LeaderCommit > node.getCommitIndex() {
		lastLogIndex, _ := node.lastLogIndexAndTerm()
		commitIdx := min(args.LeaderCommit, lastLogIndex)
		node.setCommitIndex(commitIdx)
		node.applyLogs()
	}

	// TODO: synch
	node.setLastContact(time.Now())

	response.Success = true
	*reply = response

	return nil
}

// Call RPC on peer p. It will retry every TIMEOUT ms and return the reply (or an error) until CTX errors. Blocking.
func callRPC[ResponseType any](p *Peer, rpc string, args any, timeout int, ctx context.Context) (Optional[ResponseType], error) {
	// Do not continue calling RPC if p cannot be connected to
	if p == nil && p.Connect() != nil {
		return None[ResponseType](), errors.New("Could not connect")
	}
	var reply ResponseType
	call := p.client.Go(rpc, args, &reply, nil)
	select {
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		log.Warnf("RPC %s to node-%d timed out", rpc, p.id)
		// TODO: how can we handle the fact that ctx can be cancelled in between the case statement and recalling callRPC?
		// If ctx is cancelled between the case statement and the call to callRPC, we'll issue an extra request, but
		// that is okay because they are idempotent.
		return callRPC[ResponseType](p, rpc, args, timeout, ctx)
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			return None[ResponseType](), resp.Error
		}
		return Some[ResponseType](reply), nil
	case <-ctx.Done():
		log.Debugf("RPC %s to node-%d cancelled", rpc, p.id)
		return None[ResponseType](), errors.New("RPC cancelled")
	}
}

func callRPCNoRetry[ResponseType any](p *Peer, rpc string, args any, ctx context.Context) (Optional[ResponseType], error) {
	// Do not continue calling RPC if p cannot be connected to
	if p == nil && p.Connect() != nil {
		return None[ResponseType](), errors.New("Could not connect")
	}
	var reply ResponseType
	call := p.client.Go(rpc, args, &reply, nil)
	select {
	case resp := <-call.Done:
		if resp != nil && resp.Error != nil {
			return None[ResponseType](), resp.Error
		}
		return Some[ResponseType](reply), nil
	case <-ctx.Done():
		return None[ResponseType](), errors.New("RPC cancelled")
	}
}

// Calls RPC on all peers in parallel. It will ignore all errors and send any replies through CHANNEL. Nonblocking.
func callPeers[ResponseType any](node *Node, rpc string, args any, timeout int, ctx context.Context, channel chan<- ResponseType) {
	for _, peer := range node.peers {
		go func(peer *Peer) {
			reply, err := callRPC[ResponseType](peer, rpc, args, timeout, ctx)
			if err == nil && reply.HasValue() {
				channel <- reply.Value()
			}
		}(peer)
	}
}
