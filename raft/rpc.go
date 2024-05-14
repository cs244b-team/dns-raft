package raft

import (
	"net"
	"net/http"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

func rpcServerAddress(id NodeId) string {
	return id.String()
}

func rpcServerPath(id NodeId) string {
	return "/" + id.String()
}

func rpcServerDebugPath(id NodeId) string {
	return "/debug/" + id.String()
}

func (node *RaftNode) startRpcServer() {
	server := rpc.NewServer()
	server.Register(node)
	server.HandleHTTP(rpcServerPath(node.serverId), rpcServerDebugPath(node.serverId))

	listener, err := net.Listen("tcp", rpcServerAddress(node.serverId))
	if err != nil {
		log.Fatal("Listen error: ", err)
	}

	go http.Serve(listener, nil)
	log.Infof("%s's RPC server started", node.serverId.String())
}

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
	log.Debugf("%s received vote request from %s", node.serverId.String(), args.CandidateId.String())

	response := RequestVoteResponse{CurrentTerm: node.getCurrentTerm(), VoteGranted: false}

	// Reply false if term < currentTerm (Section 5.1)
	if args.CandidateTerm < node.getCurrentTerm() {
		log.Debugf(
			"%s rejecting vote request from %s because candidate term %d < current term %d",
			node.serverId.String(),
			args.CandidateId.String(),
			args.CandidateTerm,
			node.getCurrentTerm(),
		)
		*reply = response
		return nil
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (Section 5.4.1)
	if (!node.getVotedFor().HasValue() || node.getVotedFor().Value() == args.CandidateId) && node.isCandidateUpToDate(args.LastLogTerm, args.LastLogIndex) {
		log.Debugf("%s granting vote to %s", node.serverId.String(), args.CandidateId.String())

		node.setVotedFor(args.CandidateId)
		response.VoteGranted = true

		// TODO: synch
		node.setLastContact(time.Now())
	}

	// If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
	if args.CandidateTerm > node.getCurrentTerm() {
		log.Debugf(
			"%s converting to follower because candidate term %d > current term %d",
			node.serverId.String(),
			args.CandidateTerm,
			node.getCurrentTerm(),
		)
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
	log.Debugf("%s received append entries request from %s", node.serverId.String(), args.LeaderId.String())

	// TODO: Section 5.2, indicate that during this timeout period, we granted a vote
	// If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate

	// TODO (1) ...

	response := AppendEntriesResponse{CurrentTerm: node.getCurrentTerm(), Success: false}

	// 1. Reply false if term < currentTerm (Section 5.1)
	if args.LeaderTerm < node.getCurrentTerm() {
		log.Debugf(
			"%s rejecting AppendEntries request from %s because leader term %d < current term %d",
			node.serverId.String(),
			args.LeaderId.String(),
			args.LeaderTerm,
			node.getCurrentTerm(),
		)
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

	// TODO: 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (Section 5.3)

	// TODO 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (Section 5.3)

	// TODO 4. Append any new entries not already in the log
	// if len(args.Entries) > 0 {}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > 0 && args.LeaderCommit > node.getCommitIndex() {
		commitIdx := min(args.LeaderCommit, node.lastLogIndex())
		node.setCommitIndex(commitIdx)

		// TODO: apply logs

	}

	// TODO: synch
	node.setLastContact(time.Now())

	response.Success = true
	*reply = response

	return nil
}