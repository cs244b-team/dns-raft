package main

type Term int

type NodeId int

type LogEntry struct {
	term Term
	// data generic?w 4y
}

type RaftNode struct {
	// Persistent state on all servers
	// TODO: make these persistent
	currentTerm Term       // Latest term server has seen (init to zero)
	votedFor    NodeId     // Candidate id that received vote in current term (-1 if none)
	log         []LogEntry // Each entry contains command for state machine, and term when entry was received by leader (first index is 0!)

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed (initialized to 0)
	lastApplied int // Index of highest log entry applied to state machine (initialized to 0)

	// Volatile state on leaders
	nextIndex  map[NodeId]int // For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[NodeId]int // For each server, index of highest log entry known to be replicated on server
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

func (node *RaftNode) convertToFollower() {}

type RequestVoteArgs struct {
	candiateTerm Term
	candidateId  NodeId
	lastLogIndex int
	lastLogTerm  Term
}

type RequestVoteResponse struct {
	currentTerm Term
	voteGranted bool
}

// Invoked by candidates to gather votes, not called directly by this RaftNode (Section 5.2)
func (node *RaftNode) RequestVote(args RequestVoteArgs) RequestVoteResponse {
	reply := RequestVoteResponse{currentTerm: node.currentTerm, voteGranted: false}

	// Reply false if term < currentTerm (Section 5.1)
	if args.candiateTerm < node.currentTerm {
		return reply
	}

	// TODO: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (Section 5.1)
	// node.convertToFollower()

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (Section 5.4.1)
	if (node.votedFor == -1 || node.votedFor == args.candidateId) && node.isCandidateUpToDate(args.lastLogTerm, args.lastLogIndex) {
		node.votedFor = args.candidateId
		reply.voteGranted = true

		// TODO: Section 5.2, indicate that during this timeout period, we granted a vote
		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate
	}

	return reply
}

// Determine if a candidate's log is at least as up-to-date as the receiver's log
func (node *RaftNode) isCandidateUpToDate(lastLogTerm Term, lastLogIndex int) bool {
	return lastLogTerm > node.LastLogTerm() || (lastLogTerm == node.LastLogTerm() && lastLogIndex > node.LastLogIndex())
}

type AppendEntriesArgs struct {
	leaderTerm   Term
	leaderId     NodeId     // So follower can redirect clients
	prevLogIndex int        // Index of log entry immediately preceding new ones
	prevLogTerm  int        // Term of prevLogIndex entry
	entries      []LogEntry // Log entries to store (empty for heartbeat; may send more than one for efficiency)
	leaderCommit int        // Leader's commit index
}

type AppendEntriesResponse struct {
	currentTerm Term
	success     bool
}

// Invoked by leader to replicate log entries (Section 5.3); also used as a heartbeat
func (node *RaftNode) AppendEntries(args AppendEntriesArgs) AppendEntriesResponse {
	reply := AppendEntriesResponse{currentTerm: node.currentTerm, success: false}
	return reply
}
