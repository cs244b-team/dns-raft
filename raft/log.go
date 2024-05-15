package raft

type LogEntry struct {
	Term int
	// data generic?w 4y
}

// Each entry contains command for state machine, and term when entry was received by leader (first index is 0!)
type StableLog struct {
}
