package raft

import "github.com/miekg/dns"

type CommandType uint8

const (
	Update CommandType = iota
	Remove
)

type Command struct {
	Type  CommandType
	Key   string
	Value Optional[dns.RR]
}

type LogEntry struct {
	Term int
	Cmd  Command
}

// Each entry contains command for state machine, and term when entry was received by leader (first index is 0!)
type StableLog struct {
}
