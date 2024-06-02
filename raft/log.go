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
