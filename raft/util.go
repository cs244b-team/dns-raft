package raft

import (
	"math/rand"
	"net"
	"strconv"
	"time"
)

type RaftTimer struct {
	timeout time.Duration
	C       <-chan time.Time
}

func NewRandomTimer(minVal int, maxVal int) *RaftTimer {
	randVal := minVal + rand.Intn(maxVal-minVal)
	randTime := time.Duration(randVal) * time.Millisecond
	return &RaftTimer{randTime, time.After(randTime)}
}

type Address struct {
	ip   string
	port uint16
}

func NewAddress(ip string, port uint16) Address {
	return Address{ip: ip, port: port}
}

func AddressFromString(addr string) (Address, error) {
	ip, port, err := net.SplitHostPort(addr)
	if err != nil {
		return Address{}, err
	}

	portUint, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return Address{}, err
	}

	return Address{ip: ip, port: uint16(portUint)}, nil
}

func (addr *Address) String() string {
	return net.JoinHostPort(addr.ip, strconv.Itoa(int(addr.port)))
}

type IndexVoteCounter map[int]map[int]bool // [idx]: { server1: true, ... }, ...

func (counter IndexVoteCounter) AddVote(idx int, serverId int) {
	if _, exists := counter[idx]; !exists {
		counter[idx] = make(map[int]bool)
	}
	counter[idx][serverId] = true
}

func (counter IndexVoteCounter) CountVotes(idx int) int {
	if _, exists := counter[idx]; !exists {
		return 0
	}
	return len(counter[idx])
}
