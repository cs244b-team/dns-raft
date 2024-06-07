package raft

import (
	"net/rpc"
	"sync"
)

type Peer struct {
	id      int
	address Address
	client  *rpc.Client
	mu      sync.RWMutex
}

func NewPeer(id int, address Address) *Peer {
	return &Peer{id: id, address: address}
}

// Do not call with any of p's locks held
func (p *Peer) Connect() error {
	p.mu.RLock()
	// Check if we need to connect
	if p.client != nil {
		p.mu.RUnlock()
		return nil
	}
	// We need to connect since we have a nil client
	_addr := p.address.String()
	_pid := p.id
	p.mu.RUnlock()
	client, err := rpc.DialHTTPPath("tcp", _addr, rpcServerPath(_pid))
	// We set a client, so we take a write lock
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.client = nil
		return err
	}
	p.client = client
	return nil
}

func (p *Peer) Disconnect() {
	p.mu.Lock()
	p.client.Close()
	p.mu.Unlock()
}
