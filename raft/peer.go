package raft

import "net/rpc"

type Peer struct {
	id      int
	address Address
	client  *rpc.Client
}

func NewPeer(id int, address Address) *Peer {
	return &Peer{id: id, address: address}
}

func (p *Peer) Connect() error {
	client, err := rpc.DialHTTPPath("tcp", p.address.String(), rpcServerPath(p.id))
	if err != nil {
		return err
	}
	p.client = client
	return nil
}

func (p *Peer) Disconnect() {
	p.client.Close()
}

func (p *Peer) RequestVote(args RequestVoteArgs) (RequestVoteResponse, error) {
	var reply RequestVoteResponse
	err := p.client.Call("Node.RequestVote", args, &reply)
	return reply, err
}

func (p *Peer) AppendEntries(args AppendEntriesArgs) (AppendEntriesResponse, error) {
	var reply AppendEntriesResponse
	err := p.client.Call("Node.AppendEntries", args, &reply)
	return reply, err
}
