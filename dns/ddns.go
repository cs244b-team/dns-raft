package dns

import (
	"cs244b-team/dns-raft/raft"
	"fmt"
	"net/netip"
	"time"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

const (
	ResourceRecordTTL = 64
	UpdateTimeout     = 5 * time.Second
)

type DDNSClient struct {
	zone       string
	domain     string
	monitorIp  func(chan netip.Addr)
	serverConn *dns.Conn
	dnsClient  *dns.Client
	serverPort string
}

func NewDDNSClient(zone string, domain string, server string, serverPort string, monitorIp func(chan netip.Addr)) *DDNSClient {
	client, conn := connect(server, serverPort)
	log.Infof("Created dynamic DNS client for %s in zone %s with server %s", domain, zone, server)
	return &DDNSClient{
		zone:       zone,
		domain:     domain,
		monitorIp:  monitorIp,
		serverConn: conn,
		dnsClient:  &client,
		serverPort: serverPort,
	}
}

func (c *DDNSClient) Run() {
	ch := make(chan netip.Addr)
	go c.monitorIp(ch)
	for addr := range ch {
		record := c.createUpdateRecord(addr)
		log.Debugf("Updating %s to %s", c.domain, addr)
		c.sendUpdate(addr, record)
	}
}

func (c *DDNSClient) sendUpdate(addr netip.Addr, record *dns.Msg) {
	timer := time.NewTimer(UpdateTimeout)
	start := time.Now()
	for {
		select {
		case <-timer.C:
			log.Errorf("Update for %s to %s timed out", c.domain, addr)
			return
		default:
			err := c.sendUpdateOnce(record)
			if err == nil {
				log.Infof("Successfully updated %s to %s in %v", c.domain, addr, time.Since(start))
				return
			}
			log.Errorf("Error updating %s to %s: %v", c.domain, addr, err)
		}
	}
}

func (c *DDNSClient) sendUpdateOnce(record *dns.Msg) error {
	reply, _, err := c.dnsClient.ExchangeWithConn(record, c.serverConn)

	if err != nil {
		return err
	}

	if reply.Id != record.Id {
		return fmt.Errorf("received response with mismatched ID: %d != %d", reply.Id, record.Id)
	}

	if reply.Rcode != dns.RcodeSuccess {
		return fmt.Errorf("received error response: %v", dns.RcodeToString[reply.Rcode])
	}

	return nil
}

func connect(server string, serverPort string) (dns.Client, *dns.Conn) {
	client := dns.Client{Net: "udp"}
	conn, err := client.Dial(server + ":" + serverPort)
	if err != nil {
		log.Fatalf("Error dialing server: %v", err)
	}
	return client, conn
}

func (c *DDNSClient) createUpdateRecord(addr netip.Addr) *dns.Msg {
	m := new(dns.Msg)
	m.SetUpdate(c.zone)
	m.Insert([]dns.RR{
		&dns.A{
			Hdr: dns.RR_Header{
				Name:   c.domain,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    ResourceRecordTTL,
			},
			A: addr.AsSlice(),
		},
	})
	return m
}

type DDNSServer struct {
	inner    *dns.Server
	raftNode *raft.Node
}

func NewDDNSServer(
	dnsListenPort int,
	raftNodeId int,
	raftCluster []raft.Address,
	raftConfig raft.Config,
) *DDNSServer {
	address := fmt.Sprintf("0.0.0.0:%v", dnsListenPort)
	server := &DDNSServer{
		inner:    &dns.Server{Addr: address, Net: "udp"},
		raftNode: raft.NewNode(raftNodeId, raftCluster, raftConfig),
	}
	server.inner.MsgAcceptFunc = server.msgAcceptFunc
	dns.HandleFunc(".", func(w dns.ResponseWriter, m *dns.Msg) {
		server.handleRequest(w, m)
	})
	return server
}

func (s *DDNSServer) Run() {
	go func() {
		if err := s.inner.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()
	s.raftNode.ConnectToCluster()
	s.raftNode.Run()
}

func (s *DDNSServer) handleRequest(w dns.ResponseWriter, r *dns.Msg) {
	if r.Opcode == dns.OpcodeQuery {
		s.handleQueryRequest(w, r)
	} else if r.Opcode == dns.OpcodeUpdate {
		s.handleUpdateRequest(w, r)
	} else {
		m := new(dns.Msg)
		m.SetReply(r)
		m.SetRcode(r, dns.RcodeNotImplemented)
		w.WriteMsg(m)
	}
}

func (s *DDNSServer) handleQueryRequest(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)

	for _, question := range r.Question {
		ip, ok := s.raftNode.GetValue(question.Name)
		if !ok {
			log.Warnf("`A` record not found for %s", question.Name)
			m.Rcode = dns.RcodeNameError
		} else {
			answer := &dns.A{
				Hdr: dns.RR_Header{
					Name:   question.Name,
					Rrtype: dns.TypeA,
					Class:  dns.ClassINET,
					Ttl:    60,
				},
				A: ip,
			}
			m.Answer = append(m.Answer, answer)
		}
	}

	w.WriteMsg(m)
}

func (s *DDNSServer) handleUpdateRequest(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)

	if len(r.Ns) == 0 {
		m.Rcode = dns.RcodeFormatError
	} else {
		// TODO get update type
		err := s.raftNode.UpdateValue(r.Ns[0].Header().Name, r.Ns[0].(*dns.A).A)
		if err != nil {
			m.Rcode = dns.RcodeServerFailure
			log.Errorf("Failed to update: %v", err)
		}
	}

	// println("Received update request:")
	// println(m.String())

	w.WriteMsg(m)
}

func (s *DDNSServer) msgAcceptFunc(dh dns.Header) dns.MsgAcceptAction {
	opcode := int(dh.Bits>>11) & 0xF
	if opcode == dns.OpcodeUpdate {
		return dns.MsgAccept
	}
	return dns.DefaultMsgAcceptFunc(dh)
}
