package dns

import (
	"cs244b-team/dns-raft/raft"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

const (
	ResourceRecordTTL = 64
	UpdateTimeout     = 5 * time.Second
)

func connect(server string, serverPort string) (dns.Client, *dns.Conn) {
	client := dns.Client{Net: "udp"}
	conn, err := client.Dial(server + ":" + serverPort)
	if err != nil {
		log.Fatalf("Error dialing server: %v", err)
	}
	return client, conn
}

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
	log.Debugf("Created dynamic DNS client for %s in zone %s with server %s", domain, zone, server)
	return &DDNSClient{
		zone:       zone,
		domain:     domain,
		monitorIp:  monitorIp,
		serverConn: conn,
		dnsClient:  &client,
		serverPort: serverPort,
	}
}

func (c *DDNSClient) RunMonitor() {
	ch := make(chan netip.Addr)
	go c.monitorIp(ch)
	for addr := range ch {
		m := c.CreateUpdateMessage(addr)
		log.Debugf("Updating %s to %s", c.domain, addr)
		c.SendUpdate(addr, m)
	}
}

func (c *DDNSClient) SendMessage(m *dns.Msg) (r *dns.Msg, rtt time.Duration, err error) {
	reply, rtt, err := c.dnsClient.ExchangeWithConn(m, c.serverConn)

	if err != nil {
		return nil, 0, err
	}

	if reply.Id != m.Id {
		return nil, 0, fmt.Errorf("received response with mismatched ID: %d != %d", reply.Id, m.Id)
	}

	if reply.Rcode != dns.RcodeSuccess {
		return nil, 0, fmt.Errorf("received error response: %v", dns.RcodeToString[reply.Rcode])
	}

	return reply, rtt, err
}

func (c *DDNSClient) SendUpdate(addr netip.Addr, m *dns.Msg) {
	timer := time.NewTimer(UpdateTimeout)
	for {
		select {
		case <-timer.C:
			log.Errorf("Update for %s to %s timed out", c.domain, addr)
			return
		default:
			// TODO: handle removal of domain for demo
			_, rtt, err := c.SendMessage(m)
			if err == nil {
				log.Infof("Successfully updated %s to %s in %v", c.domain, addr, rtt)
				return
			}
			log.Errorf("Error updating %s to %s: %v", c.domain, addr, err)
		}
	}
}

func (c *DDNSClient) CreateQuestion(domain string) *dns.Msg {
	m := new(dns.Msg)
	m.SetQuestion(domain, dns.TypeA)
	return m
}

func (c *DDNSClient) CreateUpdateMessage(addr netip.Addr) *dns.Msg {
	m := new(dns.Msg)
	m.SetUpdate(c.zone)
	if addr.Is6() {
		m.Insert([]dns.RR{
			&dns.AAAA{
				Hdr: dns.RR_Header{
					Name:   c.domain,
					Rrtype: dns.TypeAAAA,
					Class:  dns.ClassINET,
					Ttl:    ResourceRecordTTL,
				},
				AAAA: addr.AsSlice(),
			},
		})
	} else {
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
	}
	return m
}

func (c *DDNSClient) CreateRemoveRRsetMessage() *dns.Msg {
	m := new(dns.Msg)
	m.SetUpdate(c.zone)
	m.RemoveRRset([]dns.RR{
		&dns.ANY{
			Hdr: dns.RR_Header{
				Name: c.domain,
			},
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
		records, ok := s.raftNode.GetValue(question.Name)
		if !ok {
			log.Warnf("Record not found for %s", question.Name)
			m.Rcode = dns.RcodeNameError
		} else {
			for _, record := range records {
				if question.Qtype == record.Header().Rrtype || question.Qtype == dns.TypeANY {
					if record.Header().Rrtype == dns.TypeNS {
						m.Ns = append(m.Ns, record)
					} else {
						m.Answer = append(m.Answer, record)
					}
				}
			}

			// Keep response tidy, regardless of record insertion order
			sort.Slice(m.Answer, func(i, j int) bool {
				return m.Answer[i].Header().Rrtype < m.Answer[j].Header().Rrtype
			})
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
		for _, record := range r.Ns {
			var err error
			if record.Header().Class == dns.ClassANY {
				// Delete all RRs from a name
				err = s.raftNode.UpdateValue(record.Header().Name, raft.Remove, raft.None[dns.RR]())
			} else {
				err = s.raftNode.UpdateValue(record.Header().Name, raft.Update, raft.Some(record))
			}
			if err != nil {
				m.Rcode = dns.RcodeServerFailure
				log.Errorf("Failed to update: %v", err)
			}
		}
	}

	w.WriteMsg(m)
}

func (s *DDNSServer) msgAcceptFunc(dh dns.Header) dns.MsgAcceptAction {
	opcode := int(dh.Bits>>11) & 0xF
	if opcode == dns.OpcodeUpdate {
		return dns.MsgAccept
	}
	return dns.DefaultMsgAcceptFunc(dh)
}
