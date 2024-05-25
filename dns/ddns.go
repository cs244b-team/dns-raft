package dns

import (
	"net/netip"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
)

const (
	ResourceRecordTTL = 64
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

func (c *DDNSClient) Run() {
	ch := make(chan netip.Addr)
	go c.monitorIp(ch)
	for addr := range ch {
		record := c.createUpdateRecord(addr)
		log.Infof("Updating %s to %s", c.domain, addr)
		c.sendUpdate(record)
	}
}

func (c *DDNSClient) sendUpdate(record *dns.Msg) {
	reply, rtt, err := c.dnsClient.ExchangeWithConn(record, c.serverConn)
	if err != nil {
		log.Warnf("Error updating record: %v", err)
		return
	}

	if reply.Id != record.Id {
		log.Warnf("Error updating record: ID mismatch")
		return
	}

	if reply.Rcode != dns.RcodeSuccess {
		log.Printf("Reply: %v", reply)
		log.Warnf("Error updating record: %v", dns.RcodeToString[reply.Rcode])
		return
	}

	// If there is an NS record we need to retry the update with this new server (updates go to the Raft leader)
	if len(reply.Ns) > 0 {
		log.Debugf("Error updating record: NS record found")
		server := reply.Ns[0].(*dns.NS).Ns

		// Find the matching A record in the additional section
		var addr string
		for _, extra := range reply.Extra {
			if extra.Header().Name == server {
				addr = extra.(*dns.A).A.String()
				break
			}
		}

		if addr == "" {
			log.Warnf("Error updating record: No A record found for new nameserver %s", server)
			return
		}

		c.serverConn.Close()

		// Create a new client with the new server
		client, conn := connect(addr, c.serverPort)
		c.serverConn = conn
		c.dnsClient = &client

		// Retry the update
		log.Infof("Retrying update with new server %s", addr+":"+c.serverPort)
		c.sendUpdate(record)
	}

	log.Infof("Record updated in %v", rtt)
}

// type DDNSServer struct {
// }

// func (s *DDNSServer) Run() {

// }

// func (s *DDNSServer) handleRequest() {
// }
