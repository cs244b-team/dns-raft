package main

import (
	log "github.com/sirupsen/logrus"

	"github.com/miekg/dns"
)

func handleRequest(w dns.ResponseWriter, r *dns.Msg) {
	if r.Opcode == dns.OpcodeQuery {
		handleQueryRequest(w, r)
	} else if r.Opcode == dns.OpcodeUpdate {
		handleUpdateRequest(w, r)
	} else {
		m := new(dns.Msg)
		m.SetReply(r)
		m.SetRcode(r, dns.RcodeNotImplemented)
		w.WriteMsg(m)
	}
}

func handleQueryRequest(w dns.ResponseWriter, r *dns.Msg) {
	// TODO: a node will read from the Raft layer and respond to the query request
}

func handleUpdateRequest(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)

	println("Received update request:")
	println(r.String())

	// Raft leader would send a reply of the following format to the DDNS client
	// m.Ns = append(m.Ns, &dns.NS{
	// 	Hdr: dns.RR_Header{
	// 		Name:   "example.com.",
	// 		Rrtype: dns.TypeNS,
	// 		Class:  dns.ClassINET,
	// 		Ttl:    60,
	// 	},
	// 	Ns: "ns1.example.com.",
	// })

	// m.Extra = append(m.Extra, &dns.A{
	// 	Hdr: dns.RR_Header{
	// 		Name:   "ns1.example.com.",
	// 		Rrtype: dns.TypeA,
	// 		Class:  dns.ClassINET,
	// 		Ttl:    60,
	// 	},
	// 	A: net.ParseIP("192.168.0.1").To4(),
	// })

	w.WriteMsg(m)
}

func msgAcceptFunc(dh dns.Header) dns.MsgAcceptAction {
	opcode := int(dh.Bits>>11) & 0xF
	if opcode == dns.OpcodeUpdate {
		return dns.MsgAccept
	}
	return dns.DefaultMsgAcceptFunc(dh)
}

func main() {
	dns.HandleFunc(".", handleRequest)
	server := &dns.Server{Addr: "127.0.0.1:8053", Net: "udp"}
	server.MsgAcceptFunc = msgAcceptFunc
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
