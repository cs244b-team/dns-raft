package main

import (
	"cs244b-team/dns-raft/common"
	"cs244b-team/dns-raft/dns"
	"flag"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	MonitorInterval    = 500 * time.Millisecond
	IpRetrievalTimeout = 1 * time.Second
)

func getIp() (netip.Addr, error) {
	c := &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   IpRetrievalTimeout,
				KeepAlive: IpRetrievalTimeout,
			}).Dial,
			TLSHandshakeTimeout:   IpRetrievalTimeout,
			ResponseHeaderTimeout: IpRetrievalTimeout,
		},
	}

	resp, err := c.Get("https://checkip.amazonaws.com/")
	if err != nil {
		log.Warnf("Error getting IP: %v", err)
		return netip.Addr{}, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading IP: %v", err)
	}

	addr, err := netip.ParseAddr(strings.TrimSpace(string(body)))
	if err != nil {
		log.Fatalf("Error parsing IP: %v", err)
	}

	return addr, nil
}

func monitorIp(updateChannel chan netip.Addr) {
	current, err := getIp()
	if err != nil {
		log.Fatalf("Error getting IP: %v", err)
	}

	log.Infof("Current IP: %s", current)
	updateChannel <- current

	ticker := time.NewTicker(MonitorInterval)
	for range ticker.C {
		new, err := getIp()
		if err != nil {
			continue
		}

		if new != current {
			log.Infof("IP changed from %s to %s", current, new)
			current = new
			updateChannel <- new
		}
	}
}

func main() {
	common.InitLogger()

	zone := flag.String("zone", "example.com.", "DNS zone")
	domain := flag.String("domain", "www.example.com.", "Domain to update")
	server := flag.String("server", "127.0.0.1:8053", "DNS server")
	eval := flag.Bool("eval", false, "Run in evaluation mode")
	flag.Parse()

	// TODO: implement monitorIp for evaluation mode (w/o fetching IP from AWS)
	if *eval {
		log.Fatal("Evaluation mode not implemented")
	}

	serverSplit := strings.Split(*server, ":")
	if len(serverSplit) != 2 {
		log.Fatalf("Invalid server address: %s", *server)
	}

	client := dns.NewDDNSClient(*zone, *domain, serverSplit[0], serverSplit[1], monitorIp)
	client.Run()
}
