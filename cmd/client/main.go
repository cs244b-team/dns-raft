package main

import (
	"context"
	"cs244b-team/dns-raft/common"
	"cs244b-team/dns-raft/dns"
	"flag"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	_dns "github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
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

var UpdateOnceIP string

func monitorIpOnce(updateChannel chan netip.Addr) {
	current, err := netip.ParseAddr(UpdateOnceIP)
	if err != nil {
		log.Fatalf("Error getting IP: %v", err)
	}

	log.Infof("Current IP: %s", current)
	updateChannel <- current
	close(updateChannel)
}

func main() {
	common.InitLogger()

	domain := flag.String("domain", "www.example.com.", "Domain to query for or update")
	zone := flag.String("zone", "example.com.", "DNS zone")
	server := flag.String("server", "127.0.0.1:8053", "DNS server to connect to")

	// Testing
	eval := flag.Bool("eval", false, "Run in evaluation mode")
	update := flag.Bool("update", false, "Send updates to server (default sends queries)")
	numRoutines := flag.Int("routines", 1, "Number of concurrent routines")
	sendRate := flag.Int("rate", 1, "Rate of queries/updates per second")
	flag.Parse()

	serverSplit := strings.Split(*server, ":")
	if len(serverSplit) != 2 {
		log.Fatalf("Invalid server address: %s", *server)
	}

	if !*eval {
		client := dns.NewDDNSClient(*zone, *domain, serverSplit[0], serverSplit[1], monitorIp)
		client.RunMonitor()
		return
	}

	// If we are running in query mode, we should create at least one record to avoid NXDOMAIN responses
	if !*update {
		client := dns.NewDDNSClient(*zone, *domain, serverSplit[0], serverSplit[1], nil)
		addr, err := getIp()
		if err != nil {
			log.Fatalf("Error getting IP: %v", err)
		}

		m := client.CreateUpdateMessage(addr)
		_, _, err = client.SendMessage(m)
		if err != nil {
			log.Fatalf("Error sending request: %v", err)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < *numRoutines; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			client := dns.NewDDNSClient(*zone, *domain, serverSplit[0], serverSplit[1], nil)
			limiter := rate.NewLimiter(rate.Limit(*sendRate/(*numRoutines)), *sendRate/(*numRoutines))

			j := 0
			for {
				limiter.Wait(ctx)

				var m *_dns.Msg

				if *update {
					// Alternate between updating and clearning the zone
					ip := rand.Uint32()
					bytes := [4]byte{byte(ip >> 24), byte(ip >> 16), byte(ip >> 8), byte(ip)}
					addr := netip.AddrFrom4(bytes)
					m = client.CreateUpdateMessage(addr)
				} else {
					m = client.CreateQuestion(*domain)
				}

				if *update && (id == 0 && j%3 == 0) {
					m = client.CreateRemoveNameMesssage()
				}

				_, rtt, err := client.SendMessage(m)
				if err != nil {
					log.Errorf("Error sending request: %v", err)
				} else {
					log.Infof("Response latency: %dms", rtt/1e6)
				}

				j++
			}
		}(i)
	}

	wg.Wait()
}
