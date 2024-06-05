package main

import (
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

	domain := flag.String("domain", "www.example.com.", "Domain to query for or update")
	zone := flag.String("zone", "example.com.", "DNS zone")
	server := flag.String("server", "127.0.0.1:8053", "DNS server to connect to")

	// Testing
	eval := flag.Bool("eval", false, "Run in evaluation mode")
	numRoutines := flag.Int("routines", 8, "Number of goroutines to use in evaluation mode")
	update := flag.Bool("update", false, "Send updates to server (default sends queries)")

	flag.Parse()

	serverSplit := strings.Split(*server, ":")
	if len(serverSplit) != 2 {
		log.Fatalf("Invalid server address: %s", *server)
	}

	var monitorFunc func(chan netip.Addr) = monitorIp
	if !*eval {
		client := dns.NewDDNSClient(*zone, *domain, serverSplit[0], serverSplit[1], monitorFunc)
		client.RunMonitor()
	} else {
		var wg sync.WaitGroup

		for i := 1; i <= *numRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				client := dns.NewDDNSClient(*zone, *domain, serverSplit[0], serverSplit[1], monitorFunc)
				var m *_dns.Msg
				for {
					// Either create update or query request
					if *update {
						ip := rand.Uint32()
						bytes := [4]byte{byte(ip >> 24), byte(ip >> 16), byte(ip >> 8), byte(ip)}
						addr := netip.AddrFrom4(bytes)
						m = client.CreateUpdateMessage(addr)
					} else {
						m = client.CreateQuestion(*domain)
					}
					start := time.Now()
					err := client.SendMessage(m)
					if err != nil {
						log.Errorf("Error sending request: %v", err)
					}
					log.Infof("Response latency: %v", time.Since(start))
				}
			}()
		}

		wg.Wait()
	}
}
