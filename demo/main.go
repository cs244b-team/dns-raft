package main

import (
	"cs244b-team/dns-raft/dns"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/netip"
	"time"

	log "github.com/sirupsen/logrus"
)

func GetLocalIP() (netip.Addr, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return netip.Addr{}, err
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				addr, err := netip.ParseAddr(ipnet.IP.String())
				if err != nil {
					return netip.Addr{}, err
				}
				return addr, nil
			}
		}
	}
	return netip.Addr{}, errors.New("No local address found")
}

func connected() (ok bool) {
	_, err := http.Get("http://clients3.google.com/generate_204")
	if err != nil {
		return false
	}
	return true
}

func monitorIp(updateChannel chan netip.Addr) {
	current, err := GetLocalIP()
	if err != nil {
		log.Fatalf("Error getting IP: %v", err)
	}

	log.Infof("Current IP: %s", current)
	updateChannel <- current

	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		new, err := GetLocalIP()
		if err != nil {
			continue
		}

		if new != current && connected() {
			log.Infof("IP changed from %s to %s", current, new)
			current = new
			updateChannel <- new
		}
	}
}

func launchDemoClient(zone string, domain string, dnsServerIP string, portString string) {
	client := dns.NewDDNSClient(zone, domain, dnsServerIP, portString, monitorIp)
	client.RunMonitorWithDelete()
	return
}

type PageData struct {
	LocalIP string
}

func main() {
	tmpl := template.Must(template.ParseFiles("home.html"))

	http.HandleFunc("/omg_img.jpg", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "omg_img.jpg")
	})
	http.HandleFunc("/main.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "main.css")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		current, err := GetLocalIP()
		if err != nil {
			log.Errorf("Could not find ip for reason %v", err)
			fmt.Fprintln(w, "We did not find an IP :(")
			return
		}
		data := PageData{LocalIP: current.String()}
		tmpl.Execute(w, data)
	})

	go launchDemoClient("catamaran.com.", "www.catamaran.com.", "34.83.34.185", "53")
	fmt.Println("Server starting on port 80...")
	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Println("Error starting server:", err)
	}
}
