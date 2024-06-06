import random
import ipaddress
import dns
from dns import message
from dns import query
from dns import update
from dns import rcode
from dns import rdatatype
import socket

UPDATE_TIMEOUT = 10
RESOURCE_RECORD_TTL = 300

class DDNSClient(object):
    def __init__(self, zone, domain, server, server_port, monitor_ip):
        self.zone = zone
        self.domain = domain
        self.monitor_ip = monitor_ip
        self.server = server
        self.server_port = server_port
        # self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # self.socket.connect((server, server_port)) 

    def create_question(self, domain: str):
        m = dns.message.make_query(domain, dns.rdatatype.A)
        return m
    
    def send_message(self, m):
        try:
            response = dns.query.udp(m, self.server, port=self.server_port)
            rtt = response.time
        except Exception as e:
            print("Exception: ", e)
            return None, 0, e

        if response.id != m.id:
            return None, 0, Exception(f"received response with mismatched ID: {response.id} != {m.id}")

        if response.rcode() != dns.rcode.NOERROR:
            return None, 0, Exception(f"received error response: {dns.rcode.to_text(response.rcode())}")

        return response, rtt, None

    def create_update_message(self, domain, addr):
        m = dns.update.Update(self.zone)
        addr_ip = ipaddress.ip_address(addr)
        if addr_ip.version == 6:
            m.add(domain, RESOURCE_RECORD_TTL, dns.rdatatype.AAAA, addr_ip.exploded)
        else:
            m.add(domain, RESOURCE_RECORD_TTL, dns.rdatatype.A, addr_ip.exploded)
        return m



def generate_random_ip():
    ip = random.getrandbits(32)
    return ipaddress.ip_address(ip)

if __name__ == '__main__':
    # TODO: rename dns.py to import the dnspython package by 'import dns'
    # TODO: parse args
    ddns_client = DDNSClient(zone="example.com.", 
                             domain="", 
                             server = "127.0.0.1", 
                             server_port = 8053, 
                             monitor_ip="",
                            )
    
    # TODO: timeouts
    addr = generate_random_ip()
    print(addr)

    msg = ddns_client.create_update_message("www.example.com.", addr)
    response, rtt, _ = ddns_client.send_message(msg)
    print(response, rtt)

    msg = ddns_client.create_question("www.example.com.")
    response, rtt, _ = ddns_client.send_message(msg)
    print(response, rtt)
    

