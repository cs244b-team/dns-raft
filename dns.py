import random
import subprocess
import time

def send_one_dns_update_to_random_NS_server(cluster, ip_str):
    server, port = random.choice(cluster)
    subprocess.call(["go", "run", "cmd/client/main.go", "-domain", "www.example.com.", "-server", f"{server}:{port}", "-oneupdate", ip_str])

def get_dns_record(cluster): # latency reported in seconds
    server_port = random.choice(cluster)
    server, port = server_port
    start_time = time.time()
    x = subprocess.run(["dig", f"@{server}", "-p", port, "www.example.com", "A"], capture_output=True)
    end_time = time.time()
    x.stderr = x.stderr.decode("utf-8")
    x.stdout = x.stdout.decode("utf-8")
    if x.stderr or 'ANSWER SECTION' not in x.stdout:
        elapsed_time = float('inf')
    else:
        elapsed_time = end_time - start_time
    return elapsed_time