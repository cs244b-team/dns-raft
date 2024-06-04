# Latency testing for raft-based DNS
# Make sure to start the cluster and modify this cluster output before
# running this script
dns_cluster = [
    ("127.0.0.1", "7000"),
    ("127.0.0.1", "7001"),
    ("127.0.0.1", "7002"),
]

import random
random.seed(100)

from multiprocessing import Pool
import time
import dns as _dns
import numpy as np

if __name__ == '__main__':
    ### Latencies for reads
    latencies = {}
    throughputs = [1000,]
    for throughput in throughputs: # THROUGHPUT reqs/s
        concurrency = 1000 # we send CONCURRENCY requests in a batch
        # We can model the events as arriving with a poisson lambda of THROUGHPUT/CONCURRENCY.
        # Each event, CONCURRENCY requests are sent, so a total of THROUGHPUT requests will be send each second
        LAMBDA = throughput/concurrency
        with Pool(4) as pool:
            res = []
            for event in range(throughput // concurrency):
                multiple_results = [pool.apply_async(_dns.get_dns_record, (dns_cluster,)) for i in range(1000)]
                res += multiple_results
                time.sleep(np.random.exponential(1/LAMBDA))
            latencies[throughput] = [r.get(timeout=5) for r in res]

    for throughput in throughputs:
        print('latency (ms) for throughput: ', np.percentile(latencies[throughput], 99) * 1000)

    ### Latencies for writes
    import socket
    import struct
    def generate_random_ipv4():
        return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))

    latencies = {}
    throughputs = [1000]
    for throughput in throughputs: # THROUGHPUT reqs/s
        concurrency = 1000 # we send CONCURRENCY requests in a batch
        # We can model the events as arriving with a poisson lambda of THROUGHPUT/CONCURRENCY.
        # Each event, CONCURRENCY requests are sent, so a total of THROUGHPUT requests will be send each second
        LAMBDA = throughput/concurrency
        with Pool(4) as pool:
            res = []
            for event in range(throughput // concurrency):
                multiple_results = [pool.apply_async(_dns.write_dns_record, (dns_cluster, "www.example.com.", generate_random_ipv4())) for i in range(1000)]
                res += multiple_results
                time.sleep(np.random.exponential(1/LAMBDA))
            latencies[throughput] = [r.get(timeout=5) for r in res]

    for throughput in throughputs:
        print('latency (ms) for throughput: ', np.percentile(latencies[throughput], 99) * 1000)