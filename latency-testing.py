cluster = [
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

### Latencies for reads
latencies = {}
throughputs = [1000, 10000, 50000, 100000]
for throughput in throughputs: # THROUGHPUT reqs/s
    concurrency = 1000 # we send CONCURRENCY requests in a batch
    # We can model the events as arriving with a poisson lambda of THROUGHPUT/CONCURRENCY.
    # Each event, CONCURRENCY requests are sent, so a total of THROUGHPUT requests will be send each second
    LAMBDA = throughput/concurrency
    with Pool(4) as pool:
        res = []
        for event in range(throughput // concurrency):
            multiple_results = [pool.apply_async(_dns.get_dns_record, (cluster,)) for i in range(1000)]
            res += multiple_results
            time.sleep(np.random.exponential(1/LAMBDA))
        latencies[throughput] = [r.get(timeout=5) for r in res]

for throughput in throughputs:
    print('latency (ms) for throughput: ', np.percentile(latencies[throughput], 99) * 1000)

### Latencies for writes (incomplete)
x = subprocess.run(["go", "run", "cmd/client/main.go", "-domain", "www.example.com.", "-server", 
                    f"127.0.0.1:7000", "-oneupdate", "10.200.201.9"], capture_output=True)
x.stderr = x.stderr.decode("utf-8")
x.stdout = x.stdout.decode("utf-8")
x