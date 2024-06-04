import argparse
import matplotlib.pyplot as plt
import re
from datetime import datetime


# [2024-06-01T02:17:26-07:00] Successfully updated www.example.com. to 128.12.122.8 in 56.504583ms
pattern = '\[(.*?)\] Successfully updated .*? to (\d+.\d+.\d+.\d+) in (\d+\.\d+)ms'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, help='input log file')
    parser.add_argument('--file', type=str, help='output plot file')

    args = parser.parse_args()

    latency_dict = dict()
    count_dict = dict()

    start_time = None
    with open(args.log, 'r') as f:
        lines = f.readlines()

        for line in lines:
            # extract time and latency
            match = re.search(pattern, line)
            timestamp = match.group(1)
            time_obj = datetime.fromisoformat(timestamp)
            
            if start_time is None:
                start_time = time_obj
                dt = 0
            else:
                dt = (time_obj - start_time).total_seconds()

            ip_addr = match.group(2)
            latency = float(match.group(3))

            if dt not in latency_dict:
                latency_dict[dt] = latency
                count_dict[dt] = 1
            else:
                latency_dict[dt] += latency
                count_dict[dt] += 1
    
    # calculate average latency
    for key in latency_dict:
        latency_dict[key] /= count_dict[key]
    
    # plot
    # TODO
