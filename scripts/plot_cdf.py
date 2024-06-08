import argparse
import matplotlib.pyplot as plt
import re
from datetime import datetime
import pandas as pd
import os
import numpy as np
from scipy import stats

# time="2024-06-04T21:48:07-07:00" level=info msg="Response latency: 59.606917ms"
pattern = 'time="(.*?)" level=info msg="Response latency: (\d+)(.)s"'

def parse_line(line):
    match = re.search(pattern, line)
    if match is None:
        return None, None, None
    timestamp = match.group(1)
    time_obj = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    latency = float(match.group(2))
    time_unit = match.group(3)
    return time_obj, latency, time_unit


def parse_file(filename, start_dt, end_dt):
    # key: latency, value: count
    latency_dict = dict()
    start_time = None

    with open(filename, 'r') as f:
        lines = f.readlines()
        for line in lines:
            time_obj, latency, _ = parse_line(line)

            if time_obj is None:
                continue

            if start_time is None:
                start_time = time_obj
            else:
                dt = (time_obj - start_time).total_seconds()

                if dt >= start_dt and dt < end_dt:
                    if latency not in latency_dict:
                        latency_dict[latency] = 1
                    else:
                        latency_dict[latency] += 1
    
    return latency_dict


def parse_all_trials(data_dir, prefix, num_trials, start_dt, end_dt):
    latency_dict = dict()
    for i in range(num_trials):
        filename = os.path.join(data_dir, prefix + f'_trial_{i+1}', 'client.log')
        start_time = None

        with open(filename, 'r') as f:
            lines = f.readlines()
            for line in lines:
                time_obj, latency, _ = parse_line(line)

                if time_obj is None:
                    continue

                if start_time is None:
                    start_time = time_obj
                else:
                    dt = (time_obj - start_time).total_seconds()

                    if dt >= start_dt and dt <= end_dt:
                        if latency not in latency_dict:
                            latency_dict[latency] = 1
                        else:
                            latency_dict[latency] += 1

    return latency_dict


line_styles = ['solid', 'dotted', 'dashed', 'dashdot']


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefix', type=str, help='experiment dir prefix')
    parser.add_argument('--out', type=str, help='output plot file')
    parser.add_argument('--bind', action='store_true', help='plot bind data')
    parser.add_argument('--start', type=int, help='start time in seconds', default=2)
    parser.add_argument('--end', type=int, help='end time in seconds', default=32)
    parser.add_argument('--num_trials', type=int, help='number of trials', default=3)

    args = parser.parse_args()
    
    prefixes = args.prefix.split(',')
    data_dir = 'bind-data' if args.bind else 'data'

    for prefix, line_style in zip(prefixes, line_styles):
        latency_dict = parse_all_trials(data_dir, prefix, args.num_trials, args.start, args.end)

        print(len(latency_dict.keys()))
        latencies = sorted(latency_dict.keys())
        counts = [latency_dict[latency] for latency in latencies]
    
        # Calculate cumulative sum
        cum_counts = [sum(counts[:i+1]) for i in range(len(counts))]
    
        # Normalize cumulative counts
        total_counts = sum(counts)
        cdf = [count / total_counts for count in cum_counts]

        print(latencies, cdf)
    
        # Plot CDF
        plt.plot(latencies, cdf, label=prefix, linestyle=line_style)
    
    plt.xlabel('Latency')
    plt.ylabel('CDF')

    suffix = 'read' if args.prefix.startswith('read') else 'write'
    if suffix == 'read':
        plt.xlim(None, 96)
    else:
        plt.xlim(None, 1100)
    
    plt.title('Latency CDF')
    plt.grid(True)
    plt.legend()
    
    outfile = f'bind_{suffix}_cdf.png' if args.bind else f'raft_{suffix}_cdf.png'
    plt.savefig(outfile, dpi=300)
