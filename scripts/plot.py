import argparse
import matplotlib.pyplot as plt
import re
from datetime import datetime
import pandas as pd


# [2024-06-01T02:17:26-07:00] Successfully updated www.example.com. to 128.12.122.8 in 56.504583ms
# pattern = '\[(.*?)\] Successfully updated .*? to (\d+.\d+.\d+.\d+) in (\d+\.\d+).s'

# time="2024-06-04T19:15:18-07:00" level=info msg="Successfully updated www.example.com. to 8.8.8.8 in 253.875µs"
# pattern = 'time="(.*?)" level=info msg="Successfully updated .*? to (\d+.\d+.\d+.\d+) in (\d+\.\d+)(.)s"'

# time="2024-06-04T21:48:07-07:00" level=info msg="Response latency: 59.606917ms"
pattern = 'time="(.*?)" level=info msg="Response latency: (\d+)(.)s"'

def parse_line(line):
    match = re.search(pattern, line)
    if match is None:
        return None, None, None
    timestamp = match.group(1)
    time_obj = datetime.fromisoformat(timestamp)
    # ip_addr = match.group(2)
    latency = float(match.group(2))
    time_unit = match.group(3)
    return time_obj, latency, time_unit

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--log', type=str, help='input log file')
    parser.add_argument('--file', type=str, help='output plot file', default="")

    args = parser.parse_args()

    if args.file == "":
        args.file = args.log.replace('.log', '.png')

    latency_dict = dict()
    count_dict = dict()

    time_unit = None
    start_time = None
    filenames = args.log.split(',')
    for filename in filenames:
        with open(filename, 'r') as f:
            lines = f.readlines()

            for line in lines:
                time_obj, latency, unit = parse_line(line)

                if time_obj is None:
                    continue
                
                if start_time is None:
                    start_time = time_obj
                    time_unit = unit
                elif time_obj < start_time:
                    start_time = time_obj

    assert start_time is not None

    for filename in filenames:
        with open(filename, 'r') as f:
            lines = f.readlines()

            for line in lines:
                time_obj, latency, unit = parse_line(line)

                if time_obj is None:
                    continue
            
                dt = (time_obj - start_time).total_seconds()
                dt = int(dt * 10) * 1.0 / 10

                if dt not in latency_dict:
                    latency_dict[dt] = latency
                    count_dict[dt] = 1
                else:
                    latency_dict[dt] += latency
                    count_dict[dt] += 1
    
    # calculate average latency
    for key in latency_dict:
        latency_dict[key] /= count_dict[key]

    # drop smallest and largest key as they are not full seconds
    try:
        del latency_dict[0]
        del count_dict[0]
        del latency_dict[max(latency_dict.keys())]
        del count_dict[max(count_dict.keys())]
    except KeyError:
        pass
    
    # plot raw latency
    # plt.plot(latency_dict.keys(), latency_dict.values())
    # plt.xlabel('Time (s)')
    # plt.ylabel(f'Latency ({time_unit}s)')
    # plt.savefig(args.file, dpi=300)
    # plt.close()

    # plot moving average
    df = pd.DataFrame(list(latency_dict.items()), columns=['time', 'latency'])
    df.sort_values('time', inplace=True)
    df['moving_avg'] = df['latency'].rolling(window=10).mean()
    plt.plot(df['time'], df['moving_avg'])
    plt.xlabel('Time (s)')
    plt.ylabel('Latency (ms)')
    plt.title('Latency vs Time')
    plt.savefig(args.file, dpi=300)
    plt.close()

    df = pd.DataFrame(list(count_dict.items()), columns=['time', 'count'])
    df.sort_values('time', inplace=True)
    df['moving_sum'] = df['count'].rolling(window=10).sum()
    plt.plot(df['time'], df['moving_sum'])
    plt.xlabel('Time (s)')
    plt.ylabel('Count (request/s)')
    plt.title('Throughput vs Time')
    plt.savefig(args.file.replace('.png', '_count.png'), dpi=300)
    plt.close()

    # # plot raw count
    # new_count_dict = dict()
    # for key, val in count_dict.items():
    #     k = int(key)
    #     if k not in new_count_dict:
    #         new_count_dict[k] = val
    #     else:
    #         new_count_dict[k] += val
    # plt.plot(new_count_dict.keys(), new_count_dict.values())
    # plt.xlabel('Time (s)')
    # plt.ylabel('Count (requests/second)')
    # plt.title('Count over Time')
    # plt.savefig(args.file.replace('.png', '_count_raw.png'), dpi=300)
    # plt.close()
