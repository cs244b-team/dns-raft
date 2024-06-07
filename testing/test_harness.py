import argparse
import logging
import os
import random
import re
import signal
import subprocess
import time
import typing as T
from dataclasses import dataclass

IDENTITY_FILE = "~/.ssh/id_ed25519_gh"
REMOTE_USER = "user"

LOAD_BALANCER_IP = "34.83.34.185"
RAFT_PORT = 9000
DNS_SERVER_PORT = 8053

NODE_LOG_LEVEL = "DEBUG"
NODE_LOG_PATH = "node-%d.log"
GRANT_LEADER_PATTERN = r"node-(\d) granting vote to node-(\d)"
CONVERT_TO_LEADER_PATTERN = r"node-(\d) converting to leader"


@dataclass
class Node:
    node_id: int
    public_ip: str
    private_ip: str


logger = logging.getLogger("test_harness")


def init_logger():
    level = os.environ.get("LOG_LEVEL", "DEBUG")
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[%(name)s: %(asctime)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.setLevel(level)


def run_cmd(command: str, background: bool = False, **kwargs):
    if "stdout" not in kwargs:
        kwargs["stdout"] = subprocess.PIPE
    if "stderr" not in kwargs:
        kwargs["stderr"] = subprocess.PIPE

    if background:
        return subprocess.Popen(command, shell=True, **kwargs)
    else:
        return subprocess.run(command, shell=True, **kwargs)


def run_remote_cmd(node: Node, command: str, background: bool = False, **kwargs):
    cmd = f"ssh -T -i {IDENTITY_FILE} {REMOTE_USER}@{node.public_ip} '{command}'"
    return run_cmd(cmd, background=background, **kwargs)


class Cluster:
    def __init__(self, nodes: T.List[Node], log_prefix: str):
        self.nodes = nodes
        self.log_prefix = log_prefix

    def start(self) -> None:
        for node in self.nodes:
            self.stop_node(node)

        logger.debug("Starting cluster...")
        for node in self.nodes:
            self.start_node(node)

    def stop(self) -> None:
        logger.debug("Shutting cluster down...")
        for node in self.nodes:
            self.stop_node(node)

    def start_node(self, node: Node):
        node_addresses = [
            (
                f"{n.private_ip}:{RAFT_PORT}"
                if n.node_id != node.node_id
                else "0.0.0.0:9000"
            )
            for n in self.nodes
        ]

        cmd = (
            "sudo rm -rf /tmp/*.logents /tmp/*.state /tmp*.log && "
            "cd dns-raft/cmd/server && go build . && "
            f"sudo LOG_LEVEL={NODE_LOG_LEVEL} ./server --id {node.node_id} --port {DNS_SERVER_PORT} "
        )

        for addr in node_addresses:
            cmd += f"--node {addr} "

        return run_remote_cmd(
            node,
            cmd,
            background=True,
            stdout=open(
                os.path.join(self.log_prefix, NODE_LOG_PATH % node.node_id), "a+"
            ),
            stderr=subprocess.STDOUT,
        )

    def stop_node(self, node: Node) -> None:
        run_remote_cmd(node, f"sudo kill -9 $(sudo lsof -t -i:{DNS_SERVER_PORT})")

    def get_leader(self) -> T.Optional[Node]:
        suspected_leaders = {}

        for node in self.nodes:
            with open(
                os.path.join(self.log_prefix, NODE_LOG_PATH % node.node_id), "r"
            ) as f:
                for line in f:
                    matches = re.search(GRANT_LEADER_PATTERN, line)
                    if matches:
                        suspected_leaders[int(matches.group(1))] = int(matches.group(2))
                        continue
                    matches = re.search(CONVERT_TO_LEADER_PATTERN, line)
                    if matches:
                        suspected_leaders[node.node_id] = node.node_id

        if not suspected_leaders:
            return None

        leader = list(suspected_leaders.values())[0]
        nodes_agree = all(l == leader for l in suspected_leaders.values())

        # Did all nodes log something about the most recent leader election and do they all agree?
        if len(suspected_leaders) == len(self.nodes) and nodes_agree:
            return self.nodes[leader]

        return None

    def get_random_follower(self, leader: Node) -> Node:
        return random.choice([n for n in self.nodes if n.node_id != leader.node_id])


def run_client(
    server_ip: str, num_routines: int, rate: int, update_mode: bool, log_prefix: str
):
    return run_cmd(
        f"cd cmd/client && go run . --server {server_ip}:{DNS_SERVER_PORT} "
        f"--eval --routines {num_routines} --rate {rate} {'--update' if update_mode else ''}",
        background=True,
        stdout=open(os.path.join(log_prefix, "client.log"), "w"),
        stderr=subprocess.STDOUT,
    )


def test_catamaran(args):
    nodes = [
        Node(node_id=0, public_ip="34.168.204.145", private_ip="10.138.0.2"),
        Node(node_id=1, public_ip="35.203.169.53", private_ip="10.138.0.3"),
        Node(node_id=2, public_ip="35.203.184.108", private_ip="10.138.0.4"),
        Node(node_id=3, public_ip="104.199.119.116", private_ip="10.138.0.5"),
        Node(node_id=4, public_ip="35.233.157.154", private_ip="10.138.0.6"),
    ]

    cluster = Cluster(nodes, args.experiment_name)
    cluster.start()

    node_name_to_kill = "leader" if args.kill_leader else "follower"

    leader = None
    while leader is None:
        time.sleep(1)
        leader = cluster.get_leader()
        if leader is None:
            logger.debug("Cluster failed to agree on a leader, waiting to stabilize...")

    logger.info(
        f"Cluster agrees that node-{leader.node_id} is the leader, starting experiment"
    )

    client_process = run_client(
        LOAD_BALANCER_IP, args.goroutines, args.rate, args.write, args.experiment_name
    )

    logger.debug("Waiting for client to start...")
    time.sleep(5)

    if args.fault_tolerance:
        logger.info(
            f"Running for {args.kill_after}s before killing {node_name_to_kill}"
        )
        time.sleep(args.kill_after)
        node_to_kill = (
            leader if args.kill_leader else cluster.get_random_follower(leader)
        )

        logger.info(f"Killing {node_name_to_kill} (node-{node_to_kill.node_id})")
        cluster.stop_node(node_to_kill)

        logger.info(
            f"Running for {args.restart_after}s before restarting {node_name_to_kill}"
        )
        time.sleep(args.restart_after)
        logger.info(f"Restarting {node_name_to_kill} (node-{node_to_kill.node_id})")
        cluster.start_node(node_to_kill)

        logger.info(
            f"Running for {args.kill_after}s after restarting {node_name_to_kill}"
        )
        time.sleep(args.kill_after)
    else:
        logger.info(f"Running for {args.duration}s before stopping experiment")
        time.sleep(args.duration)

    client_process.kill()
    cluster.stop()


def test_bind9(args):
    client_process = run_client(
        "34.168.204.145" if args.write else LOAD_BALANCER_IP,
        args.goroutines,
        args.rate,
        args.write,
        args.experiment_name,
    )

    logger.debug("Waiting for client to start...")
    time.sleep(2)

    logger.info(f"Running for {args.duration}s before stopping experiment")
    time.sleep(args.duration)

    client_process.kill()


def main(args):
    init_logger()
    os.mkdir(args.experiment_name)
    with open(os.path.join(args.experiment_name, "settings.txt"), "w") as f:
        f.write(str(args))

    if args.bind9:
        test_bind9(args)
    else:
        test_catamaran(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--experiment-name",
        type=str,
        default=f"./testing/experiment-{int(time.time())}",
        help="Name of the experiment to store log files",
    )

    parser.add_argument(
        "-w",
        "--write",
        action="store_true",
        default=False,
        help="Run the Go client in write/update mode opposed to read/query mode",
    )
    parser.add_argument(
        "-g",
        "--goroutines",
        type=int,
        default=32,
        help="Number of client goroutines to run",
    )
    parser.add_argument(
        "-r",
        "--rate",
        type=int,
        default=32,
        help="Number of requests per second to send to the server",
    )
    parser.add_argument(
        "-t",
        "--duration",
        type=int,
        default=30,
        help="Duration of the experiment in seconds (ignored if fault tolerance is enabled)",
    )

    parser.add_argument(
        "-f",
        "--fault-tolerance",
        action="store_true",
        default=False,
        help="Run fault tolerance experiments by killing a node and restarting it",
    )
    parser.add_argument(
        "-l",
        "--kill-leader",
        action="store_true",
        default=False,
        help="Kill the leader node (default is to kill a follower)",
    )
    parser.add_argument(
        "--kill-after",
        type=int,
        default=10,
        help="Time in seconds to kill the node after experiment starts (will also be how long experiment lasts after restart)",
    )
    parser.add_argument(
        "--restart-after",
        type=int,
        default=5,
        help="Time in seconds until the killed node is restarted after it was killed",
    )

    parser.add_argument(
        "--bind9",
        action="store_true",
        default=False,
        help="Run a client against a BIND9 server instead of a Catamaran cluster",
    )

    args = parser.parse_args()

    main(args)

    os.setpgrp()
    try:
        main(args)
    finally:
        os.killpg(0, signal.SIGKILL)
