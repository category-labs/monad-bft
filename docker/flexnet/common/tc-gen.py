# generate the tc script for each node in the net
#!/usr/bin/python3
import argparse
from collections import defaultdict
from pathlib import Path
from dataclasses import dataclass, fields
import dataclasses

"""
 delay  TIME [ JITTER [ CORRELATION ]]]
              Delays the packets before sending.  The optional
              parameters allow introducing a delay variation and a
              correlation.  Delay and jitter values are expressed in
              milliseconds; Correlation is set by specifying a percent
              of how much the previous delay will impact the current
              random value.
"""


@dataclass
class LatencyProfile:
    latency_ms: int = 0
    jitter_ms: int = 0
    correlation: float = 0.0
    loss: float = 0.0

    def __post_init__(self):
        # Loop through the fields
        for field in fields(self):
            # If there is a default and the value of the field is none we can assign a value
            if (
                not isinstance(field.default, dataclasses._MISSING_TYPE)
                and getattr(self, field.name) is None
            ):
                setattr(self, field.name, field.default)


parser = argparse.ArgumentParser()
parser.add_argument("topology_input_file")

args = parser.parse_args()

with open(args.topology_input_file) as topofile:
    import json

    regions = json.load(topofile)

device = "eth0"

common_cmds = []

common_cmds.append("set -ex")

# wait for docker network to start running and other nodes discoverable
common_cmds.append("sleep 3")

common_cmds.append("# IFB")
common_cmds.append("ip link set dev eth0 mtu 1500")
common_cmds.append("ip link add ifb0 type ifb")
common_cmds.append("ip link set dev ifb0 up")
common_cmds.append(
    "tc qdisc add dev {} handle ffff: ingress".format(
        device,
    )
)
common_cmds.append(
    "tc filter add dev {} parent ffff: u32 match ip src 0/0 action mirred egress redirect dev ifb0".format(
        device,
    )
)

common_cmds.append("tc qdisc add dev ifb0 root handle 1: htb".format(device))
common_cmds.append("")


region_node = defaultdict(list)  # list of node structs
region_latency = defaultdict(dict)  # map to latency value
total_nodes = sum([len(region["nodes"]) for region in regions])

common_cmds.append("# RESOLVE HOSTS")
# iterate to get all service names
for region in regions:
    region_name = region["name"]
    assert len(region["latencies_ms"]) == len(regions)

    for peer_idx, latency in enumerate(region["latencies_ms"]):
        peer_region_name = regions[peer_idx]["name"]

        if isinstance(latency, int):
            latency_profile = LatencyProfile(latency_ms=latency)
        elif isinstance(latency, dict):
            latency_profile = LatencyProfile(
                latency_ms=latency["latency"],
                jitter_ms=latency.get("jitter"),
                correlation=latency.get("correlation"),
                loss=latency.get("loss"),
            )

        region_latency[region_name][peer_region_name] = latency_profile

        # latency values should be symmetric now
        if region_name in region_latency[peer_region_name]:
            assert region_latency[peer_region_name][region_name] == latency_profile

    for node in region["nodes"]:
        region_node[region_name].append(node)
        common_cmds.append(
            f"{node['service']}=$(getent hosts {node['service']} | awk '{{ print $1 }}' )"
        )
common_cmds.append("")


def generate_single_node_tc(self_node, self_region):

    cmds = common_cmds[:]  # deepcopy common commands

    last_used_class_id = 1

    # inbound rules
    cmds.append("# INBOUND")

    cmds.append(
        f"tc class add dev ifb0 parent 1: classid 1:1 htb rate {node['down_Mbps']}mbit"
    )

    cmds.append(
        "tc filter add dev ifb0 protocol ip parent 1: prio 0 u32 match ip src 0/0 flowid 1:1"
    )

    peer_idx = 0
    for peer_region, peers in region_node.items():
        latency = region_latency[self_region][peer_region]
        latency_ms = latency.latency_ms
        jitter_ms = latency.jitter_ms
        correlation = latency.correlation
        loss = latency.loss

        for peer in peers:
            if peer == self_node:
                continue

            # every node has a fair share of bandwidth, no priorities
            # child links have 1kbit bandwidth, they borrow from parent htb
            # https://tldp.org/HOWTO/Traffic-Control-HOWTO/classful-qdiscs.html
            cmds.append(
                f"tc class add dev ifb0 parent 1:1 classid 1:{peer_idx + 2} htb rate {node['down_Mbps'] / (total_nodes-1):.2f}mbit ceil {node['down_Mbps']}mbit"
            )
            cmds.append(
                f"tc filter add dev ifb0 protocol ip parent 1:1 prio 1 u32 match ip src ${peer['service']} flowid 1:{peer_idx + 2}"
            )

            netem_qdisc = last_used_class_id + 1
            last_used_class_id += 1

            bdp_bytes = node["down_Mbps"] * 1_000_000 * latency_ms / 1000 / 8
            bdp_packets = bdp_bytes / 400  # 400 bytes packets are too small
            limit = int(bdp_packets * 1.5)

            qdisc_cmd = (
                f"tc qdisc add dev ifb0 parent 1:{peer_idx + 2} handle {netem_qdisc}:"
            )
            if jitter_ms == 0:
                qdisc_cmd += f" netem delay {latency_ms}ms limit {limit}"
            else:
                # the rate option prevents packet reordering, described here
                # https://lists.linuxfoundation.org/pipermail/netem/2018-May/001691.html
                # the solution from netem man page doesn't work on 5.15.0-97-generic
                # From https://man7.org/linux/man-pages/man8/tc-netem.8.html
                # > If you don't want this behavior then replace the internal queue
                # > discipline tfifo with a simple FIFO queue discipline.
                # > tc qdisc add dev eth0 root handle 1: netem delay 10ms 100ms
                # > tc qdisc add dev eth0 parent 1:1 pfifo limit 1000

                # qdisc_cmd += f" netem delay {latency_ms}ms {jitter_ms}ms {correlation * 100}% rate 25gbps limit {limit}"
                qdisc_cmd += f" netem delay {latency_ms}ms {jitter_ms}ms {correlation * 100}% limit {limit}"

            if loss != 0.0:
                qdisc_cmd += f" loss random {loss*100}%"

            cmds.append(qdisc_cmd)

            peer_idx += 1
    cmds.append("")

    # outbound rules
    cmds.append("# OUTBOUND")

    cmds.append(f"tc qdisc add dev {device} root handle 1: htb")
    cmds.append(
        f"tc class add dev {device} parent 1: classid 1:1 htb rate {node['up_Mbps']}mbit"
    )
    cmds.append(
        f"tc filter add dev {device} protocol ip parent 1: prio 0 u32 match ip dst 0/0 flowid 1:1"
    )
    return cmds


for region, nodes in region_node.items():
    for node in nodes:
        cmds = generate_single_node_tc(node, region)

        cwd = Path(args.topology_input_file).parents[0]
        tc_path = cwd / node["volume"] / "scripts" / "tc.sh"
        tc_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tc_path, "w+") as tc_file:
            for cmd in cmds:
                tc_file.write(f"{cmd}\n")
