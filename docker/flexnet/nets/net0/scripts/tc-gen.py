# generate the tc script for each node in the net
#!/usr/bin/python3
import argparse
from collections import defaultdict
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("topology_input_file")

args = parser.parse_args()

with open(args.topology_input_file) as topofile:
    import json

    regions = json.load(topofile)

device = "eth0"

common_cmds = []

common_cmds.append("set -x")
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
        region_latency[region_name][peer_region_name] = latency
        # latency values should be symmetric now
        if region_name in region_latency[peer_region_name]:
            assert region_latency[peer_region_name][region_name] == latency

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

        for peer in peers:
            if peer == self_node:
                continue

            # major number: 10
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

            bdp_bytes = node["down_Mbps"] * 1_000_000 * latency / 1000 / 8
            bdp_packets = bdp_bytes / 400
            cmds.append(
                f"tc qdisc add dev ifb0 parent 1:{peer_idx + 2} handle {netem_qdisc}: netem delay {latency}ms limit {int(bdp_packets * 1.5)}"
            )

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

        cwd = Path.cwd()
        tc_path = cwd / node["volume"] / "scripts" / "tc.sh"
        with open(tc_path, "w+") as tc_file:
            for cmd in cmds:
                tc_file.write(f"{cmd}\n")
