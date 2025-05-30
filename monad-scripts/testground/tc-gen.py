#!/usr/bin/python3

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("topology_input_file")
parser.add_argument("tc_output_file")
parser.add_argument("addresses_output_file")
args = parser.parse_args()

with open(args.topology_input_file) as topofile:
    import json

    regions = json.load(topofile)

device = "lo"

commands = []
node_ips = []

commands.append("set -x")
commands.append("# INBOUND")
commands.append("ip link set dev lo mtu 1500")
commands.append("tc qdisc add dev {} handle ffff: ingress".format(device))

last_used_class_id = 1


def next_class_id():
    global last_used_class_id
    last_used_class_id += 1
    return last_used_class_id


node_idx = 0
for region_idx, region in enumerate(regions):
    for node in region["nodes"]:
        node_ip = "127.{}.0.{}".format(100 + region_idx, node_idx + 1)

        commands.append("")
        commands.append("# -> TO NODE: {}".format(node_idx))

        # create ifb for this node
        commands.append("ip link add ifb{} type ifb".format(node_idx))
        commands.append("ip link set dev ifb{} up".format(node_idx))

        # route packets for this node to its ifb
        commands.append(
            "tc filter add dev {} parent ffff: u32 match ip dst {}/32 action mirred egress redirect dev ifb{}".format(
                device, node_ip, node_idx
            )
        )
        # create a htb root to its ifb
        root_class_id = next_class_id()
        commands.append(
            "tc qdisc add dev ifb{} root handle {}: htb".format(node_idx, root_class_id)
        )

        # ifb outbound (node inbound)
        htb_qdisc = next_class_id()
        # download bandwidth
        # create class for this download rate limit
        down_mbit = node["down_Mbps"]
        commands.append(
            "tc class add dev ifb{} parent {}: classid {}:{} htb rate {}mbit".format(
                node_idx, root_class_id, root_class_id, htb_qdisc, down_mbit
            )
        )

        # # filter this nodes packets from root in ifb to its class
        commands.append(
            "tc filter add dev ifb{} protocol ip parent {}: prio 1 u32 match ip dst {}/32 flowid {}:{}".format(
                node_idx, root_class_id, node_ip, root_class_id, htb_qdisc
            )
        )

        num_bands = 1 + len(regions)
        prio_qdisc = next_class_id()
        # add prio qdisc to rate limit class with bands for each region
        commands.append(
            "tc qdisc add dev ifb{} parent {}:{} handle {}: prio bands {} priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0".format(
                node_idx, root_class_id, htb_qdisc, prio_qdisc, num_bands
            )
        )
        # band 0 has minor number 1
        commands.append(
            "tc filter add dev ifb{} protocol ip parent {}: prio 2 u32 match ip src 0/0 flowid {}:1".format(
                node_idx, prio_qdisc, prio_qdisc
            )
        )

        for r_idx, (r_delay_mean, r_delay_var) in enumerate(region["latencies_ms"]):
            region_subnet = "127.{}.0.0".format(100 + r_idx)  # /16
            # route packets from other regions first (prio 1) to respective bands
            commands.append(
                "tc filter add dev ifb{} protocol ip parent {}: prio 1 u32 match ip src {}/16 flowid {}:{}".format(
                    node_idx, prio_qdisc, region_subnet, prio_qdisc, r_idx + 2
                )
            )

            netem_qdisc = next_class_id()
            bdp_bytes = node["up_Mbps"] * 1_000_000 * r_delay_mean / 1000 / 8
            min_packet_size = 400
            bdp_packets = bdp_bytes / min_packet_size
            max_queued_packets = int(bdp_packets * 10)

            loss_percent = node["loss"]
            # add netem delay to band
            commands.append(
                "tc qdisc add dev ifb{} parent {}:{} handle {}: netem delay {}ms {}ms distribution normal limit {} loss {}%".format(
                    node_idx,
                    prio_qdisc,
                    r_idx + 2,
                    netem_qdisc,
                    r_delay_mean,
                    r_delay_var,
                    max_queued_packets,
                    loss_percent,
                )
            )

        node_idx += 1
        node_ips.append(node_ip + ":8000")

with open(args.tc_output_file, "w") as tcfile:
    for command in commands:
        tcfile.write("{}\n".format(command))

with open(args.addresses_output_file, "w") as addressesfile:
    addressesfile.write(" ".join(node_ips))
