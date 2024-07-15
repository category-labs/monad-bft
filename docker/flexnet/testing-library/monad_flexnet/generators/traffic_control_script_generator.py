import collections
import functools
import json
import os
import pathlib

from monad_flexnet.topology import LatencyProfile, Node, Topology


class TrafficControlScriptGenerator:
    @staticmethod
    def _get_common_commands(device: str = 'eth0', ifb_name: str = 'ifb0') -> list[str]:
        return[
            'set -ex',
            'sleep 3',
            f'ip link set dev {device} mtu 1500',
            f'ip link add {ifb_name} type ifb',
            f'ip link set dev {ifb_name} up',
            f'tc qdisc add dev {device} handle ffff: ingress',
            f'tc filter add dev {device} parent ffff: u32 match ip src 0/0 action mirred egress redirect dev {ifb_name}',
            f'tc qdisc add dev {ifb_name} root handle 1: htb'
        ]

    @staticmethod
    def _get_node_specific_commands(node: Node, own_region: str, node_count: int, regions: dict, region_latencies: dict, device: str = 'eth0', ifb_name: str = 'ifb0'):
        commands = []
        last_used_class_id = 1
        commands.extend([
            f'tc class add dev {ifb_name} parent 1: classid 1:1 htb rate {node.download_speed}mbit',
            f'tc filter add dev {ifb_name} protocol ip parent 1: prio 0 u32 match ip src 0/0 flowid 1:1'
        ])

        peer_idx = 0
        for peer_region, peers in regions.items():
            latency: LatencyProfile = region_latencies[own_region][peer_region]
            for peer in peers:
                if peer == node:
                    continue

                # Every node has a fair share of bandwidth, no priorities
                # Child links have 1kbit bandwidth, they borrow from parent htb
                # https://tldp.org/HOWTO/Traffic-Control-HOWTO/classful-qdiscs.html
                commands.extend([
                    f'tc class add dev {ifb_name} parent 1:1 classid 1:{peer_idx + 2} htb rate {node.download_speed / (node_count - 1):.2f}mbit ceil {node.download_speed}mbit',
                    f'tc filter add dev {ifb_name} protocol ip parent 1:1 prio 1 u32 match ip src ${peer.name} flowid 1:{peer_idx + 2}'
                ])

                netem_qdisc = last_used_class_id + 1
                last_used_class_id += 1

                bdp_bytes = node.download_speed * 1000000 * latency.ms / 1000 / 8
                bdp_packets = bdp_bytes / 400
                limit = int(bdp_packets * 1.5)

                qdisc_cmd = f'tc qdisc add dev ifb0 parent 1:{peer_idx + 2} handle {netem_qdisc}:'
                if latency.jitter == 0:
                    qdisc_cmd += f' netem delay {latency.ms}ms limit {limit}'
                else:
                    # the rate option prevents packet reordering, described here
                    # https://lists.linuxfoundation.org/pipermail/netem/2018-May/001691.html
                    # the solution from netem man page doesn't work on 5.15.0-97-generic
                    # From https://man7.org/linux/man-pages/man8/tc-netem.8.html
                    # > If you don't want this behavior then replace the internal queue
                    # > discipline tfifo with a simple FIFO queue discipline.
                    # > tc qdisc add dev eth0 root handle 1: netem delay 10ms 100ms
                    # > tc qdisc add dev eth0 parent 1:1 pfifo limit 1000
                    qdisc_cmd += f' netem delay {latency.ms}ms {latency.jitter}ms {latency.correlation * 100}% limit {limit}'

                if latency.loss != 0.0:
                    qdisc_cmd += f' loss random {latency.loss * 100}%'

                commands.append(qdisc_cmd)
                peer_idx += 1

        commands.extend([
            f'tc qdisc add dev {device} root handle 1: htb',
            f'tc class add dev {device} parent 1: classid 1:1 htb rate {node.upload_speed}mbit',
            f'tc filter add dev {device} protocol ip parent 1: prio 0 u32 match ip dst 0/0 flowid 1:1'
        ])

        return commands

    @staticmethod
    def generate_scripts(topology: Topology, output_dir: str | os.PathLike, run_id: str = None):
        cmds = TrafficControlScriptGenerator._get_common_commands()

        region_node = collections.defaultdict(list)
        region_latency = collections.defaultdict(dict)
        node_count = topology.get_total_node_count()

        for _, region in topology.regions.items():
            region_latency[region.name][region.name] = region.intraregion_latency
            for peer_region_name, latency in region.interregion_latencies.items():
                region_latency[region.name][peer_region_name] = latency

                # Latency values should be symmetric now
                if region.name in region_latency[peer_region_name]:
                    assert region_latency[peer_region_name][region.name] == latency

            id_append = f'-{run_id}' if run_id is not None else ''
            for node in region.nodes:
                region_node[region.name].append(node)
                cmds.append(
                    f'{node.name}=$(getent hosts {node.name + id_append} | awk \'{{ print $1 }}\' )'
                )

        for region, nodes in region_node.items():
            for node in nodes:
                node_cmds = TrafficControlScriptGenerator._get_node_specific_commands(node, region, node_count, region_node, region_latency)
                out_path = pathlib.Path(output_dir) / node.name / 'scripts' / 'tc.sh'
                out_path.parent.mkdir(parents=True, exist_ok=True)
                with open(out_path, 'w+') as f:
                    all_cmds = cmds + node_cmds
                    f.writelines([cmd + '\n' for cmd in all_cmds])
