#!/usr/bin/python3

device = "lo"
regions = [
    {
        "name": "us",
        "latencies_ms": [10, 50, 100],
        "nodes": [
            {
                "up_Mbps": 100,
                "down_Mbps": 100,
            },
        ] * 20,
    },
    {
        "name": "europe",
        "latencies_ms": [50, 10, 50],
        "nodes": [
            {
                "up_Mbps": 100,
                "down_Mbps": 100,
            },
        ] * 20,
    },
    {
        "name": "asia",
        "latencies_ms": [100, 50, 10],
        "nodes": [
            {
                "up_Mbps": 100,
                "down_Mbps": 100,
            },
        ] * 20,
    },
]

commands = []
node_ips = []

commands.append("set -x")
commands.append("# INBOUND")
commands.append("ip link set dev lo mtu 1500")
commands.append("ip link add ifb0 type ifb")
commands.append("ip link set dev ifb0 up")
commands.append(
    "tc qdisc add dev {} handle ffff: ingress".format(
        device,
    )
)
commands.append(
    "tc filter add dev {} parent ffff: u32 match ip src 0/0 action mirred egress redirect dev ifb0".format(
        device,
    )
)

commands.append("tc qdisc add dev ifb0 root handle 1: htb".format(device))

last_used_class_id = 1
def next_class_id():
    global last_used_class_id
    last_used_class_id += 1
    return last_used_class_id

node_idx = 0
for region_idx, region in enumerate(regions):
    for node in region["nodes"]:
        commands.append("")
        commands.append("# -> TO NODE: {}".format(node_idx))

        # outbound
        htb_qdisc = next_class_id()
        commands.append(
            "tc class add dev ifb0 parent 1: classid 1:{} htb rate {}mbit".format(
                htb_qdisc,
                node["down_Mbps"],
            )
        )
        node_ip = "127.{}.0.{}".format(100 + region_idx, node_idx + 1)
        commands.append(
            "tc filter add dev ifb0 protocol ip parent 1: prio 1 u32 match ip dst {}/32 flowid 1:{}".format(
                node_ip,
                htb_qdisc,
            )
        )

        num_bands = 1 + len(regions)
        prio_qdisc = next_class_id()
        commands.append("tc qdisc add dev ifb0 parent 1:{} handle {}: prio bands {} priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0".format(htb_qdisc, prio_qdisc, num_bands))
        commands.append(
            "tc filter add dev ifb0 protocol ip parent {}: prio 2 u32 match ip src 0/0 flowid {}:1".format(
                prio_qdisc,
                prio_qdisc,
            )
        )

        for r_idx, r_delay in enumerate(region["latencies_ms"]):
            region_subnet = "127.{}.0.0".format(100 + r_idx)
            commands.append(
                "tc filter add dev ifb0 protocol ip parent {}: prio 1 u32 match ip src {}/16 flowid {}:{}".format(
                    prio_qdisc,
                    region_subnet,
                    prio_qdisc,
                    r_idx + 2
                )
            )

            netem_qdisc = next_class_id()
            bdp_bytes = node["up_Mbps"] * 1_000_000 * r_delay / 1000
            bdp_packets = bdp_bytes / 400
            commands.append(
                "tc qdisc add dev ifb0 parent {}:{} handle {}: netem delay {}ms limit {}".format(
                    prio_qdisc,
                    r_idx + 2,
                    netem_qdisc,
                    r_delay,
                    int(bdp_packets * 1.5)
                )
            )

        node_idx += 1

commands.append("")
commands.append("")
commands.append("")
commands.append("# OUTBOUND")
commands.append("tc qdisc add dev {} root handle 1: htb".format(device))

last_used_class_id = 1

node_idx = 0
for region_idx, region in enumerate(regions):
    commands.append("")
    commands.append("")
    commands.append("# REGION: {}".format(region["name"]))
    for node in region["nodes"]:
        commands.append("")
        commands.append("# -> FROM_NODE: {}".format(node_idx))

        # outbound
        htb_qdisc = next_class_id()
        commands.append(
            "tc class add dev {} parent 1: classid 1:{} htb rate {}mbit".format(
                device,
                htb_qdisc,
                node["up_Mbps"],
            )
        )
        node_ip = "127.{}.0.{}".format(100 + region_idx, node_idx + 1)
        commands.append(
            "tc filter add dev {} protocol ip parent 1: prio 1 u32 match ip src {}/32 flowid 1:{}".format(
                device,
                node_ip,
                htb_qdisc,
            )
        )
        node_ips.append(node_ip + ":5000")
        node_idx += 1


commands.append("")
commands.append("")
commands.append("MONAD_MEMPOOL_RNDUDS=true monad-testground -o http://jaeger:4317 --addresses {}".format(" ".join(node_ips)))

for command in commands:
    print(command)
