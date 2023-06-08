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
# regions = [
#     {
#         "name": "asia",
#         "latencies_ms": [50],
#         "nodes": [
#             {
#                 "up_Mbps": 100,
#                 "down_Mbps": 100,
#             },
#             {
#                 "up_Mbps": 100,
#                 "down_Mbps": 100,
#             },
#         ],
#     },
# ]

commands = []
node_ips = []

commands.append("set -x")
commands.append("# INBOUND")
commands.append(
    "tc qdisc add dev {} handle ffff: ingress".format(
        device,
    )
)

commands.append("")
commands.append("")
commands.append("")
commands.append("# OUTBOUND")
commands.append("tc qdisc add dev {} root handle 1: htb".format(device))

last_used_class_id = 1
def next_class_id():
    global last_used_class_id
    last_used_class_id += 1
    return last_used_class_id

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
        node_ips.append(node_ip)

        drr_qdisc = next_class_id()
        commands.append(
            "tc qdisc add dev {} parent 1:{} handle {}: drr".format(
                device, htb_qdisc, drr_qdisc
            )
        )

        # TODO fix this
        # # inbound
        # commands.append(
        #     "tc filter add dev {} parent ffff: u32 match ip dst {}/32 police rate {}mbit burst {} conform-exceed drop".format(
        #         device,
        #         node_ip,
        #         node["down_Mbps"],
        #         100_000, # 100 KB
        #     )
        # )

        for r_idx, r_delay in enumerate(region["latencies_ms"]):
            commands.append("")
            commands.append("# ---> TO: {}".format(regions[r_idx]["name"]))
            commands.append(
                "tc class add dev {} parent {}: classid {}:{} drr".format(
                    device,
                    drr_qdisc,
                    drr_qdisc,
                    r_idx + 1,
                )
            )
            commands.append(
                "tc filter add dev {} protocol ip parent {}: prio 1 u32 match ip dst 127.{}.0.0/16 flowid {}:{}".format(
                    device,
                    drr_qdisc,
                    100 + r_idx,
                    drr_qdisc,
                    r_idx + 1,
                )
            )

            netem_qdisc = next_class_id()
            bdp_bytes = node["up_Mbps"] * 1_000_000 * r_delay / 1000
            bdp_packets = bdp_bytes / 400
            commands.append(
                "tc qdisc add dev {} parent {}:{} handle {}: netem delay {}ms limit {}".format(
                    device,
                    drr_qdisc,
                    r_idx + 1,
                    netem_qdisc,
                    r_delay,
                    int(bdp_packets * 1.5)
                )
            )

        node_idx += 1


commands.append("")
commands.append("")
commands.append("monad-node --addresses {}".format(" ".join(node_ips)))

for command in commands:
    print(command)
