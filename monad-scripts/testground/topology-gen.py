import argparse

parser = argparse.ArgumentParser()
parser.add_argument("topology_output_file")
parser.add_argument("node_counts")
args = parser.parse_args()

node_counts = list(map(int, args.node_counts.split(",")))
node_counts = [
    ("us", node_counts[0]),
    ("europe", node_counts[1]),
    ("me", node_counts[2]),
    ("asia", node_counts[3]),
    ("aus", node_counts[4]),
]

# latency: (delay, variance)
#     us         eu         me        asia       aus
latencies = [
    [(20, 10), (100, 20), (150, 20), (200, 25), (250, 25)],  # us
    [(100, 20), (20, 10), (100, 20), (150, 20), (200, 25)],  # eu
    [(150, 20), (100, 20), (20, 10), (100, 20), (150, 20)],  # me
    [(200, 25), (150, 20), (100, 20), (20, 10), (100, 20)],  # asia
    [(250, 25), (200, 25), (150, 20), (100, 20), (20, 10)],  # aus
]

bandwidth = 1000
loss_percent = 0

topology = []
for region_idx, (region_name, region_node_count) in enumerate(node_counts):
    topology.append(
        {
            "name": region_name,
            "latencies_ms": latencies[region_idx],
            "nodes": [
                {"up_Mbps": bandwidth, "down_Mbps": bandwidth, "loss": loss_percent},
            ]
            * region_node_count,
        }
    )

with open(args.topology_output_file, "w") as f:
    import json

    json.dump(topology, f)
