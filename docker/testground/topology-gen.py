topology = [
    {
        "name": "us",
        "latencies_ms": [10, 50, 100],
        "nodes": [
            {
                "up_Mbps": 100,
                "down_Mbps": 100,
            },
        ] * 5,
    },
    {
        "name": "europe",
        "latencies_ms": [50, 10, 50],
        "nodes": [
            {
                "up_Mbps": 100,
                "down_Mbps": 100,
            },
        ] * 5,
    },
    {
        "name": "asia",
        "latencies_ms": [100, 50, 10],
        "nodes": [
            {
                "up_Mbps": 100,
                "down_Mbps": 100,
            },
        ] * 5,
    },
]

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("topology_output_file")
args = parser.parse_args()

with open(args.topology_output_file, 'w') as f:
    import json
    json.dump(topology, f)
