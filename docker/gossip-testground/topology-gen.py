topology = [
    {
        "name": "us",
        "latencies_ms": [10, 100],
        "nodes": [
            {
                "up_Mbps": 1000,
                "down_Mbps": 1000,
            },
        ] * 50,
    },
    {
        "name": "eu",
        "latencies_ms": [100, 10],
        "nodes": [
            {
                "up_Mbps": 1000,
                "down_Mbps": 1000,
            },
        ] * 50,
    },
]

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("topology_output_file")
args = parser.parse_args()

with open(args.topology_output_file, 'w') as f:
    import json
    json.dump(topology, f)
