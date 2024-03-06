# generate the ethtx.sh for each node in the net
#!/usr/bin/python3
import argparse
from pathlib import Path

ETHTX_TEMPLATE = """
#!/bin/bash

set -e

mkdir -p /monad/logs

# wait for node service to create IPC socket
while [[ ! -S "/monad/mempool.sock" ]]; do
    sleep 1
done

ethtx --num-tx {num_tx} \\
    ipc --ipc-path /monad/mempool.sock --tps {tps} \\
    > /monad/logs/ethtx.log 2>&1
"""


parser = argparse.ArgumentParser()
parser.add_argument("topology_input_file")
parser.add_argument("--tps", type=int, help="ethtx tool tps")
parser.add_argument("--num-tx", type=int, help="ethtx tool total txns to send")

args = parser.parse_args()
tps = args.tps
num_tx = args.num_tx

ethtx_text = ETHTX_TEMPLATE.format(num_tx=num_tx, tps=tps)

with open(args.topology_input_file) as topofile:
    import json

    regions = json.load(topofile)

volumes = []
for region in regions:
    for node in region["nodes"]:
        volume = node["volume"]
        cwd = Path(args.topology_input_file).parents[0]
        ethtx_path = cwd / volume / "scripts" / "ethtx.sh"
        ethtx_path.parent.mkdir(parents=True, exist_ok=True)
        with open(ethtx_path, "w+") as ethtx_file:
            ethtx_file.write(ethtx_text)
