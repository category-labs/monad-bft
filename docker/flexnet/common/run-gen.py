# generate the run.sh for each node in the net
#!/usr/bin/python3
import argparse
from pathlib import Path

RUN_SH_TEXT = """
#!/bin/bash

set -e

bash /monad/scripts/tc.sh

mkdir -p /monad/logs

monad-node \\
    --secp-identity /monad/config/id-secp \\
    --bls-identity /monad/config/id-bls \\
    --node-config /monad/config/node.toml \\
    --forkpoint-config /monad/config/forkpoint.toml \\
    --wal-path /monad/wal \\
    --blockdb-path /monad/blockdb \\
    --mempool-ipc-path /monad/mempool.sock \\
    --triedb-path /monad/triedb \\
    --control-panel-ipc-path /monad/controlpanel.sock \\
    --execution-ledger-path /monad/ledger > /monad/logs/client.log 2>&1
"""


parser = argparse.ArgumentParser()
parser.add_argument("topology_input_file")

args = parser.parse_args()

with open(args.topology_input_file) as topofile:
    import json

    regions = json.load(topofile)

volumes = []
for region in regions:
    for node in region["nodes"]:
        volume = node["volume"]
        cwd = Path(args.topology_input_file).parents[0]
        run_path = cwd / volume / "scripts" / "run.sh"
        run_path.parent.mkdir(parents=True, exist_ok=True)
        # don't overwrite existing file
        if run_path.exists():
            continue
        with open(run_path, "w+") as runsh_file:
            runsh_file.write(RUN_SH_TEXT)
