import json
import os
import pathlib

from monad_flexnet.topology import Topology

class RunScriptGenerator:
    script_text = '''
    #!/bin/bash

    set -e

    bash /monad/scripts/tc.sh

    mkdir -p /monad/logs

    monad-node \\
        --secp-identity /monad/config/id-secp \\
        --bls-identity /monad/config/id-bls \\
        --node-config /monad/config/node.toml \\
        --genesis-path /monad/config/genesis.json \\
        --statesync-ipc-path /monad/statesync.sock \\
        --forkpoint-config /monad/config/forkpoint.toml \\
        --wal-path /monad/wal \\
        --blockdb-path /monad/blockdb \\
        --mempool-ipc-path /monad/mempool.sock \\
        --triedb-path /monad/triedb \\
        --control-panel-ipc-path /monad/controlpanel.sock \\
        --execution-ledger-path /monad/ledger > /monad/logs/client.log 2>&1
    '''

    @staticmethod
    def generate_scripts(topology: Topology, out_dir: str | os.PathLike):
        for _, region in topology.regions.items():
            for node in region.nodes:
                out_file_path = pathlib.Path(out_dir) / node.name / 'scripts' / 'run.sh'
                out_file_path.parent.mkdir(parents=True, exist_ok=True)
                # Don't overwrite existing file
                if out_file_path.exists():
                    continue
                with open(out_file_path, 'w+') as f:
                    f.write(RunScriptGenerator.script_text)
