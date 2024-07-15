import json
import os
import pathlib

from monad_flexnet.topology import Topology

class EthTxScriptGenerator:
    script_template = """
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

    @staticmethod
    def generate_scripts(topology: Topology, out_dir: str | os.PathLike, tps: int, num_tx: int):
        for _, region in topology.regions.items():
            for node in region.nodes:
                out_file_path = pathlib.Path(out_dir) / node.name / 'scripts' / 'ethtx.sh'
                out_file_path.parent.mkdir(parents=True, exist_ok=True)
                if out_file_path.exists():
                    continue
                with open(out_file_path, 'w+') as f:
                    f.write(EthTxScriptGenerator.script_template.format(num_tx=num_tx, tps=tps))
