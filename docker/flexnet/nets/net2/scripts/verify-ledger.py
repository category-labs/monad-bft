import argparse
from pathlib import Path
import os
import sys
import filecmp
import json

"""
Diff the ledgers
Args: 
    ledger_dirs: [pathlib.Path]
Return value:
    common prefix length if all ledgers agree, otherwise print error and exit early
"""


def diff_ledger(ledger_dirs):
    ledger_lens = [len([*path.iterdir()]) for path in ledger_dirs]

    pivot_pos = ledger_lens.index(min(ledger_lens))
    pivot_path = ledger_dirs[pivot_pos]
    blocks = [block_path.name for block_path in pivot_path.iterdir()]

    # assert all block numbers are present
    for i in range(1, len(blocks)):
        assert str(i) in blocks

    mismatch_set = set()
    for path in ledger_dirs:
        _, mismatch, error = filecmp.cmpfiles(path, pivot_path, blocks, shallow=False)
        mismatch_set.update(mismatch)
        # file is always present in the pivot dir
        assert len(error) == 0

    if len(mismatch_set) != 0:
        print(f"ledger mismatch {mismatch_set}")
        exit(1)

    return len(blocks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="verify-ledger", description="Verify nodes committing the same ledger"
    )

    parser.add_argument("-c", "--count", type=int, help="Node count", required=True)
    parser.add_argument(
        "-l", "--ledger", type=str, help="Ledger directory name", required=True
    )
    parser.add_argument(
        "-n",
        "--blocks",
        type=int,
        help="Minimum number of blocks",
        default=0,
    )

    args = parser.parse_args()
    node_count = args.count
    ledger_dir = args.ledger
    ledger_min_length = args.blocks

    # parse the volume paths
    topology_path = Path(os.getcwd()) / "topology.json"
    if not topology_path.exists():
        print(f"topology file {topology_path} doesn't exist in cwd")
        sys.exit(1)

    with open(topology_path, "r") as f:
        topology_json = json.load(f)

    volume_paths = []
    for region in topology_json:
        for node in region["nodes"]:
            volume = node["volume"]
            volume_paths.append(Path(os.getcwd()) / volume)

    ledger_paths = [vol_path / ledger_dir for vol_path in volume_paths]

    common_prefix_len = diff_ledger(ledger_paths)

    if common_prefix_len < ledger_min_length:
        print(
            f"common ledger prefix length {common_prefix_len} < expected {ledger_min_length}"
        )
        exit(1)

    print(f"ledger verify success. Common prefix length {common_prefix_len}")
