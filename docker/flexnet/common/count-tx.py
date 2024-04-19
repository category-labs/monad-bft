import rlp
from eth.vm.forks.shanghai import ShanghaiBlock
from pathlib import Path
import argparse
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--min", type=int, help="Minimum of txns expected")

    args = parser.parse_args()
    min_txns = args.min

    total_txns = 0
    for block_file in Path("node0/ledger").iterdir():
        with open(block_file, "rb") as f:
            data = f.read()

        block = rlp.decode(data, ShanghaiBlock).as_dict()
        total_txns += len(block["transactions"])

    verification_success = True

    if total_txns < min_txns:
        print(f"Txns in ledger: {total_txns} < Expected {min_txns}")
        verification_success = False

    if verification_success:
        print(f"Txns count success: {total_txns} txns in ledger")
    else:
        sys.exit(1)
