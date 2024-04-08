import rlp
from eth.vm.forks.shanghai import ShanghaiBlock
from pathlib import Path
import argparse
import json
import tomllib

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", type=str, help="data file path", required=True)
    parser.add_argument(
        "--byzantine", type=str, help="byzantine proposer volume", required=True
    )
    parser.add_argument("--delay", type=int, help="state root delay", required=True)

    args = parser.parse_args()

    data_path = args.data
    byzantine_volume_path = Path(args.byzantine)
    state_root_delay = args.delay

    with open(data_path, "r") as f:
        txns_json = json.load(f)

    with open(byzantine_volume_path / "config" / "node.toml", "rb") as f:
        data = tomllib.load(f)
        byzantine_beneficiary = bytearray.fromhex(data["beneficiary"][2:])

    # list all transactions
    ledger_txns = []
    byzantine_blocks = []
    for block_file in Path("node0/ledger").iterdir():
        with open(block_file, "rb") as f:
            data = f.read()

        block = rlp.decode(data, ShanghaiBlock).as_dict()
        ledger_txns.extend(["0x" + txn.hash.hex() for txn in block["transactions"]])

        # validators should not vote on non-empty blocks with an invalid state root
        # the state root hash check is skipped for empty blocks so that's allowed
        if (
            block["header"]["coinbase"] == byzantine_beneficiary
            and len(block["transactions"]) > 0
            and int(block_file.name) > state_root_delay
        ):
            byzantine_blocks.append(block_file.name)

    print(f"Total txns in ledger: {len(ledger_txns)}")

    verification_success = True

    if len(ledger_txns) == 0:
        print("No transactions found in ledger")
        verification_success = False

    if len(byzantine_blocks) > 0:
        byzantine_blocks = sorted([int(block_num) for block_num in byzantine_blocks])
        print("Byzantine blocks: ", byzantine_blocks)
        verification_success = False

    if verification_success:
        print("Block inspection success")
    else:
        print("Block inspection failure")
        exit(1)
