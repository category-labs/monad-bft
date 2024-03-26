import rlp
from eth.vm.forks.shanghai import ShanghaiBlock
from pathlib import Path
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", type=str, help="data file path", required=True)

    args = parser.parse_args()

    data_path = args.data

    with open(data_path, "r") as f:
        txns_json = json.load(f)

    # list all transactions
    ledger_txns = []
    for block_file in Path("node0/ledger").iterdir():

        with open(block_file, "rb") as f:
            data = f.read()

        block = rlp.decode(data, ShanghaiBlock).as_dict()
        ledger_txns.extend(["0x" + txn.hash.hex() for txn in block["transactions"]])

    print(f"Total txns in ledger: {len(ledger_txns)}")

    submitted_txns = []
    for txn in txns_json:
        if not txn["submitted"]:
            print(f"ERROR: txns {txn["hash"]} not submitted")
            continue
        submitted_txns.append(txn["hash"])
        
    print(f"Total txns submitted: {len(submitted_txns)}")

    only_in_ledger = set(ledger_txns) - set(submitted_txns)
    only_in_submitted = set(submitted_txns) - set(ledger_txns)

    verification_success = True
    if len(only_in_ledger) > 0:
        print("Only found in ledger: ", only_in_ledger)
        verification_success = False

    if len(only_in_submitted) > 0:
        print("Submitted but not in ledger: ", only_in_submitted)
        verification_success = False

    if verification_success:
        print("Block inspection success")
    else:
        print("Block inspection failure")
        exit(1)
