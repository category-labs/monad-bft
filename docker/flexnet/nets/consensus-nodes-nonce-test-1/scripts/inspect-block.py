import rlp
from eth.vm.forks.shanghai import ShanghaiBlock
from pathlib import Path
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--data", type=str,
                        help="data file path", required=True)

    args = parser.parse_args()

    data_path = args.data

    with open(data_path, "r") as f:
        txns_json = json.load(f)

    submitted_txns = []
    expected_txns = []
    for txn in txns_json:
        if not txn["submitted"]:
            print(f"ERROR: txns {txn["hash"]} not submitted")
            continue
        submitted_txns.append(txn["hash"])

        if txn["expected"]:
            expected_txns.append(txn["hash"])

    ledger_paths = ["/monad/node0/ledger", "/monad/node1/ledger"]

    exitcode = 0
    for node_i, ledger_path in enumerate(ledger_paths):
        # list all transactions
        ledger_txns = []
        for block_file in Path(ledger_path).iterdir():

            with open(block_file, "rb") as f:
                data = f.read()

            block = rlp.decode(data, ShanghaiBlock).as_dict()
            ledger_txns.extend(["0x" + txn.hash.hex()
                                for txn in block["transactions"]])

        print(f"Total txns in node{node_i} ledger: {len(ledger_txns)}")
        print(f"Total txns expected: {len(expected_txns)}")

        only_in_ledger = set(ledger_txns) - set(expected_txns)
        only_in_expected = set(expected_txns) - set(ledger_txns)

        verification_success = True
        if len(only_in_ledger) > 0:
            print("Only found in ledger: ", only_in_ledger)
            verification_success = False

        if len(only_in_expected) > 0:
            print("Expected but not in ledger: ", only_in_expected)
            verification_success = False

        if verification_success:
            print(f"Block inspection success for node{node_i}")
        else:
            print(f"Block inspection failure for node{node_i}")
            exitcode = 1

    exit(exitcode)
