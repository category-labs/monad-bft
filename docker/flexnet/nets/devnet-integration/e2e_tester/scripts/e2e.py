#!/usr/local/bin/python3

import argparse
from eth_account import Account
from web3 import Web3, HTTPProvider
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import sys

sender_privkey = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

INITIAL_ACCOUNT_BAL = 10000000000000000000000

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="e2e", description="End-to-end transfer test")
    parser.add_argument("--rpc", type=str, help="rpc server address", required=True)
    parser.add_argument("--data", type=str, help="data file path", required=False)

    args = parser.parse_args()
    rpc_addr = args.rpc
    data_path = args.data

    sender_account = Account.from_key(sender_privkey)

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    w3 = Web3(HTTPProvider(rpc_addr, session=session))
    txns_json = []
    for i in range(5):
        receiver_account = Account.create()
        print(f"receiver account address {receiver_account.address}")
        transfer_tx = {
            "from": sender_account.address,
            "to": receiver_account.address,
            "value": 1000,
            "nonce": i,
            "gas": 21000,
            "maxFeePerGas": 1000,  # hardcoded in the eth block header as 1000
            "maxPriorityFeePerGas": 0,
            "chainId": 41454,
        }

        signed_transfer = sender_account.sign_transaction(transfer_tx)
        tx_hash = w3.eth.send_raw_transaction(signed_transfer.rawTransaction)
        txns_json.append({"hash": tx_hash.hex(), "submitted": True})
        try:
            w3.eth.wait_for_transaction_receipt(tx_hash, timeout=5)
        except Exception as e:
            print("wait_for_transaction_receipt exception ", e, file=sys.stderr)

        sender_tx_count = w3.eth.get_transaction_count(sender_account.address)
        if sender_tx_count != i + 1:
            print(
                f"incorrect sender_tx_count expected {i + 1} actual {sender_tx_count}",
                file=sys.stderr,
            )

        sender_balance = w3.eth.get_balance(sender_account.address)
        expected_sender_balance = INITIAL_ACCOUNT_BAL - (
            transfer_tx["gas"] * transfer_tx["maxFeePerGas"] + transfer_tx["value"]
        ) * (i + 1)
        if sender_balance != expected_sender_balance:
            print(
                f"sender balance error expected {expected_sender_balance} actual {sender_balance}",
                file=sys.stderr,
            )

        receiver_balance = w3.eth.get_balance(receiver_account.address)
        expected_receiver_balance = transfer_tx["value"]
        if receiver_balance != expected_receiver_balance:
            print(
                f"receiver balance error expected {expected_receiver_balance} actual {receiver_balance}",
                file=sys.stderr,
            )

    if data_path is not None:
        with open(data_path, "w") as f:
            json.dump(txns_json, f)
