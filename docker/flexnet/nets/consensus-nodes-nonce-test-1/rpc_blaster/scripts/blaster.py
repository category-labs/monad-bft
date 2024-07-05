#!/usr/bin/python3
import argparse
import json
from time import sleep
import random
import requests

from eth_account import Account
from web3 import Web3, HTTPProvider
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def make_transfer_tx(sender_address, receiver_address, nonce):
    return {
        "from": sender_address,
        "to": receiver_address,
        "value": 1000,
        "nonce": nonce,
        "gas": 21000,
        "maxFeePerGas": 1000,  # hardcoded in the eth block header as 1000
        "maxPriorityFeePerGas": 0,
        "chainId": 1,
    }


if __name__ == "__main__":
    '''
    Valid nonce test
    Send 10 transactions with valid nonces to a single node
    '''
    parser = argparse.ArgumentParser(
        prog="rpc blaster", description="Send transactions to the rpc server"
    )

    parser.add_argument("--rpc", type=str,
                        help="rpc server address", required=True)
    parser.add_argument("--data", type=str,
                        help="data file path", required=True)

    args = parser.parse_args()

    rpc_addr = args.rpc
    data_path = args.data

    sender_privkey = "0x5c2339f6ac56b12fb5fd14af1d355ca3cad72f8549055f4a8523a8422c992c27"
    sender_account = Account.from_key(sender_privkey)

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    w3 = Web3(HTTPProvider(rpc_addr, session=session))

    txns = []
    NUM_TXNS = 10
    # generate NUM_TXNS transactions with sequential nonces and send to rpc server
    for nonce in range(NUM_TXNS):
        receiver_account = Account.create()

        transfer_tx = make_transfer_tx(
            sender_account.address, receiver_account.address, nonce)
        signed_transfer = sender_account.sign_transaction(transfer_tx)
        resp_hash = w3.eth.send_raw_transaction(signed_transfer.rawTransaction)

        if resp_hash == signed_transfer.hash:
            print(f"submitted tx {signed_transfer.hash.hex()}")

            txn = {
                "transaction": signed_transfer.rawTransaction.hex(),
                "hash": signed_transfer.hash.hex(),
                "submitted": True,
                "expected": True,
                "nonce": nonce,
            }
            txns.append(txn)
        else:
            print(
                f"expected txn hash: {signed_transfer.hash.hex()}, got {resp_hash.hex()}")

    # generate 20 transactions that aren't sequential and send to rpc server
    for nonce in range(20, 40):
        receiver_account = Account.create()

        transfer_tx = make_transfer_tx(
            sender_account.address, receiver_account.address, nonce)
        signed_transfer = sender_account.sign_transaction(transfer_tx)
        resp_hash = w3.eth.send_raw_transaction(signed_transfer.rawTransaction)

        if resp_hash == signed_transfer.hash:
            print(f"submitted tx {signed_transfer.hash.hex()}")

            txn = {
                "transaction": signed_transfer.rawTransaction.hex(),
                "hash": signed_transfer.hash.hex(),
                "submitted": True,
                "expected": False,
                "nonce": nonce,
            }
            txns.append(txn)
        else:
            print(
                f"expected txn hash: {signed_transfer.hash.hex()}, got {resp_hash.hex()}")

    with open(data_path, "w") as f:
        json.dump(txns, f)
