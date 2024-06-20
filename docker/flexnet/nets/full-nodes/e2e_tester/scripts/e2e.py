#!/usr/local/bin/python3

from eth_account import Account
from web3 import Web3, HTTPProvider
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import sys

sender_privkey = "0x5c2339f6ac56b12fb5fd14af1d355ca3cad72f8549055f4a8523a8422c992c27"

rpc_addrs = [
    "http://rpc0:8080",
    "http://rpc1:8080",
    "http://rpc2:8080",
    "http://rpc3:8080",
]

INITIAL_ACCOUNT_BAL = 200000000000000000000

if __name__ == "__main__":
    sender_account = Account.from_key(sender_privkey)

    w3_clients = []
    for rpc_addr in rpc_addrs:
        session = requests.Session()
        retry = Retry(connect=5, backoff_factor=2)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        w3 = Web3(HTTPProvider(rpc_addr, session=session))
        w3_clients.append(w3)

    for i in range(5):
        # pick different rpc to service the transaction
        w3_sender = w3_clients[i % len(rpc_addrs)]

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
            "chainId": 1,
        }

        signed_transfer = sender_account.sign_transaction(transfer_tx)
        tx_hash = w3_sender.eth.send_raw_transaction(signed_transfer.rawTransaction)

        # confirm transaction is executed at every node and balance is updated
        for rpc_i, w3 in enumerate(w3_clients):
            addr = rpc_addrs[rpc_i]
            try:
                w3.eth.wait_for_transaction_receipt(tx_hash, timeout=3)
            except Exception as e:
                print(
                    f"{addr} wait_for_transaction_receipt exception {e}",
                    file=sys.stderr,
                )

            sender_tx_count = w3.eth.get_transaction_count(sender_account.address)
            if sender_tx_count != i + 1:
                print(
                    f"{addr} incorrect sender_tx_count expected {i + 1} actual {sender_tx_count}",
                    file=sys.stderr,
                )

            sender_balance = w3.eth.get_balance(sender_account.address)
            expected_sender_balance = INITIAL_ACCOUNT_BAL - (
                transfer_tx["gas"] * transfer_tx["maxFeePerGas"] + transfer_tx["value"]
            ) * (i + 1)
            if sender_balance != expected_sender_balance:
                print(
                    f"{addr} sender balance error expected {expected_sender_balance} actual {sender_balance}",
                    file=sys.stderr,
                )

            receiver_balance = w3.eth.get_balance(receiver_account.address)
            expected_receiver_balance = transfer_tx["value"]
            if receiver_balance != expected_receiver_balance:
                print(
                    f"{addr} receiver balance error expected {expected_receiver_balance} actual {receiver_balance}",
                    file=sys.stderr,
                )
    print("test finished")
