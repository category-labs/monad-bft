#!/usr/local/bin/python3
from time import sleep
import sys
import random
import requests

from eth_account import Account
from web3 import Web3, HTTPProvider
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sender_private_keys = [
    "0x5c2339f6ac56b12fb5fd14af1d355ca3cad72f8549055f4a8523a8422c992c27",
    "0x92ceb2600fdf4c6f920e96f4caf76fe693260891ff8e8ac2aa25e6f434870838",
    "0xcd6c19a04d14c851b7efaca8b38e29f134936fb351e614848a4d86a00a50d1c8",
    "0x4b2e5266f9c93366539fa15bfb5c303c1f1a2dbbc29c08af4688fbf9f7f9db15",
]
NUM_SENDERS = len(sender_private_keys)

rpc_addrs = [
    "http://rpc0:8080",
    "http://rpc1:8080",
    "http://rpc2:8080",
    "http://rpc3:8080",
]

INITIAL_ACCOUNT_BAL = 200000000000000000000


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
    """
    Multinode multiple senders nonce test:
    Send valid transactions from different accounts and expect all of them to run to completion
    Send random transactions with used nonces which should be ignored
    """
    sender_accounts = [
        Account.from_key(sender_privkey) for sender_privkey in sender_private_keys
    ]

    w3_clients = []
    for rpc_addr in rpc_addrs:
        session = requests.Session()
        retry = Retry(connect=5, backoff_factor=2)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        w3 = Web3(HTTPProvider(rpc_addr, session=session))
        w3_clients.append(w3)

    expected_tx_hashes = []
    NUM_TXNS = 10

    print(f"Start sending {NUM_TXNS} valid transactions from each account")
    for node_i, w3 in enumerate(w3_clients):
        rpc_addr = rpc_addrs[node_i]
        sender_account = sender_accounts[node_i]

        for nonce in range(NUM_TXNS):
            receiver_account = Account.create()

            transfer_tx = make_transfer_tx(
                sender_account.address, receiver_account.address, nonce)
            signed_transfer = sender_account.sign_transaction(transfer_tx)
            tx_hash = w3.eth.send_raw_transaction(
                signed_transfer.rawTransaction)
            print(
                f"sent transaction to {rpc_addr}: from: {sender_account.address}, to: {receiver_account.address}, tx_hash: {tx_hash.hex()}, nonce: {nonce}")
            expected_tx_hashes.append(tx_hash)
    print("Finish sending valid transactions")

    # confirm transactions are executed at every node
    for node_i, w3 in enumerate(w3_clients):
        rpc_addr = rpc_addrs[node_i]
        for tx_hash in expected_tx_hashes:
            try:
                w3.eth.wait_for_transaction_receipt(tx_hash, timeout=5)
            except Exception as e:
                print(
                    f"{rpc_addr} wait_for_transaction_receipt exception {e}",
                    file=sys.stderr,
                )

    # send random transactions with duplicate nonces to nodes
    NUM_RANDOM_TXNS = 50
    print("Start sending random invalid transactions")
    for _ in range(NUM_RANDOM_TXNS):
        used_nonce = random.randint(0, NUM_TXNS - 1)
        random_sender_account = random.choice(sender_accounts)
        receiver_account = Account.create()
        transfer_tx = make_transfer_tx(
            random_sender_account.address, receiver_account.address, used_nonce)
        signed_transfer = random_sender_account.sign_transaction(transfer_tx)

        random_node_i = random.randint(0, 3)
        rpc_addr = rpc_addrs[random_node_i]
        w3 = w3_clients[random_node_i]

        tx_hash = w3.eth.send_raw_transaction(signed_transfer.rawTransaction)
        print(
            f"sent transaction to {rpc_addr}: from: {random_sender_account.address}, to: {receiver_account.address}, tx_hash: {tx_hash.hex()}, nonce: {used_nonce}",)
    print("Finish sending random invalid transactions")

    # sleep sufficiently long to let each become leader
    sleep(10)

    # get transaction count for each sender address from each node
    for rpc_i, w3 in enumerate(w3_clients):
        rpc_addr = rpc_addrs[node_i]

        for sender_account in sender_accounts:
            try:
                tx_count = w3.eth.get_transaction_count(sender_account.address)
                if tx_count != NUM_TXNS:
                    print(
                        f"expected transaction count for {sender_account.address} to be {NUM_TXNS}, got {tx_count} from {rpc_addr}",
                        file=sys.stderr,
                    )
            except Exception as e:
                print(
                    f"{rpc_addr} wait_for_transaction_receipt exception {e}",
                    file=sys.stderr,
                )

    print("test finished")
