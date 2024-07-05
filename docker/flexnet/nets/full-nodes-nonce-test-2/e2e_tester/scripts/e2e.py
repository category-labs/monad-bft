#!/usr/local/bin/python3
from time import sleep
import sys
import requests

from eth_account import Account
from web3 import Web3, HTTPProvider
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sender_privkey = "0x5c2339f6ac56b12fb5fd14af1d355ca3cad72f8549055f4a8523a8422c992c27"

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
    '''
    Duplicate nonce test
    Send 10 transactions with valid nonces to a single node and expect them to run to
    completion.
    Send random invalid transactions with same nonces to each node and expect none of them
    to be included in a block and run.
    '''
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

    # pick different rpc to service the transaction
    w3_sender_0 = w3_clients[0]

    expected_tx_hashes = []
    NUM_TXNS = 10
    print("Sending 10 valid transactions to node 0")
    for nonce in range(NUM_TXNS):
        receiver_account = Account.create()

        transfer_tx = make_transfer_tx(
            sender_account.address, receiver_account.address, nonce)
        signed_transfer = sender_account.sign_transaction(transfer_tx)
        tx_hash = w3_sender_0.eth.send_raw_transaction(
            signed_transfer.rawTransaction
        )
        print(
            f"sent transaction to node 0: from: {sender_account.address}, to: {receiver_account.address}, tx_hash: {tx_hash.hex()}, nonce: {nonce}")
        expected_tx_hashes.append(tx_hash)
    print("Finish sending valid transactions")

    # confirm transactions are executed at every node
    for node_i, w3 in enumerate(w3_clients):
        rpc_addr = rpc_addrs[node_i]
        for tx_hash in expected_tx_hashes:
            try:
                w3.eth.wait_for_transaction_receipt(tx_hash, timeout=3)
            except Exception as e:
                print(
                    f"{rpc_addr} wait_for_transaction_receipt exception {e}",
                    file=sys.stderr,
                )

    # send transactions with duplicate nonces to each node
    print("Start sending invalid transactions with duplicate nonces")
    for node_i, w3 in enumerate(w3_clients):
        rpc_addr = rpc_addrs[node_i]
        for nonce in range(NUM_TXNS):
            receiver_account = Account.create()

            transfer_tx = make_transfer_tx(
                sender_account.address, receiver_account.address, nonce)
            signed_transfer = sender_account.sign_transaction(transfer_tx)
            tx_hash = w3.eth.send_raw_transaction(
                signed_transfer.rawTransaction
            )
            print(
                f"sent transaction to {rpc_addr}: from: {sender_account.address}, to: {receiver_account.address}, tx_hash: {tx_hash.hex()}, nonce: {nonce}")
    print("Finish sending invalid transactions with duplicate nonces")

    # sleep sufficiently long to let each node become leader
    sleep(10)

    # get transaction count for sender address from each node
    for node_i, w3 in enumerate(w3_clients):
        rpc_addr = rpc_addrs[node_i]

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
