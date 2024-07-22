import collections
import eth_account
import json
import os
import requests
import sys
import urllib3

from hexbytes import HexBytes
from web3 import Web3

from .transaction import Transaction

class RpcConnection:
    def __init__(self, rpc_server: str, internal_tx_count: bool = False):
        self.rpc_server = rpc_server
        self.transactions = []
        self.transaction_count = collections.defaultdict(lambda: 0)
        self.internal_tx_count = internal_tx_count

        adapter = requests.adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(connect=3, backoff_factor=1))
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.w3 = Web3(Web3.HTTPProvider(rpc_server, session=session))

    def create_account(self, key: str | None = None) -> eth_account.Account:
        if key is not None:
            return eth_account.Account.from_key(key)
        return eth_account.Account.create()

    def create_transaction(self, transaction: Transaction) -> HexBytes:
        sender_tx_count = self.get_transaction_count(transaction.sender) # self.transaction_count[transaction.sender.address]
        self.transaction_count[transaction.sender.address] = sender_tx_count + 1
        transfer_tx = {
            'from': transaction.sender.address,
            'to': transaction.recipient.address,
            'value': transaction.amount,
            'nonce': sender_tx_count,
            'gas': transaction.gas,
            'maxFeePerGas': transaction.maxFeePerGas,
            'maxPriorityFeePerGas': 0,
            'chainId': 1
        }

        signed_transfer = transaction.sender.sign_transaction(transfer_tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_transfer.raw_transaction)
        self.transactions.append({'hash': tx_hash.hex(), 'submitted': True})
        return tx_hash

    def create_and_wait_for_transaction(self, transaction: Transaction) -> HexBytes:
        tx_hash = self.create_transaction(transaction)
        self.wait_for_transaction_receipt(tx_hash)
        return tx_hash

    def wait_for_transaction_receipt(self, tx_hash, timeout=5):
        self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout)

    def get_account_balance(self, account: eth_account.Account) -> int:
        return self.w3.eth.get_balance(account.address)

    def expect_account_balance(self, account: eth_account.Account, balance: int):
        assert(self.get_account_balance(account) == balance)

    def get_transaction_count(self, account: eth_account.Account) -> int:
        if self.internal_tx_count:
            return self.transaction_count[account.address]
        return self.w3.eth.get_transaction_count(account.address)

    def dump_transaction_data(self, out_file: str | os.PathLike):
        with open(out_file, 'w') as  f:
            json.dump(self.transactions, f)
