import eth_account
import itertools
import json
import os
import pathlib
import requests
import shutil
import sys
import threading
import time
import urllib3
from web3 import Web3

from hexbytes import HexBytes
from python_on_whales import docker

from .ledger import BlockLedger, Ledger
from .transaction import Transaction
from .topology import Topology
from . import generators

class Flexnet:
    def __init__(self, topology_path: str | os.PathLike):
        self.topology_path = pathlib.Path(topology_path)
        self.topology = None
        self.transactions = []
        self.root_dir = None
        self.run_id = None
    
    def create_topology(self):
        # Create output directory
        self.run_id = os.urandom(8).hex()
        self.run_name = f'{self.topology_path.stem}-{time.strftime("%Y%m%d_%H%M%S")}-{self.run_id}'
        self.root_dir = pathlib.Path(f'{os.getcwd()}/logs/{self.run_name}')
        shutil.copytree(self.topology_path.parent, self.root_dir)

        self._create_empty_triedb(self.root_dir / 'node'  / 'triedb' / 'test.db')

        self.topology = Topology.from_json(self.topology_path)

        # Build the containers that will be needed
        docker.build(
            '.',
            file='./images/dev/Dockerfile',
            tags='monad-python-dev'
        )
        docker.build(
            '../..',
            file='./images/monad-bft-builder/Dockerfile',
            tags='monad-bft-builder:latest'
        )
        docker.build(
            '../..',
            file='./images/image0/Dockerfile',
            tags='image0:latest'
        )
        docker.build(
            '../..',
            file='./images/rpc/Dockerfile',
            tags='flexnet-rpc:latest'
        )

        # Generate scripts for each node
        # Config script has to be generated from in a Docker container, since it relies on swig and system packages
        docker.run(
            image='monad-python-dev:latest',
            remove=True,
            volumes=[('.', '/monad')],
            command=['bash', '-c', f'cd /monad/logs/{self.root_dir.name} && python3 /monad/common/config-gen.py -c {self.topology.get_total_node_count()} -s \'\' -i {self.run_id}']
        )
        generators.TrafficControlScriptGenerator.generate_scripts(self.topology, self.root_dir, self.run_id)
        generators.RunScriptGenerator.generate_scripts(self.topology, self.root_dir)

        # Create a bridge network
        network = docker.network.create(self.run_name)

    def run(self, runtime: int | None = None, blocking: bool = False):
        ''' Run the topology for a certain amount of time. runtime=None will run indefinitely until stop() is called manually '''
        self.topology.start_all_nodes(self.root_dir, self.run_name, self.run_id)
        self.topology.print_containers()
        # Give a few seconds for nodes to fully start
        time.sleep(10)

        def wait_and_stop():
            if runtime is not None:
                time.sleep(runtime)
                self.stop()
        if blocking:
            wait_and_stop()
        else:
            threading.Thread(target=wait_and_stop).start()

    def stop(self):
        self.topology.stop_all_nodes()
        net = docker.network.inspect(self.run_name)
        net.remove()

    def connect(self, rpc_node_name: str):
        rpc_node = self.topology.find_node_by_name(rpc_node_name)
        rpc_port = rpc_node.get_rpc_port()

        adapter = requests.adapters.HTTPAdapter(max_retries=urllib3.util.retry.Retry(connect=3, backoff_factor=1))
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.w3 = Web3(Web3.HTTPProvider(f'http://localhost:{rpc_port}', session=session))

    def create_account(self, key: str | None = None) -> eth_account.Account:
        if key is not None:
            return eth_account.Account.from_key(key)
        return eth_account.Account.create()

    def create_transaction(self, transaction: Transaction) -> HexBytes:
        transfer_tx = {
            'from': transaction.sender.address,
            'to': transaction.recipient.address,
            'value': transaction.amount,
            'nonce': self.w3.eth.get_transaction_count(transaction.sender.address),
            'gas': transaction.gas,
            'maxFeePerGas': transaction.maxFeePerGas,
            'maxPriorityFeePerGas': 0,
            'chainId': 1
        }

        signed_transfer = transaction.sender.sign_transaction(transfer_tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed_transfer.raw_transaction)
        self.transactions.append({'hash': tx_hash.hex(), 'submitted': True})
        return tx_hash

    def create_and_wait_for_transaction(self, transaction: Transaction):
        tx_hash = self.create_transaction(transaction)
        self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=5)

    def get_account_balance(self, account: eth_account.Account) -> int:
        return self.w3.eth.get_balance(account.address)

    def expect_account_balance(self, account: eth_account.Account, balance: int):
        print(self.get_account_balance(account))
        assert(self.get_account_balance(account) == balance)

    def check_block_ledger_equivalence(self) -> bool:
        nodes = self.topology.get_all_nodes()
        ledger1 = self.get_block_ledger(nodes[0].name)
        for i in range(1, len(nodes)):
            ledger2 = self.get_block_ledger(nodes[i].name)
            if ledger1 != ledger2:
                return False
        return True

    def dump_transaction_data(self, out_file: str | os.PathLike):
        with open(out_file, 'w') as  f:
            json.dump(self.transactions, f)

    def get_block_ledger(self, node_name: str) -> Ledger:
        return BlockLedger(f'{self.root_dir}/{node_name}/ledger')

    @staticmethod
    def _create_empty_triedb(path: str | os.PathLike):
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            # 4 GiB
            f.truncate(4 * 1024 * 1024 * 1024)
