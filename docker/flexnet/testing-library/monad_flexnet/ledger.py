import eth
import filecmp
import json
import os
import pathlib
import rlp

class Ledger:
    def __init__(self, transactions: set[str]):
        self.transactions = transactions 

    @classmethod
    def from_json(cls, json_path: str | os.PathLike):
        with open(json_path, 'r') as f:
            transactions_raw = json.load(f)
        transactions = set(map(lambda t: t['hash'] if t['submitted'] else None, transactions_raw))
        return cls(transactions)
    
    def get_transactions(self) -> set[str]:
        return self.transactions

    def __eq__(self, other):
        if isinstance(other, Ledger):
            return self.transactions == other.transactions
        return NotImplemented

class BlockLedger(Ledger):
    def __init__(self, blockpath: str | os.PathLike):
        self.blockpath = blockpath

        transactions = set()
        path = pathlib.Path(blockpath)
        for block_file in path.iterdir():
            with open(block_file, 'rb') as f:
                data = f.read()

            block = rlp.decode(data, eth.vm.forks.shanghai.ShanghaiBlock).as_dict()
            transactions |= set(['0x' + txn.hash.hex() for txn in block['transactions']])
        self.block_count = len([*path.iterdir()])
        self.transactions = transactions

    def __eq__(self, other):
        if isinstance(other, BlockLedger):
            if abs(self.block_count - other.block_count) > 20:
                return False
            iterpath = self.blockpath if self.block_count <= other.block_count else other.blockpath
            for f in pathlib.Path(iterpath).iterdir():
                if not filecmp.cmp(f, pathlib.Path(other.blockpath) / f.name):
                    return False
            return True
        else:
            return Ledger.__eq__(self, other)
