import eth_account

from dataclasses import dataclass

@dataclass
class Transaction:
    sender: eth_account.Account
    recipient: eth_account.Account
    amount: int
    gas: int = 21000
    maxFeePerGas: int = 1000
