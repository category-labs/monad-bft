from monad_flexnet import Flexnet, Transaction

import time

def main():
    net_config = Flexnet('./nets/1node-full.json')

    with net_config.start_topology(gen_config=False) as net:
        client = net.connect('node')

        sender_privkey = '0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80'
        INITIAL_ACCOUNT_BAL = 10000000000000000000000
        TRANSACTION_AMOUNT = 1000
        GAS_AMOUNT = 21000
        MAX_FEE_PER_GAS = 1000
        TRANSACTION_COUNT = 5

        sender_account = client.create_account(sender_privkey)
        print(f'sender account address {sender_account.address}')

        for i in range(TRANSACTION_COUNT):
            receiver_account = client.create_account()
            client.create_and_wait_for_transaction(Transaction(sender_account, receiver_account, TRANSACTION_AMOUNT, GAS_AMOUNT))

            print(f'sender balance = {client.get_account_balance(sender_account)}')
            print(f'receiver balance = {client.get_account_balance(receiver_account)}')
            client.expect_account_balance(sender_account, INITIAL_ACCOUNT_BAL - (GAS_AMOUNT * MAX_FEE_PER_GAS + TRANSACTION_AMOUNT) * (i + 1))
            client.expect_account_balance(receiver_account, TRANSACTION_AMOUNT)

    ledger = net_config.get_block_ledger('node')
    print(f'num transactions = {len(ledger.get_transactions())}')
    assert len(ledger.get_transactions()) == 5

if __name__ == "__main__":
    main()
