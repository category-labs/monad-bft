from monad_flexnet import Flexnet, RpcConnection, Transaction

import random
import time

INITIAL_ACCOUNT_BAL = 200000000000000000000
TRANSACTION_AMOUNT = 1000
GAS_AMOUNT = 21000
MAX_FEE_PER_GAS = 1000

nodes_keys = [
    ('node0', '0x5c2339f6ac56b12fb5fd14af1d355ca3cad72f8549055f4a8523a8422c992c27'),
    ('node1', '0x92ceb2600fdf4c6f920e96f4caf76fe693260891ff8e8ac2aa25e6f434870838'),
    ('node2', '0xcd6c19a04d14c851b7efaca8b38e29f134936fb351e614848a4d86a00a50d1c8'),
    ('node3', '0x4b2e5266f9c93366539fa15bfb5c303c1f1a2dbbc29c08af4688fbf9f7f9db15')
]

def main():
    net_config = Flexnet('./nets/4nodes-full.json')

    with net_config.start_topology(gen_config=True) as net:
        clients_accounts = {}

        for node, key in nodes_keys:
            client = net.connect(node)
            account = client.create_account(key)
            clients_accounts[node] = (client, account)

        # Submit transactions from all accounts on different clients
        NUM_VALID_TX_PER_ACCT = 10
        valid_txns = []
        for client, send_account in clients_accounts.values():
            for i in range(NUM_VALID_TX_PER_ACCT):
                receiver = client.create_account()
                tx = client.create_transaction(Transaction(send_account, receiver, 1000), i)
                valid_txns.append(tx)

        time.sleep(10)

        # Verify all transactions are completed on all clients
        valid = True
        for client, _ in clients_accounts.values():
            for _, account in clients_accounts.values():
                count = client.get_transaction_count(account)
                if (count != NUM_VALID_TX_PER_ACCT):
                    print(f'{client}: {count} != {NUM_VALID_TX_PER_ACCT} (incorrect number of valid transactions)')
                    valid = False
        if valid:
            print('Valid transactions submitted successfully!')

        # Submit random invalid transactions
        NUM_INVALID_TX_PER_ACCT = 50
        for _ in range(NUM_INVALID_TX_PER_ACCT):
            used_nonce = random.randint(0, NUM_VALID_TX_PER_ACCT - 1)
            random_client, _ = random.choice(list(clients_accounts.values()))
            _, random_account = random.choice(list(clients_accounts.values()))
            receiver = random_client.create_account()
            random_client.create_transaction(Transaction(random_account, receiver, 1000), used_nonce)

        time.sleep(10)

        # Verify only the valid transactions make it into the ledger
        valid = True
        for client, _ in clients_accounts.values():
            for _, account in clients_accounts.values():
                count = client.get_transaction_count(account)
                if (count != NUM_VALID_TX_PER_ACCT):
                    print(f'{client}: {count} != {NUM_VALID_TX_PER_ACCT} (invalid transactions accepted)')
                    valid = False
        if valid:
            print('Invalid transactions rejected!')

if __name__ == "__main__":
    main()