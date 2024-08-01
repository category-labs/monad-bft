from monad_flexnet import Flexnet, RpcConnection, Transaction

import time

sender_privkey = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

INITIAL_ACCOUNT_BAL = 10000000000000000000000
TRANSACTION_AMOUNT = 1000
GAS_AMOUNT = 21000
MAX_FEE_PER_GAS = 1000

rpc_nodes = [
    'node0',
    'node1',
    'node2',
    'node3'
]

def main():
    net_config = Flexnet('./nets/4nodes-full.json')

    with net_config.start_topology(gen_config=True) as net:
        sender_account = None
        clients = {}

        for rpc_node in rpc_nodes:
            client = net.connect(rpc_node)
            clients[rpc_node] = client
            if sender_account is None:
                sender_account = client.create_account(sender_privkey)
        NUM_VALID_TX = 5
        for i in range(NUM_VALID_TX):
            # Pick different RPC to service the transaction
            send_client: RpcConnection = clients[rpc_nodes[i % len(rpc_nodes)]]
            receiver_account = send_client.create_account()
            print(f'receiver account address {receiver_account.address}')
            tx_hash = send_client.create_and_wait_for_transaction(Transaction(sender_account, receiver_account, TRANSACTION_AMOUNT, GAS_AMOUNT))

            print(f'sender balance {client.get_account_balance(sender_account)}')
            print(f'receiver balance {client.get_account_balance(receiver_account)}')

            for client_name, client in clients.items():
                client.wait_for_transaction_receipt(tx_hash)
                sender_tx_count = client.get_transaction_count(sender_account)
                if sender_tx_count != i + 1:
                    print(f'{client_name}: incorrect sender_tx_count ({sender_tx_count} != {i + 1})')
                client.expect_account_balance(sender_account, INITIAL_ACCOUNT_BAL - (GAS_AMOUNT * MAX_FEE_PER_GAS + TRANSACTION_AMOUNT) * (i + 1))

        # Send 10 transactions with invalid nonces, and verify they are not included
        for i in range(20, 30):
            send_client: RpcConnection = clients[rpc_nodes[i % len(rpc_nodes)]]
            receiver_account = send_client.create_account()
            print(f'receiver account address {receiver_account.address}')
            tx_hash = send_client.create_transaction(Transaction(sender_account, receiver_account, TRANSACTION_AMOUNT, GAS_AMOUNT), nonce=i)

            time.sleep(2)
            print(f'sender balance {client.get_account_balance(sender_account)}')
            print(f'receiver balance {client.get_account_balance(receiver_account)}')

            for other_client_name, other_client in clients.items():
                sender_tx_count = other_client.get_transaction_count(sender_account)
                if sender_tx_count != NUM_VALID_TX:
                    print(f'{other_client_name}: incorrect sender_tx_count ({sender_tx_count} != {NUM_VALID_TX})')
                other_client.expect_account_balance(sender_account, INITIAL_ACCOUNT_BAL - (GAS_AMOUNT * MAX_FEE_PER_GAS + TRANSACTION_AMOUNT) * NUM_VALID_TX) 

        # Send 10 transactions with duplicate nonces, and verify they are not included
        for i in range(10):
            send_client: RpcConnection = clients[rpc_nodes[i % len(rpc_nodes)]]
            receiver_account = send_client.create_account()
            print(f'receiver account address {receiver_account.address}')
            tx_hash = send_client.create_transaction(Transaction(sender_account, receiver_account, TRANSACTION_AMOUNT, GAS_AMOUNT), nonce=NUM_VALID_TX-1)

            time.sleep(2)
            print(f'sender balance {client.get_account_balance(sender_account)}')
            print(f'receiver balance {client.get_account_balance(receiver_account)}')

            for other_client_name, other_client in clients.items():
                sender_tx_count = other_client.get_transaction_count(sender_account)
                if sender_tx_count != NUM_VALID_TX:
                    print(f'{other_client_name}: incorrect sender_tx_count ({sender_tx_count} != {NUM_VALID_TX})')
                other_client.expect_account_balance(sender_account, INITIAL_ACCOUNT_BAL - (GAS_AMOUNT * MAX_FEE_PER_GAS + TRANSACTION_AMOUNT) * NUM_VALID_TX)

if __name__ == "__main__":
    main()