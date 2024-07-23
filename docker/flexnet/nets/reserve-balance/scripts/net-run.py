from monad_flexnet import Flexnet, RpcConnection, Transaction

import time

sender_privkey = "0x5c2339f6ac56b12fb5fd14af1d355ca3cad72f8549055f4a8523a8422c992c27"

INITIAL_ACCOUNT_BAL = 200000000000000000000
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
    net_config = Flexnet('./nets/reserve-balance/topology.json')

    with net_config.start_topology(gen_config=True) as net:
        sender_account = None
        clients = {}

        for rpc_node in rpc_nodes:
            client = net.connect(rpc_node)
            clients[rpc_node] = client
            if sender_account is None:
                sender_account = client.create_account(sender_privkey)

        for i in range(5):
            # Pick different RPC to service the transaction
            send_client: RpcConnection = clients[rpc_nodes[i % len(rpc_nodes)]]
            receiver_account = send_client.create_account()
            print(f'receiver account address {receiver_account.address}')
            tx_hash = send_client.create_and_wait_for_transaction(Transaction(sender_account, receiver_account, TRANSACTION_AMOUNT, GAS_AMOUNT))

            print(f'sender balance {client.get_account_balance(sender_account)}')
            print(f'receiver balance {client.get_account_balance(receiver_account)}')

            for client_name, client in clients.items():
                try:
                    client.wait_for_transaction_receipt(tx_hash)
                except Exception as e:
                    print(f'{client_name}: wait_for_transaction_receipt exception {e}')
                sender_tx_count = client.get_transaction_count(sender_account)
                if sender_tx_count != i + 1:
                    print(f'{client_name}: incorrect sender_tx_count ({sender_tx_count} != {i + 1})')
                client.expect_account_balance(sender_account, INITIAL_ACCOUNT_BAL - (GAS_AMOUNT * MAX_FEE_PER_GAS + TRANSACTION_AMOUNT) * (i + 1))

if __name__ == "__main__":
    main()
