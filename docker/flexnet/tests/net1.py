from monad_flexnet import Flexnet, Transaction

import time

def main():
    net_config = Flexnet('./nets/4nodes-consensus-rpc.json')
    net_config.set_test_mode(True)
    byzantine_node = net_config.topology.find_node_by_name('node1')
    byzantine_node.set_byzantine()

    with net_config.start_topology(mock_drivers=True) as net:
        beneficiary = byzantine_node.get_beneficiary()
        clients = [net.connect(node.name) for node in net_config.topology.get_all_nodes()]
        sender_account = clients[0].create_account()

        # Submit a bunch of random transactions
        for i in range(100):
            send_client = clients[i % len(clients)]
            receiver_account = send_client.create_account()
            send_client.create_transaction(Transaction(sender_account, receiver_account, 1000))

        # Give time for the transactions to go through and (hopefully) not include any Byzantine transactions
        time.sleep(15)

    # Verify that none of the Byzantine transactions went through
    ledger = net_config.get_block_ledger('node0')
    byzantine_blocks = ledger.get_blocks(lambda b: b['header']['coinbase'] == beneficiary and len(b['transactions']) > 0)
    byzantine_transactions = 0
    for block in byzantine_blocks:
        byzantine_transactions += len(block['transactions'])

    print(f'Byzantine block count: {len(byzantine_blocks)}')
    print(f'Byzantine transaction count: {byzantine_transactions}')

    net_config.check_block_ledger_equivalence()

if __name__ == '__main__':
    main()