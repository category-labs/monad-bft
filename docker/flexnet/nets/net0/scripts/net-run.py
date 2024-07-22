from monad_flexnet import Flexnet, Transaction

import time

def main():
    net_config = Flexnet('./nets/net0/topology.json')
    with net_config.start_topology(gen_config=True) as net:
        client = net.connect('node0')

        TX_AMOUNT = 1000
        GAS_AMOUNT = 21000

        for i in range(5):
            print(f'Creating account {i}')
            receiver = client.create_account()
            sender = client.create_account()
            print(f'Creating transaction {i}')
            client.create_transaction(Transaction(sender, receiver, TX_AMOUNT, GAS_AMOUNT))

    ledger = net_config.get_block_ledger('node0')
    assert len(ledger.get_transactions()) == 5
    assert net_config.check_block_ledger_equivalence()
    print('Verification successful!')

if __name__ == "__main__":
    main()
