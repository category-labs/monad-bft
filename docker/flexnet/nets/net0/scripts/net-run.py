from monad_flexnet import Flexnet, Transaction

import time

def main():
    net = Flexnet('./nets/net0/topology.json')
    net.create_topology()
    net.run()
    net.connect('node0')

    TX_AMOUNT = 1000
    GAS_AMOUNT = 21000

    for i in range(5):
        print(f'Creating account {i}')
        receiver = net.create_account()
        sender = net.create_account()
        print(f'Creating transaction {i}')
        net.create_transaction(Transaction(sender, receiver, TX_AMOUNT, GAS_AMOUNT))

    time.sleep(5)

    net.stop()

    ledger = net.get_block_ledger('node0')
    assert len(ledger.get_transactions()) == 5
    assert net.check_block_ledger_equivalence()
    print('Verification successful!')

if __name__ == "__main__":
    main()
