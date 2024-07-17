from monad_flexnet import Flexnet, Transaction

import time
import sys

faucet_privkey = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

INITIAL_ACCOUNT_BAL = 1000000000000000000000
TRANSACTION_AMOUNT = 1000
GAS_AMOUNT = 21000
MAX_FEE_PER_GAS = 1000

rpc_nodes = ["node0", "node1", "node2", "node3"]


def main():
    net_config = Flexnet("./nets/4nodes-full.json")

    with net_config.start_topology(gen_config=True) as net:
        faucet_account = None
        clients = {}

        for rpc_node in rpc_nodes:
            client = net.connect(rpc_node)
            clients[rpc_node] = client
            if faucet_account is None:
                faucet_account = client.create_account(faucet_privkey)

        print(f"faucet address {faucet_account.address}")
        print("Test starting: sufficient")
        client = clients[rpc_nodes[0]]
        # (21000 + 32000) is current intrinsic gas amount constant
        # min amount to make trasfer (21000 + 32000) * MAX_FEE_PER_GAS + value
        sufficient_account = client.create_account()
        print(f"sufficient account address {sufficient_account.address}")
        min_amount = (21000 + 32000) * MAX_FEE_PER_GAS + TRANSACTION_AMOUNT
        client.create_and_wait_for_transaction(
            Transaction(faucet_account, sufficient_account, min_amount, GAS_AMOUNT)
        )

        faucet_balance = client.get_account_balance(faucet_account)
        receiver_balance = client.get_account_balance(sufficient_account)

        print(f"faucet balance {faucet_balance}")
        print(f"receiver balance {receiver_balance}")

        assert (
            faucet_balance
            == INITIAL_ACCOUNT_BAL - min_amount - GAS_AMOUNT * MAX_FEE_PER_GAS
        )
        assert receiver_balance == min_amount

        print("fund transfer complete")
        # wait for execution gap so sufficient_account is created from consensus
        # view
        time.sleep(5)

        sink_account = client.create_account()
        try:
            client.create_and_wait_for_transaction(
                Transaction(
                    sufficient_account, sink_account, TRANSACTION_AMOUNT, GAS_AMOUNT
                )
            )
            sender_balance = client.get_account_balance(sufficient_account)
            sink_balance = client.get_account_balance(sink_account)

            print(f"sender balance {sender_balance}")
            print(f"sink balance {sink_balance}")

            assert (
                sender_balance
                == min_amount - TRANSACTION_AMOUNT - GAS_AMOUNT * MAX_FEE_PER_GAS
            )
            assert sink_balance == TRANSACTION_AMOUNT
        except Exception as e:
            print("Transaction with sufficient gas balance should go through")
            sys.exit(1)

        print("sufficient test PASSED\n")

        print("Test starting: insufficient")
        # insufficient test
        faucet_balance_before = client.get_account_balance(faucet_account)

        insufficient_account = client.create_account()
        print(f"insufficient account address {insufficient_account.address}")

        under_min_amount = (21000 + 32000) * MAX_FEE_PER_GAS - 1

        client.create_and_wait_for_transaction(
            Transaction(
                faucet_account, insufficient_account, under_min_amount, GAS_AMOUNT
            )
        )

        faucet_balance = client.get_account_balance(faucet_account)
        receiver_balance = client.get_account_balance(insufficient_account)

        print(f"faucet balance {faucet_balance}")
        print(f"receiver balance {receiver_balance}")

        assert (
            faucet_balance
            == faucet_balance_before - under_min_amount - GAS_AMOUNT * MAX_FEE_PER_GAS
        )
        assert receiver_balance == under_min_amount

        print("fund transfer complete")
        # wait for execution gap so sufficient_account is created from consensus
        # view
        time.sleep(5)

        sink_account = client.create_account()

        try:
            client.create_and_wait_for_transaction(
                Transaction(
                    insufficient_account, sink_account, TRANSACTION_AMOUNT, GAS_AMOUNT
                )
            )
            print("Transaction with insufficient gas balance should NOT go through")
            sys.exit(1)
        except Exception as e:
            sender_balance = client.get_account_balance(insufficient_account)
            sink_balance = client.get_account_balance(sink_account)

            print(f"sender balance {sender_balance}")
            print(f"sink balance {sink_balance}")

            assert sender_balance == under_min_amount
            assert sink_balance == 0

        print("insufficient test PASSED")


if __name__ == "__main__":
    main()
