import rlp
from eth.vm.forks.shanghai import ShanghaiBlock


if __name__ == "__main__":
    # Inspect block example
    with open("node0/ledger/1", "rb") as f:
        data = f.read()

    # strip the block hash
    data = data[32:]

    block = rlp.decode(data, ShanghaiBlock).as_dict()
    print(block)
