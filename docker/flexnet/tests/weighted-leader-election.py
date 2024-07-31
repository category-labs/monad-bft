from monad_flexnet import Flexnet

import time

from monad_flexnet.topology import Topology
from monad_flexnet.topology.node import Node


def compute_mapping_from_epoch_validators(wal):
    leader_data = wal['leader']
    epoch_validator_mapping = {}

    for round, block_info in leader_data.items():
        author, epoch = block_info['author'], block_info['epoch']

        if epoch in epoch_validator_mapping:
            epoch_validator_mapping[epoch].add(author)
        else:
            epoch_validator_mapping[epoch] = {author}

    return epoch_validator_mapping


def main():
    net_config = Flexnet('./nets/weighted-leader-election/topology.json')
    net_config.set_test_mode(True)

    net: Flexnet
    with net_config.start_topology() as net:
        time.sleep(60 * 3)

    assert net_config.check_block_ledger_equivalence()

    for node in net_config.topology.get_all_nodes():
        node: Node
        wal_json = net_config.get_write_ahead_log_as_json(node.name)

        epoch_validator_mapping = compute_mapping_from_epoch_validators(wal_json)

        from pprint import pprint
        pprint(epoch_validator_mapping)

    print('test passed!')


if __name__ == '__main__':
    main()
