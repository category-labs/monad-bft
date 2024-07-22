import os

from . import LatencyProfile, Node

class Region:
    def __init__(self, name: str, intraregion_latency: LatencyProfile = LatencyProfile(10)):
        self.name = name
        self.nodes = []
        self.intraregion_latency = intraregion_latency
        self.interregion_latencies = {}

    def create_node(self, node_name: str, **kwargs):
        new_node = Node(node_name, **kwargs)
        self.nodes.append(new_node)
        return new_node

    def set_interregion_latency(self, region: str, latency: LatencyProfile):
        self.interregion_latencies[region] = latency

    def start_all_nodes(self, root_dir: str | os.PathLike, network_name: str, run_id: str):
        for node in self.nodes:
            node.start(root_dir, network_name, run_id)

    def stop_all_nodes(self):
        for node in self.nodes:
            node.stop()

    def for_all_nodes(self, func):
        for node in self.nodes:
            func(node)
