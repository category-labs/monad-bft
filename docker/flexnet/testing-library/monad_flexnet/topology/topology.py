import functools
import json
import os
import pathlib

from . import LatencyProfile, Node, Region

class Topology:
    def __init__(self, name: str):
        self.regions = {}
        self.name = name

    @classmethod
    def from_json(cls, topology_file: str | os.PathLike):
        with open(topology_file) as f:
            topo_dict = json.load(f)

        topo_name = pathlib.Path(topology_file).stem
        topo = cls(topo_name)
        for region_dict in topo_dict['regions']:
            region = topo.create_region(region_dict['name'], intraregion_latency=LatencyProfile(**region_dict['intraregion_latency']))
            for node_dict in region_dict['nodes']:
                region.create_node(
                    node_dict['name'],
                    has_execution=node_dict['execution'],
                    has_rpc=node_dict['rpc'],
                    upload_speed=node_dict['up_Mbps'],
                    download_speed=node_dict['down_Mbps']
                )

        for latency in topo_dict['latencies']:
            topo.set_interregion_latency(latency['region1'], latency['region2'], LatencyProfile(**latency['latency']))

        return topo

    def create_region(self, region_name: str, **kwargs):
        new_region = Region(region_name, **kwargs)
        self.regions[region_name] = new_region
        return new_region

    def set_interregion_latency(self, region_1: str, region_2: str, latency: LatencyProfile):
        self.regions[region_1].set_interregion_latency(region_2, latency)
        self.regions[region_2].set_interregion_latency(region_1, latency)

    def get_all_nodes(self) -> list[Node]:
        nodes = []
        for region in self.regions.values():
            nodes.extend(region.nodes)
        return nodes

    def get_total_node_count(self):
        return functools.reduce(lambda n, r: n + len(r.nodes), self.regions.values(), 0)

    def start_all_nodes(self, root_dir: str | os.PathLike, network_name: str, run_id: str):
        for region in self.regions.values():
            region.start_all_nodes(root_dir, network_name, run_id)

    def stop_all_nodes(self):
        for region in self.regions.values():
            region.stop_all_nodes()

    def find_node_by_name(self, node_name: str):
        for region in self.regions.values():
            for node in region.nodes:
                if node.name == node_name:
                    return node
        return None

    def print_containers(self):
        print(f'{self.name}:')
        for region in self.regions.values():
            print(f'\t{region.name}:')
            for node in region.nodes:
                node.print_containers('\t\t')
