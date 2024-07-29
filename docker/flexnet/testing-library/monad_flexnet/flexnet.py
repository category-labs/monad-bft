from typing import Dict

import git
import itertools
import json
import os
import pathlib
import shutil
import threading
import time

import python_on_whales
from python_on_whales import docker

from contextlib import contextmanager

from .ledger import BlockLedger, Ledger
from .rpcconnection import RpcConnection
from .topology import Topology, Node
from . import generators


class Flexnet:
    def __init__(self, topology_path: str | os.PathLike):
        self.topology_path = pathlib.Path(topology_path)
        self.topology = Topology.from_json(self.topology_path)
        self.run_id = os.urandom(8).hex()
        self.run_name = (
            f'{self.topology_path.stem}-{time.strftime("%Y%m%d_%H%M%S")}-{self.run_id}'
        )
        self.root_dir = pathlib.Path(f"{os.getcwd()}/logs/{self.run_name}")

    @contextmanager
    def start_topology(self, **kwargs):
        self.run(**kwargs)
        try:
            yield self
        finally:
            self.stop()

    def run(
        self,
        runtime: int | None = None,
        blocking: bool = False,
        gen_config: bool = True,
        mock_drivers: bool = False,
    ):
        # Create output directory
        shutil.copytree(self.topology_path.parent, self.root_dir)

        # mock-triedb not supported in monad-node for now
        triedb_driver = "triedb_driver"
        ethcall_driver = "mock_eth_call" if mock_drivers else "monad_shared"
        # Build the containers that will be needed
        docker.build(
            ".",
            file="./images/dev/Dockerfile",
            tags="monad-python-dev",
            output={"type": "docker"},
        )
        docker.build(
            "../..",
            file="./images/monad-bft-builder/Dockerfile",
            tags="monad-bft-builder:latest",
            output={"type": "docker"},
            build_args={
                "TRIEDB_TARGET": triedb_driver,
                "ETH_CALL_TARGET": ethcall_driver,
            },
        )
        docker.build(
            "../..",
            file="./images/image0/Dockerfile",
            tags="image0:latest",
            output={"type": "docker"},
        )
        docker.build(
            "../..",
            file="../devnet/Dockerfile",
            tags="monad-node:latest",
            output={"type": "docker"},
        )
        docker.build(
            "../..",
            file="../rpc/Dockerfile",
            tags="monad-rpc:latest",
            output={"type": "docker"},
        )
        docker.build(
            "../..",
            file="./images/rpc/Dockerfile",
            tags="flexnet-rpc:latest",
            output={"type": "docker"},
            build_args={
                "TRIEDB_TARGET": "triedb_driver",
                "ETH_CALL_TARGET": ethcall_driver,
            },
        )
        docker.build(
            './images/control-panel',
            file='./images/control-panel/Dockerfile',
            tags='control-panel:latest'
        )
        docker.build(
            './images/wal',
            file='./images/wal/Dockerfile',
            tags='wal:latest'
        )

        try:
            insecure_builder = docker.buildx.inspect("insecure")
        except python_on_whales.exceptions.DockerException:
            insecure_builder = docker.buildx.create(
                name="insecure",
                buildkitd_flags="--allow-insecure-entitlement security.insecure",
            )
        execution_dir = pathlib.Path("../../monad-cxx/monad-execution")
        execution_repo = git.Repo(execution_dir)
        execution_commit_hash = execution_repo.head.object.hexsha
        docker.build(
            execution_dir,
            file="../../monad-cxx/monad-execution/docker/release.Dockerfile",
            tags="monad-execution-builder:latest",
            builder=insecure_builder,
            allow=["security.insecure"],
            build_args={"GIT_COMMIT_HASH": execution_commit_hash},
            output={"type": "docker"},
        )

        # Generate scripts for each node
        # Config script has to be generated from in a Docker container, since it relies on swig and system packages
        if gen_config:
            docker.run(
                image="monad-python-dev:latest",
                remove=True,
                volumes=[(".", "/monad")],
                command=[
                    "bash",
                    "-c",
                    f"cd /monad/logs/{self.root_dir.name} && python3 /monad/common/config-gen.py -c {self.topology.get_total_node_count()} -s '' -i {self.run_id} -t {self.topology_path.name}",
                ],
            )

            # Still need the default security profile and genesis files, so copy them in
            def copy_profile(node: Node):
                shutil.copy(
                    pathlib.Path(f"{os.getcwd()}/../devnet/monad/config/profile.json"),
                    self.root_dir / node.name / "config" / "profile.json",
                )
                shutil.copy(
                    pathlib.Path(f"{os.getcwd()}/../devnet/monad/config/genesis.json"),
                    self.root_dir / node.name / "config" / "genesis.json",
                )
                shutil.copy(
                    pathlib.Path(f"{os.getcwd()}/../devnet/monad/config/forkpoint.genesis.toml"),
                    self.root_dir / node.name / "config" / "forkpoint.genesis.toml",
                )

            self.topology.for_all_nodes(copy_profile)
        else:
            # If not generating configs, copy the defaul devnet config
            def create_config(node: Node):
                shutil.copytree(
                    pathlib.Path(f"{os.getcwd()}/../devnet/monad/config"),
                    self.root_dir / node.name / "config",
                )

            self.topology.for_all_nodes(create_config)

        generators.TrafficControlScriptGenerator.generate_scripts(
            self.topology, self.root_dir, self.run_id
        )

        # Create a bridge network
        self.network = docker.network.create(self.run_name)

        self.topology.start_all_nodes(self.root_dir, self.run_name, self.run_id)
        self.topology.print_containers()
        # Give a few seconds for nodes to fully start
        time.sleep(10)

        def wait_and_stop():
            if runtime is not None:
                time.sleep(runtime)
                self.stop()

        if blocking:
            wait_and_stop()
        else:
            threading.Thread(target=wait_and_stop).start()

    def stop(self):
        self.topology.stop_all_nodes()
        self.network.remove()

    def connect(self, rpc_node_name: str):
        rpc_node = self.topology.find_node_by_name(rpc_node_name)
        rpc_port = rpc_node.get_rpc_port()

        return RpcConnection(f"http://localhost:{rpc_port}", not rpc_node.has_execution)

    def check_block_ledger_equivalence(self) -> bool:
        nodes = self.topology.get_all_nodes()
        ledger1 = self.get_block_ledger(nodes[0].name)
        for i in range(1, len(nodes)):
            ledger2 = self.get_block_ledger(nodes[i].name)
            if ledger1 != ledger2:
                return False
        return True

    def get_block_ledger(self, node_name: str) -> Ledger:
        return BlockLedger(f"{self.root_dir}/{node_name}/ledger")

    def get_write_ahead_log_as_json(self, node_name: str) -> Dict[str, json]:
        res = {}
        for file in os.listdir(f"{self.root_dir}/{node_name}"):
            # less jank way of finding all the write ahead log files?
            if file.startswith("wal"):
                json_file = f"{file}.json"
                docker.run(
                    "wal:latest",
                    volumes=[
                        (f"{self.root_dir}/{node_name}", "/monad"),
                    ],
                    command=[
                        "wal-tool",
                        "--wal-path",
                        f"/monad/{file}",
                        "stat",
                        "--output",
                        f"/monad/{json_file}",
                    ],
                )

                with open(f"{self.root_dir}/{node_name}/{json_file}", "r") as wal:
                    res[file] = json.load(wal)
        return res
