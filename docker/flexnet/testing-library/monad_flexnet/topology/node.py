import os

from python_on_whales import docker

class Node:
    def __init__(self, name: str, has_execution: bool = False, has_rpc: bool = False, upload_speed: int = 100, download_speed: int = 100):
        self.name = name
        self.has_execution = has_execution
        self.has_rpc = has_rpc
        self.rpc_ip = None
        self.upload_speed = upload_speed
        self.download_speed = download_speed
        self.node_container = None
        self.rpc_container = None

    def start(self, root_dir: str | os.PathLike, network_name: str, run_id: str):
        self.node_container = docker.run(
            image='image0:latest',
            remove=True,
            name=f'{self.name}-{run_id}',
            volumes=[(f'{root_dir}/{self.name}', '/monad')],
            cap_add=['NET_ADMIN'],
            detach=True,
            command=['bash', '/monad/scripts/run.sh'],
            networks=[network_name]
        )

        if self.has_rpc:
            self.rpc_container = docker.run(
                image='flexnet-rpc:latest',
                remove=True,
                name=f'{self.name}-rpc-{run_id}',
                volumes=[(f'{root_dir}/{self.name}', '/monad')],
                publish=[(8080,)],
                detach=True,
                command=['bash', '/monad/scripts/rpc.sh'],
                networks=[network_name]
            )
            self.rpc_port = int(self.rpc_container.network_settings.ports['8080/tcp'][0]['HostPort'])

    def stop(self):
        self.node_container.stop()
        if self.has_rpc:
            self.rpc_container.stop()

    def get_rpc_port(self):
        return self.rpc_port

    def print_containers(self, prefix: str):
        print(f'{prefix}{self.node_container.name}: {self.node_container.id}')
        if self.has_rpc:
            print(f'{prefix}{self.rpc_container.name}: {self.rpc_container.id}')
