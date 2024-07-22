import os
import pathlib
import python_on_whales
import time

from python_on_whales import docker

class Node:

    RPC_BASE_CMD = [
        'bash', '-c',
        'mkdir /monad/logs; '
        'monad-rpc --ipc-path /monad/mempool.sock '
        '> /monad/logs/rpc.log 2>&1'
    ]
    RPC_WITH_EXECUTION_CMD = ['bash', '-c',
        'mkdir /monad/logs; '
        'monad-rpc --ipc-path /monad/mempool.sock '
        '--triedb-path /monad/triedb '
        '--blockdb-path /monad/blockdb --execution-ledger-path /monad/ledger '
        '> /monad/logs/rpc.log 2>&1'
    ]
    EXECUTION_CMD = [
        'bash', '-c',
        'mkdir /monad/logs; '
        'monad --db /monad/triedb/test.db '
        '--genesis_file /monad/config/genesis.json '
        '--block_db /monad/ledger '
        '--log_level DEBUG '
        '> /monad/logs/execution.log 2>&1'
    ]
    RUN_CMD = [
        'bash', '-c',
        '/monad/scripts/tc.sh; '
        'mkdir /monad/logs; '
        'monad-node --secp-identity /monad/config/id-secp '
        '--bls-identity /monad/config/id-bls '
        '--node-config /monad/config/node.toml '
        '--forkpoint-config /monad/config/forkpoint.toml '
        '--wal-path /monad/wal '
        '--mempool-ipc-path /monad/mempool.sock '
        '--control-panel-ipc-path /monad/controlpanel.sock '
        '--execution-ledger-path /monad/ledger '
        '--blockdb-path /monad/blockdb '
        '--triedb-path /monad/triedb '
        '> /monad/logs/client.log 2>&1'
    ]

    def __init__(self, name: str, has_execution: bool = False, has_rpc: bool = False, upload_speed: int = 100, download_speed: int = 100):
        self.name = name
        self.has_execution = has_execution
        self.has_rpc = has_rpc
        self.rpc_ip = None
        self.upload_speed = upload_speed
        self.download_speed = download_speed
        self.execution_container = None
        self.node_container = None
        self.rpc_container = None

    def start(self, root_dir: str | os.PathLike, network_name: str, run_id: str):
        Node._create_empty_triedb(f'{root_dir}/{self.name}/triedb/test.db')
        if self.has_execution:
            docker.run(
                image='monad-execution-builder:latest',
                remove=True,
                name=f'{self.name}-{run_id}-execution',
                volumes=[(f'{root_dir}/{self.name}', '/monad')],
                command=['monad_mpt', '--storage', '/monad/triedb/test.db', '--create'],
                networks=[network_name],
                security_options=[f'seccomp={root_dir}/{self.name}/config/profile.json']
            )
            self.execution_container = docker.run(
                image='monad-execution-builder:latest',
                remove=True,
                name=f'{self.name}-{run_id}-execution',
                volumes=[(f'{root_dir}/{self.name}', '/monad')],
                detach=True,
                command=Node.EXECUTION_CMD,
                networks=[network_name],
                security_options=[f'seccomp={root_dir}/{self.name}/config/profile.json']
            )

        self.node_container = docker.run(
            image='image0:latest',
            remove=True,
            name=f'{self.name}-{run_id}',
            volumes=[(f'{root_dir}/{self.name}', '/monad')],
            cap_add=['NET_ADMIN'],
            detach=True,
            command=Node.RUN_CMD,
            networks=[network_name],
            security_options=[f'seccomp={root_dir}/{self.name}/config/profile.json']
        )

        if self.has_rpc:
            rpc_cmd = Node.RPC_BASE_CMD
            if self.has_execution:
                rpc_cmd = Node.RPC_WITH_EXECUTION_CMD
            self.rpc_container = docker.run(
                image='monad-rpc:latest',
                remove=True,
                name=f'{self.name}-rpc-{run_id}',
                volumes=[(f'{root_dir}/{self.name}', '/monad')],
                publish=[(8080,)],
                detach=True,
                command=rpc_cmd,
                networks=[network_name],
                security_options=[f'seccomp={root_dir}/{self.name}/config/profile.json']
            )
            self.rpc_port = int(self.rpc_container.network_settings.ports['8080/tcp'][0]['HostPort'])

    @staticmethod
    def _try_stop(container):
        try:
            container.stop()
        except python_on_whales.exceptions.NoSuchContainer:
            pass

    def stop(self):
        Node._try_stop(self.node_container)
        if self.has_rpc:
            Node._try_stop(self.rpc_container)
        if self.has_execution:
            Node._try_stop(self.execution_container)

    def get_rpc_port(self):
        return self.rpc_port

    def print_containers(self, prefix: str):
        print(f'{prefix}{self.node_container.name}: {self.node_container.id}')
        if self.has_rpc:
            print(f'{prefix}{self.rpc_container.name}: {self.rpc_container.id}')
        if self.has_execution:
            print(f'{prefix}{self.execution_container.name}: {self.execution_container.id}')

    @staticmethod
    def _create_empty_triedb(path: str | os.PathLike):
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            # 4 GiB
            f.truncate(4 * 1024 * 1024 * 1024)
