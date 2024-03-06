"""
Run this script from the net top-level directory, e.g. net0
"""

import argparse
import base64
import subprocess
from pathlib import Path
import sys
import secp256k1
import os
import shutil
import secrets
from collections import defaultdict
from dataclasses import dataclass
import tomli_w
import json
import random

blst_bindings_path = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "blst/bindings/python"
)
blst_setup_script = "run.me"
blst_module = "blst.py"

if not blst_bindings_path.exists():
    print(
        f"blst bindings not found in {blst_bindings_path}. Check if submodules are initialized"
    )
    sys.exit(1)


if not (blst_bindings_path / blst_module).exists():
    subprocess.run(
        ["python3", blst_setup_script], cwd=blst_bindings_path.as_posix(), check=True
    )

# add blst to python path
sys.path.insert(0, blst_bindings_path.as_posix())
import blst


WARNING = """
#########################################################
#                                                       #
#        WARNING: THIS FILE SHOULD NEVER BE USED        #
#              IN PRODUCTION ENVIRONMENTS!              #
#                                                       #
#########################################################

"""
BIND_ADDRESS_PORT = 8888


@dataclass
class Peer:
    service: str = None
    secp_pubkey: str = None
    bls_pubkey: str = None


# volume name -> Peer
peers = defaultdict(Peer)


def rand_32_bytes() -> bytes:
    return secrets.token_bytes(32)


def seeded_32_bytes(seed: bytes) -> bytes:
    random.seed(seed)

    random_bytes = bytes([random.randint(0, 255) for _ in range(32)])
    return random_bytes


def gen_secret_bytes(config_path: Path, volume: str, seed: bytes) -> bytes:
    if seed is None:
        return rand_32_bytes()
    else:
        path_bytes = config_path.as_posix().encode("utf-8")
        volume_bytes = volume.encode("utf-8")
        return seeded_32_bytes(path_bytes + volume_bytes + seed)


def gen_secp_key(config_path, volume, seed):
    secret = gen_secret_bytes(config_path, volume, seed)
    secret_b64 = base64.standard_b64encode(secret)

    with open(config_path / "id-secp", "w+") as f:
        f.write(secret_b64.decode("utf-8"))

    sk = secp256k1.PrivateKey(secret)
    pk = "0x" + sk.pubkey.serialize().hex()

    global peers
    peers[volume].secp_pubkey = pk


def gen_bls_key(config_path, volume, seed):
    secret = gen_secret_bytes(config_path, volume, seed)
    secret_b64 = base64.standard_b64encode(secret)

    with open(config_path / "id-bls", "w+") as f:
        f.write(secret_b64.decode("utf-8"))

    sk = blst.SecretKey()
    sk.keygen(secret)
    pk = "0x" + blst.P1(sk).compress().hex()

    global peers
    peers[volume].bls_pubkey = pk


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="config-gen", description="Generate default config for FlexNet"
    )

    parser.add_argument("-c", "--count", type=int, help="Node count", required=True)
    parser.add_argument(
        "-s",
        "--seed",
        type=str,
        help="Seed for key generation",
        required=False,
    )

    args = parser.parse_args()
    node_count = args.count
    if args.seed is not None:
        seed = args.seed.encode("utf-8")
    else:
        seed = None

    topology_path = Path(os.getcwd()) / "topology.json"
    if not topology_path.exists():
        print(f"topology file {topology_path} doesn't exist in cwd")
        sys.exit(1)

    with open(topology_path, "r") as f:
        topology_json = json.load(f)

    for region in topology_json:
        for node in region["nodes"]:
            volume = node["volume"]
            service_name = node["service"]
            peers[volume].service = service_name

    volume_list = [Path(vol) for vol in peers.keys()]
    if node_count != len(volume_list):
        print(f"Length of node_list {len(volume_list)} != node count {node_count}")
        sys.exit(1)

    # per node: generate keys on the first pass
    for vol_path in volume_list:
        # clear the current config dir
        config_path = vol_path / "config"
        config_path.mkdir(parents=True, exist_ok=True)
        for item in config_path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        gen_secp_key(config_path, vol_path.name, seed)
        gen_bls_key(config_path, vol_path.name, seed)

    # per node: create node.toml
    for vol_path in volume_list:
        this_volume = vol_path.name
        node_toml_path = vol_path / "config" / "node.toml"

        toml = {}
        # use the first 20 bytes of the secp_pubkey as the beneficiary address
        # 42 = 2 ("0x") + 40 (20 bytes of hex string)
        toml["beneficiary"] = peers[this_volume].secp_pubkey[:42]

        local_peers = []
        for volume, peer in peers.items():
            if volume == this_volume:
                continue
            local_peers.append(
                {
                    "address": peer.service + ":" + str(BIND_ADDRESS_PORT),
                    "mempool_address": peer.service + ":" + str(8889),
                    "secp256k1_pubkey": peer.secp_pubkey,
                }
            )

        toml["bootstrap"] = {"peers": local_peers}

        toml["network"] = {
            "bind_address_host": "0.0.0.0",
            "bind_address_port": BIND_ADDRESS_PORT,
            "max_rtt_ms": 300,
            "max_mbps": 1000,
        }

        with open(node_toml_path, "wb+") as f:
            f.write(WARNING.encode("utf-8"))

            tomli_w.dump(toml, f)

        pass

    # global: create genesis.toml
    validators = []
    for vol_path in volume_list:
        volume = vol_path.name
        validators.append(
            {
                "secp256k1_pubkey": peers[volume].secp_pubkey,
                "bls12_381_pubkey": peers[volume].bls_pubkey,
                "stake": 1,
            }
        )

    genesis_toml = {"validators": validators}

    for vol_path in volume_list:
        genesis_toml_path = vol_path / "config" / "genesis.toml"
        genesis_toml_path.parent.mkdir(parents=True, exist_ok=True)

        with open(genesis_toml_path, "wb+") as f:
            f.write(WARNING.encode("utf-8"))

            tomli_w.dump(genesis_toml, f)
