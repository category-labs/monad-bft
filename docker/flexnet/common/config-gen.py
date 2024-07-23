"""
Run this script from the net top-level directory, e.g. net0
"""

import argparse
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

from secrets import randbits
from Crypto.Protocol.KDF import scrypt
from Crypto.Hash import SHA256 as SHA256
from Crypto.Cipher import AES as AES

from monad_flexnet.topology import Topology

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


def gen_keystore_file(secret, config_path):
    scrypt_params = {
        'salt': randbits(256).to_bytes(32, 'big'),
        'key_len': 32,
        'N': 2**18,
        'r': 8,
        'p': 1,
    }
    iv = randbits(128).to_bytes(16, 'big')
    password = ""

    encryption_key = scrypt(password=password, **scrypt_params)
    encryption_key = encryption_key if isinstance(encryption_key, bytes) else encryption_key[0]

    cipher = AES.new(
        key=encryption_key[:16],
        mode=AES.MODE_CTR,
        initial_value=iv,
        nonce=b''
    )
    cipher_message = cipher.encrypt(secret)
    checksum = SHA256.new(encryption_key[16:32] + cipher_message).digest()

    for (k, v) in scrypt_params.items():
        scrypt_params[k] = v.hex() if isinstance(v, bytes) else v
    keystore_json = {
        "ciphertext": cipher_message.hex(),
        "checksum": checksum.hex(),
        "cipher": {
            "cipher_function": "AES_128_CTR",
            "params": {
                "iv": iv.hex()
            }
        },
        "kdf": {
            "kdf_name": "scrypt",
            "params": scrypt_params
        },
        "hash": "SHA256",
    }

    with open(config_path, 'w+') as f:
        f.write(json.dumps(keystore_json))


def gen_secp_key(config_path, volume, seed):
    secret = gen_secret_bytes(config_path, volume, seed)

    gen_keystore_file(secret, config_path / "id-secp")

    sk = secp256k1.PrivateKey(secret)
    pk = "0x" + sk.pubkey.serialize().hex()

    global peers
    peers[volume].secp_pubkey = pk


def gen_bls_key(config_path, volume, seed):
    secret = gen_secret_bytes(config_path, volume, seed)

    gen_keystore_file(secret, config_path / "id-bls")

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
    parser.add_argument('-i', '--id', help="run ID to append to node names", required=False)

    args = parser.parse_args()
    node_count = args.count
    if args.seed is not None:
        seed = args.seed.encode("utf-8")
    else:
        seed = None

    topology = Topology.from_json(Path(os.getcwd()) / 'topology.json')

    for _, region in topology.regions.items():
        for node in region.nodes:
            volume = node.name
            service_name = node.name
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
            id_append = f'-{args.id}' if args.id is not None else ''
            local_peers.append(
                {
                    "address": peer.service + id_append + ":" + str(BIND_ADDRESS_PORT),
                    "mempool_address": peer.service + ":" + str(8889),
                    "secp256k1_pubkey": peer.secp_pubkey,
                }
            )

        toml["bootstrap"] = {"peers": local_peers}

        toml["consensus"] = {
            "block_txn_limit": 1000,
            "block_gas_limit": 800_000_000,
            "max_reserve_balance": 100_000_000,
            "execution_delay": 10,
            "reserve_balance_check_mode": 0,
        }

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
    # high_qc in genesis forkpoint is the genesis qc. It's hard coded in monad-bft
    high_qc = {}
    high_qc["signatures"] = (
        "0x0a3ca4656f72646572736269747665633a3a6f726465723a3a4c7362306468656164a2657769647468184065696e6465780064626974730064646174618012c001400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
    )
    high_qc["signature_hash"] = (
        "0x0fb1346d3fd54a316e2b16d74be47c4d285c15e1d406fc9c1e2126448c51397a"
    )
    high_qc["info"] = {
        "vote": {
            "vote_info": {
                "id": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "epoch": 1,
                "round": 0,
                "parent_id": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "parent_round": 0,
                "seq_num": "0",
            },
            "ledger_commit_info": "NoCommit",
        }
    }

    validators = []
    for vol_path in volume_list:
        volume = vol_path.name
        validators.append(
            {
                "node_id": peers[volume].secp_pubkey,
                "cert_pubkey": peers[volume].bls_pubkey,
                "stake": 1,
            }
        )

    epoch1_validators = {"epoch": 1, "round": 0, "validators": validators}
    epoch2_validators = {"epoch": 2, "validators": validators}

    root = {
        "round": 0,
        "seq_num": "0",
        "block_id": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "state_root": "0x3aeacb8c741724594aa4d8853e431eb8378cf490cf27fc2f176ce02e93a61eb4",
    }

    genesis_toml = {
        "root": root,
        "high_qc": high_qc,
        "validator_sets": [epoch1_validators, epoch2_validators],
    }

    for vol_path in volume_list:
        genesis_toml_path = vol_path / "config" / "forkpoint.toml"
        genesis_toml_path.parent.mkdir(parents=True, exist_ok=True)

        with open(genesis_toml_path, "wb+") as f:
            f.write(WARNING.encode("utf-8"))

            tomli_w.dump(genesis_toml, f)
