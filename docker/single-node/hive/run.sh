#!/bin/bash

set -ex

CHAIN_RLP_PATH=""
NBLOCKS=""

usage() {
    echo "Usage: $0 --chain-rlp /path/to/chain.rlp --nblocks <number>"
    exit 1
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --chain-rlp)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "Error: --chain-rlp requires a path argument." >&2
                usage
            fi
            CHAIN_RLP_PATH="$2"
            shift
            ;;
        --nblocks)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "Error: --nblocks requires a number argument." >&2
                usage
            fi
            NBLOCKS="$2"
            shift
            ;;
        *)
            echo "Unknown parameter passed: $1"
            usage
            ;;
    esac
    shift
done

mkdir -p logs

monad_bft_root="../.."
rpc_dir="$monad_bft_root/docker/rpc"
devnet_dir="$monad_bft_root/docker/devnet"
hive_dir="$monad_bft_root/docker/single-node/hive"

export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export RPC_DIR=$(realpath "$rpc_dir")
export MONAD_EXECUTION_ROOT="${MONAD_BFT_ROOT}/monad-cxx/monad-execution"
export GIT_TAG_VERSION=$(git -C "$MONAD_BFT_ROOT" describe --always)

rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
vol_root="logs/$(date +%Y%m%d_%H%M%S)-$rand_hex"

mkdir -p "$vol_root/node/ledger"
touch "$vol_root/node/ledger/wal"
cp -r "$hive_dir"/* "$vol_root"
cp -r "$devnet_dir/monad/config" "$vol_root/node"
sed -i'' -e 's/chain_id = 20143/chain_id = 3503995874084926/' "$vol_root/node/config/node.toml"

mkdir -p "$vol_root/node/triedb"
truncate -s 4GB "$vol_root/node/triedb/test.db"

if [ -z "$CHAIN_RLP_PATH" ]; then
    echo "Error: --chain-rlp is required." >&2
    usage
fi
if [ -z "$NBLOCKS" ]; then
    echo "Error: --nblocks is required." >&2
    usage
fi
if [ ! -f "$CHAIN_RLP_PATH" ]; then
    echo "Error: Chain RLP file does not exist: $CHAIN_RLP_PATH" >&2
    exit 1
fi
cp "$CHAIN_RLP_PATH" "$vol_root/node/chain.rlp"
export NBLOCKS

cd "$vol_root"

set +e
docker buildx inspect insecure &>/dev/null
insecure_builder_no_exist=$?
set -e
if [ $insecure_builder_no_exist -ne 0 ]; then
    docker buildx create --buildkitd-flags '--allow-insecure-entitlement security.insecure' --name insecure
fi
docker build --builder insecure --allow security.insecure \
    -f "$MONAD_EXECUTION_ROOT/docker/Dockerfile" \
    --load -t monad-execution-builder:latest "$MONAD_EXECUTION_ROOT" \
    --build-arg GIT_COMMIT_HASH=$(git -C "$MONAD_EXECUTION_ROOT" rev-parse HEAD) \
    --build-arg CC=gcc-15 \
    --build-arg CXX=g++-15 \
    --build-arg CMAKE_BUILD_TYPE=RelWithDebInfo

docker compose up --build

