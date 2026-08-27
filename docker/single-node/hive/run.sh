#!/bin/bash

set -ex

CHAIN_RLP_PATH=""
NBLOCKS=""
HIVE_CHAIN_ID="3503995874084926"

usage() {
    echo "Usage: $0 --chain-rlp /path/to/chain.rlp --nblocks <number>"
    exit 1
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --chain-rlp)
            if [[ -z "${2:-}" || "$2" == --* ]]; then
                echo "Error: --chain-rlp requires a path argument." >&2
                usage
            fi
            CHAIN_RLP_PATH="$2"
            shift
            ;;
        --nblocks)
            if [[ -z "${2:-}" || "$2" == --* ]]; then
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

if [ -z "$CHAIN_RLP_PATH" ]; then
    echo "Error: --chain-rlp is required." >&2
    usage
fi
if [ -z "$NBLOCKS" ]; then
    echo "Error: --nblocks is required." >&2
    usage
fi
if [[ ! "$NBLOCKS" =~ ^[0-9]+$ ]]; then
    echo "Error: --nblocks must be a non-negative integer: $NBLOCKS" >&2
    usage
fi
if [ ! -f "$CHAIN_RLP_PATH" ]; then
    echo "Error: Chain RLP file does not exist: $CHAIN_RLP_PATH" >&2
    exit 1
fi

replace_chain_id() {
    local config_path="$1"
    local tmp_path

    tmp_path="$(mktemp "${config_path}.XXXXXX")"
    if ! awk -v chain_id="$HIVE_CHAIN_ID" '
        /^[[:space:]]*chain_id[[:space:]]*=/ {
            count++
            sub(/=.*/, "= " chain_id)
        }
        { print }
        END {
            if (count != 1) {
                printf("Error: expected exactly one chain_id in %s, found %d\n", FILENAME, count) > "/dev/stderr"
                exit 1
            }
        }
    ' "$config_path" > "$tmp_path"; then
        rm -f "$tmp_path"
        exit 1
    fi
    mv "$tmp_path" "$config_path"
}

CHAIN_RLP_PATH="$(realpath "$CHAIN_RLP_PATH")"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

monad_bft_root="$script_dir/../../.."
rpc_dir="$monad_bft_root/docker/rpc"
devnet_dir="$monad_bft_root/docker/devnet"
hive_dir="$script_dir"

export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export RPC_DIR=$(realpath "$rpc_dir")
export MONAD_EXECUTION_ROOT="${MONAD_BFT_ROOT}/monad-execution"
export GIT_TAG_VERSION=$(git -C "$MONAD_BFT_ROOT" describe --always)

rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
logs_dir="$hive_dir/logs"
vol_root="$logs_dir/$(date +%Y%m%d_%H%M%S)-$rand_hex"

mkdir -p "$vol_root/node/ledger"
touch "$vol_root/node/ledger/wal"
cp "$hive_dir/compose.yaml" "$hive_dir/run.sh" "$vol_root"
cp -r "$devnet_dir/monad/config" "$vol_root/node"
# Hive uses the local single-node RPC config shape; only the chain ID must
# match the execution-side hive_net chain.
replace_chain_id "$vol_root/node/config/node.toml"

mkdir -p "$vol_root/node/triedb"
truncate -s 4GB "$vol_root/node/triedb/test.db"

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
    --build-arg CMAKE_BUILD_TYPE=RelWithDebInfo \
    --build-arg TOOLCHAIN=gcc-avx2

docker compose up build_triedb build_genesis monad_execution monad_rpc --build
