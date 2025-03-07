#!/bin/bash

set -ex
# Define the function to show usage
usage() {
    echo "Usage: nets/run.sh"
    exit 1
}

mkdir -p logs

output_dir="logs"
monad_bft_root="../.."
devnet_dir="$monad_bft_root/docker/devnet"
rpc_dir="$monad_bft_root/docker/rpc"
net_dir="$monad_bft_root/docker/single-node/nets"

if [ ! -d "$devnet_dir" ]; then
    echo "devnet dir $devnet_dir is not a directory"
    exit 1
fi

if [ ! -d "$rpc_dir" ]; then
    echo "rpc dir $rpc_dir is not a directory"
    exit 1
fi

# Create node volume directory
rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
vol_root="$output_dir/$(date +%Y%m%d_%H%M%S)-$rand_hex"

mkdir $vol_root
echo "Root of node volumes created at: $vol_root"

# set up output dir
mkdir -p $vol_root/node
mkdir -p $vol_root/node/ledger
touch $vol_root/node/ledger/wal
cp -r $net_dir/* $vol_root
cp -r $devnet_dir/monad/config $vol_root/node
cp $vol_root/node/config/forkpoint.genesis.toml $vol_root/node/config/forkpoint.toml

# create fresh triedb file
mkdir -p $vol_root/node/triedb
truncate -s 4GB $vol_root/node/triedb/test.db

# Set environment variables
export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export HOST_GID=$(id -g)
export HOST_UID=$(id -u)
export DEVNET_DIR=$(realpath "$devnet_dir")
export RPC_DIR=$(realpath "$rpc_dir")
export MONAD_EXECUTION_ROOT="${MONAD_BFT_ROOT}/monad-cxx/monad-execution"

# build monad execution (needs buildkit so unable to build in docker compose)
set +e
docker buildx inspect insecure
insecure_builder_no_exist=$?
set -e
if [ $insecure_builder_no_exist -ne 0 ]; then
    docker buildx create --buildkitd-flags '--allow-insecure-entitlement security.insecure' --name insecure
fi
docker build --builder insecure --allow security.insecure \
    -f $MONAD_EXECUTION_ROOT/docker/release.Dockerfile \
    --load -t monad-execution-builder:latest $MONAD_EXECUTION_ROOT \
    --build-arg GIT_COMMIT_HASH=$(git -C $MONAD_EXECUTION_ROOT rev-parse HEAD)

cd $vol_root
docker compose up build_triedb build_genesis monad_execution monad_node monad_rpc --build
exit 0
