#!/bin/bash

set -ex
# Define the function to show usage
usage() {
    echo "Usage: $0 run/test --output-dir <dir_path> --net-dir <net_path> --image-root <image_root> --monad-bft-root <monad_bft_root>"
    exit 1
}


# Initialize variables
output_dir=""
net_dir=""
image_root=""
monad_bft_root=""
mode=""


if [ "$#" -eq 0 ]; then
    usage; exit 1
fi

# Extract options and their arguments into variables
while true; do
    case "$1" in
        --output-dir)
            case "$2" in
                "") echo "$1 must have a value"; shift 1 ;;
                *)  output_dir="$2"; shift 2 ;;
            esac ;;
        --net-dir)
            case "$2" in
                "") echo "$1 must have a value"; shift 1 ;;
                *)  net_dir="$2"; shift 2 ;;
            esac ;;
        --image-root)
            case "$2" in 
                "") echo "$1 must have a value"; shift 1 ;;
                *)  image_root="$2"; shift 2 ;;
            esac ;;
        --monad-bft-root)
            case "$2" in
                "") echo "$1 must have a value"; shift 1 ;;
                *)  monad_bft_root="$2"; shift 2;;
            esac ;;
        run) shift 1; mode="run" ;;
        test) shift 1; mode="test" ;;
        "") break ;;
        *) echo "Error parsing $1, check arguments before it"; usage; exit 1 ;;
    esac
done

# Verify the arguments 
if [ -z "$output_dir" ]; then
    echo "Error: --output-dir is required."
    usage
elif [ ! -d "$output_dir" ]; then
    echo "Error: --output-dir doesn't exist"
    exit 1
fi

if [ -z "$net_dir" ]; then
    echo "Error: --net-dir is required."
    usage
elif [ ! -d "$net_dir" ]; then
    echo "Error: --net-dir is not a directory."
    exit 1
fi

if [ -z "$image_root" ]; then
    echo "Error: --image-root is required."
    usage
elif [ ! -d "$image_root" ]; then
    echo "Error: --image-root is not a directory."
    exit 1
fi

if [ -z "$monad_bft_root" ]; then
    echo "Error: --monad-bft-root is required."
    usage
elif [ ! -d "$monad_bft_root" ]; then
    echo "Error: --monad-bft-root is not a directory."
    exit 1
fi

devnet_dir="$monad_bft_root/docker/devnet"
rpc_dir="$monad_bft_root/docker/rpc"

if [ ! -d "$devnet_dir" ]; then
    echo "devnet dir $devnet_dir is not a directory"
    exit 1
fi

if [ ! -d "$rpc_dir" ]; then
    echo "rpc dir $rpc_dir is not a directory"
    exit 1
fi

# Create node volume directory
net_name=$(basename $(realpath "$net_dir"))
rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
vol_root="$output_dir/$net_name-$(date +%Y%m%d_%H%M%S)-$rand_hex"

mkdir $vol_root
echo "Root of node volumes created at: $vol_root"

# set up output dir
mkdir -p $vol_root/node
cp -r $net_dir/* $vol_root
cp -r $devnet_dir/monad/config $vol_root/node
cp $vol_root/node/config/forkpoint.genesis.toml $vol_root/node/config/forkpoint.toml

# create fresh triedb file
mkdir -p $vol_root/node/triedb
truncate -s 4GB $vol_root/node/triedb/test.db

# Set environment variables
export FLEXNET_IMAGE_ROOT=$(realpath "$image_root")
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

if [ "$mode" == "run" ]; then
    cd $vol_root
    docker compose up build_triedb monad_execution monad_node monad_rpc --build

    exit 0
elif [ "$mode" == "test" ]; then
    pushd $vol_root
    
    mkdir -p data

    build_services=$(docker compose config --services | grep build)
    node_services=$(docker compose config --services | grep -v -E "(build|runner)")

    # test mode only needs to expose ports internally
    sed -i 's/ports:/expose:/g' compose.yaml

    docker compose build $build_services &&
    docker compose up --detach --build $node_services
    sleep 10
    docker compose down $node_services

    popd # $vol_root
    # inspect consensus block ledger, verify transactions submitted are in the block
    docker run --rm -v ./$vol_root:/monad monad-python bash -c "python3 /monad/scripts/inspect-block.py --data /monad/data/txns.json"

    # e2e_tester errors are redirected to the file. Assert that it's empty
    e2e_tester_err=$vol_root/e2e_tester/logs/e2e-tester.err
    if [ -s $e2e_tester_err ]; then
        cat $e2e_tester_err
        exit 1
    fi

    exit 0
else
    echo "Unsupported mode $mode"
fi
