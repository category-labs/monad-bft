#!/bin/bash

set -ex
# Define the function to show usage
usage() {
    echo "Usage: $0 --output-dir <dir_path> --net-dir <net_path> --flexnet-root <flexnet_root> --monad-bft-root <monad_bft_root>"
    exit 1
}


# Initialize variables
output_dir=""
net_dir=""
image_root=""
monad_bft_root=""
flexnet_root=""
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
        --flexnet-root)
            case "$2" in
                "") echo "$1 must have a value"; shift 1 ;;
                *)  flexnet_root="$2"; shift 2 ;;
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

image_root=$flexnet_root/images
common_dir=$flexnet_root/common

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

if [ -z "$flexnet_root" ]; then
    echo "Error: --flexnet-root is required."
    usage
elif [ ! -d "$flexnet_root" ]; then
    echo "Error: --flexnet-root is not a directory."
    exit 1
fi

if [ ! -d "$common_dir" ]; then
    echo "Error: cannot find 'common' under flexnet root"
    exit 1
fi

if [ ! -d "$image_root" ]; then
    echo "Error: cannot find 'images' under flexnet root"
    exit 1
fi

if [ -z "$monad_bft_root" ]; then
    echo "Error: --monad-bft-root is required."
    usage
elif [ ! -d "$monad_bft_root" ]; then
    echo "Error: --monad-bft-root is not a directory."
    exit 1
fi

if [ "$mode" != "run" ] && [ "$mode" != "test" ]; then
    echo "Unsupported mode $mode"
    exit 1 
fi

# Create node volume directory
net_name=$(basename $(realpath "$net_dir"))
rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
vol_root="$output_dir/$net_name-$(date +%Y%m%d_%H%M%S)-$rand_hex"
mkdir $vol_root
echo "Root of node volumes created at: $vol_root"

cp -r $net_dir/* $vol_root

# Generate scripts and configs for nodes
vol_path_in_flexnet="/monad/$(realpath -s --relative-to=$flexnet_root $vol_root)"
topology_json_path="/monad/$(realpath -s --relative-to=$flexnet_root $vol_root)/topology.json"
docker build $image_root/dev -t monad-python-dev
# Config
docker run --rm -v $flexnet_root:/monad monad-python-dev:latest bash -c "cd $vol_path_in_flexnet && python3 /monad/common/config-gen.py -c 4 -s ''"
# tc.sh
docker run --rm -v $flexnet_root:/monad monad-python-dev:latest python3 /monad/common/tc-gen.py $topology_json_path

# Set environment variables
export FLEXNET_IMAGE_ROOT=$(realpath "$image_root")
export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export MONAD_EXECUTION_ROOT="${MONAD_BFT_ROOT}/monad-cxx/monad-execution"
export HOST_GID=$(id -g)
export HOST_UID=$(id -u)

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
    --load -t monad-execution:latest $MONAD_EXECUTION_ROOT \
    --build-arg GIT_COMMIT_HASH=$(git -C $MONAD_EXECUTION_ROOT rev-parse HEAD)

pushd $vol_root

# create fresh triedb file, and copy genesis.json file
for i in {0..3}; do
    mkdir -p node$i/triedb
    mkdir -p node$i/logs
    truncate -s 4GB node$i/triedb/test.db
    docker run --rm  -v ./node$i:/monad --security-opt seccomp=${MONAD_BFT_ROOT}/docker/devnet/monad/config/profile.json \
        -t monad-execution:latest bash -c "monad_mpt --storage /monad/triedb/test.db --create"
    cp ${MONAD_BFT_ROOT}/docker/devnet/monad/config/genesis.json node$i/config/genesis.json
done

build_services=$(docker compose config --services | grep build)
runner_services=$(docker compose config --services | grep runner)

sed -i 's/ports:/expose:/g' compose.yaml

docker compose build $build_services
docker compose build $runner_services

if [ "$mode" == "run" ]; then
    node_services=$(docker compose config --services | grep -v -E "(build|runner|tester)")

    docker compose up $node_services
    exit 0
elif [ "$mode" == "test" ]; then
    node_services=$(docker compose config --services | grep -v -E "(build|runner)")

    # test mode only needs to expose ports internally
    sed -i 's/ports:/expose:/g' compose.yaml

    docker compose up -d $node_services
    sleep 20
    docker compose down $node_services

    popd

    # e2e_tester errors are redirected to the file. Assert that it's empty
    e2e_tester_err=$vol_root/e2e_tester/logs/e2e-tester.err
    if [ -s $e2e_tester_err ]; then
        cat $e2e_tester_err
        exit 1
    fi
else
    echo "Unreachable"
    exit 1
fi
exit 0
