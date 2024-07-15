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

# Create node volume directory
net_name=$(basename $(realpath "$net_dir"))
rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
export VOL_ROOT="$output_dir/$net_name-$(date +%Y%m%d_%H%M%S)-$rand_hex"
mkdir $VOL_ROOT
echo "Root of node volumes created at: $VOL_ROOT"

cp -r $net_dir/* $VOL_ROOT

# Generate scripts and configs for nodes
vol_path_in_flexnet="/monad/$(realpath -s --relative-to=$flexnet_root $VOL_ROOT)"
topology_json_path="/monad/$(realpath -s --relative-to=$flexnet_root $VOL_ROOT)/topology.json"
docker build $image_root/../ -f $image_root/dev/Dockerfile -t monad-python-dev
# Config
docker run --rm -v $flexnet_root:/monad monad-python-dev:latest bash -c "cd $vol_path_in_flexnet && python3 /monad/common/config-gen.py -c 7 -s ''"
# tc.sh
python -c 'from monad_flexnet.generators import TrafficControlScriptGenerator; from monad_flexnet.topology import Topology; import os; topo = Topology.from_json(os.environ["VOL_ROOT"] + "/topology.json"); TrafficControlScriptGenerator.generate_scripts(topo, os.environ["VOL_ROOT"])'
# ethtx.sh
python -c 'from monad_flexnet.generators import EthTxScriptGenerator; from monad_flexnet.topology import Topology; import os; topo = Topology.from_json(os.environ["VOL_ROOT"] + "/topology.json"); EthTxScriptGenerator.generate_scripts(topo, os.environ["VOL_ROOT"], tps=1000, num_tx=1000000)'
# run.sh
python -c 'from monad_flexnet.generators import RunScriptGenerator; from monad_flexnet.topology import Topology; import os; topo = Topology.from_json(os.environ["VOL_ROOT"] + "/topology.json"); RunScriptGenerator.generate_scripts(topo, os.environ["VOL_ROOT"])'

# Set environment variables
export FLEXNET_IMAGE_ROOT=$(realpath "$image_root")
export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export HOST_GID=$(id -g)
export HOST_UID=$(id -u)

pushd $VOL_ROOT

build_services=$(docker compose config --services | grep build)
runner_services=$(docker compose config --services | grep runner)
node_services=$(docker compose config --services | grep -v -E "(build|runner)")

docker compose build $build_services
docker compose build $runner_services

docker compose up --detach $node_services
sleep $(( 60 * 10 ))
docker compose down $node_services

# return to starting dir
popd

set +e
# verify ledger
docker run --rm -v ./$flexnet_root:/monad monad-python-dev bash -c "cd $vol_path_in_flexnet && python3 /monad/common/verify-ledger.py -c 7 -l ledger -n $(( 600 * 10 - 2000 ))"
ledger_status=$?

# IDEALLY: count transactions in the ledger: 450 (ethtx actual tps) * 600 (s) * 7 (nodes)
# FIXME: ethtx generates only 10,000 unique transactions per node. total unique transactions = 10,000 (unique txns per node) * 7 (nodes)
docker run --rm -v ./$flexnet_root:/monad monad-python-dev bash -c "cd $vol_path_in_flexnet && python3 /monad/common/count-tx.py --min $(( 10000 * 7 ))"
txn_count_status=$?

if [[ $ledger_status -ne 0 || $txn_count_status -ne 0 ]]; then
    exit 1
else
    exit 0
fi

