#!/bin/bash

set -x
# Define the function to show usage
usage() {
    echo "Usage: $0 --output-dir <dir_path> --net-dir <net_path> --image-root <image_root> --monad-bft-root <monad_bft_root>"
    exit 1
}


# Initialize variables
output_dir=""
net_dir=""
image_root=""
monad_bft_root=""


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
        "") shift; break ;;
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

# Create node volume directory
net_name=$(basename $(realpath "$net_dir"))
rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
vol_root="$output_dir/$net_name-$(date +%Y%m%d_%H%M%S)-$rand_hex"
mkdir $vol_root
echo "Root of node volumes created at: $vol_root"

cp -r $net_dir/* $vol_root
# Set environment variables
export FLEXNET_IMAGE_ROOT=$(realpath "$image_root")
export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export HOST_GID=$(id -g)
export HOST_UID=$(id -u)

pushd $vol_root

build_services=$(docker compose config --services | grep build)
node_services=$(docker compose config --services | grep -v build)
docker compose build $build_services &&
docker compose up --detach $node_services
sleep 30
docker compose down $node_services

# return to starting dir
popd


# verify ledger
docker run --rm -v ./$vol_root:/monad monad-python bash -c "python3 scripts/verify-ledger.py -c 3 -l ledger -n 300"
# inspect the blocks, verify content
docker run --rm -v ./$vol_root:/monad monad-python bash -c "python3 /monad/scripts/inspect-block.py --data /monad/data/txns.json"

