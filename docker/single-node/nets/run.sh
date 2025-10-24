#!/bin/bash

set -ex

# --- Default variables ---
CACHED_VOL_ROOT=""
USE_PREBUILT=false

# --- Function Definitions ---
usage() {
    echo "Usage: $0 [--cached-build /path/to/vol_root] [--use-prebuilt]"
    echo "  --cached-build: Skips all build steps and runs docker-compose from an existing volume root."
    echo "  --use-prebuilt: Uses pre-built images (via compose.override.yaml) instead of building from source."
    exit 1
}

# --- Argument Parsing ---
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --cached-build)
            if [[ -z "$2" || "$2" == --* ]]; then
                echo "Error: --cached-build requires a path argument." >&2
                usage
            fi
            CACHED_VOL_ROOT="$2"
            shift
            ;;
        --use-prebuilt)
            USE_PREBUILT=true
            ;;
        *)
            echo "Unknown parameter passed: $1"
            usage
            ;;
    esac
    shift
done

# --- Initial Setup ---
mkdir -p logs

output_dir="logs"
monad_bft_root="../.."
devnet_dir="$monad_bft_root/docker/devnet"
rpc_dir="$monad_bft_root/docker/rpc"
net_dir="$monad_bft_root/docker/single-node/nets"

# --- Directory Sanity Checks ---
if [ ! -d "$devnet_dir" ]; then
    echo "devnet dir $devnet_dir is not a directory"
    exit 1
fi
if [ ! -d "$rpc_dir" ]; then
    echo "rpc dir $rpc_dir is not a directory"
    exit 1
fi

# --- Set Common Environment Variables ---
export MONAD_BFT_ROOT=$(realpath "$monad_bft_root")
export HOST_GID=$(id -g)
export HOST_UID=$(id -u)
export DEVNET_DIR=$(realpath "$devnet_dir")
export RPC_DIR=$(realpath "$rpc_dir")
export MONAD_EXECUTION_ROOT="${MONAD_BFT_ROOT}/monad-cxx/monad-execution"

# Get git describe tag for versioning
export GIT_TAG_VERSION=$(git -C "$MONAD_BFT_ROOT" describe --always)

# --- Main Logic: Choose between Fresh Build or Cached Run ---

if [ -z "$CACHED_VOL_ROOT" ]; then
    # === FRESH BUILD ===
    echo "Performing a fresh build..."

    # Create new node volume directory
    rand_hex=$(od -vAn -N8 -tx1 /dev/urandom | tr -d " \n" | cut -c 1-16)
    vol_root="$output_dir/$(date +%Y%m%d_%H%M%S)-$rand_hex"

    mkdir "$vol_root"
    echo "Root of node volumes created at: $vol_root"

    # Set up output dir
    mkdir -p "$vol_root/node"
    mkdir -p "$vol_root/node/ledger"
    touch "$vol_root/node/ledger/wal"
    cp -r "$net_dir"/* "$vol_root"
    cp -r "$devnet_dir/monad/config" "$vol_root/node"
    cp "$vol_root/node/config/forkpoint.genesis.toml" "$vol_root/node/config/forkpoint.toml"

    # Create fresh triedb file
    mkdir -p "$vol_root/node/triedb"
    truncate -s 4GB "$vol_root/node/triedb/test.db"

    cd "$vol_root"

    if [ "$USE_PREBUILT" = true ]; then
        # Skip building, use pre-built images
        echo "Using pre-built images..."
        docker compose -f compose.yaml -f compose.prebuilt.yaml up build_triedb build_genesis monad_execution monad_node monad_rpc
    else
        # Build monad execution (needs buildkit so unable to build in docker compose)
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

        # Run one-off build services and start node services, forcing a build of all images
        docker compose up build_triedb build_genesis monad_execution monad_node monad_rpc --build
    fi

else
    # === CACHED RUN ===
    echo "Running from cached build at $CACHED_VOL_ROOT..."
    
    vol_root=$CACHED_VOL_ROOT
    if [ ! -d "$vol_root" ]; then
        echo "Error: Provided cache directory does not exist: $vol_root" >&2
        exit 1
    fi

    echo "Using existing node volumes at: $vol_root"
    cd "$vol_root"
    # Start only the long-running services, using pre-built images
    docker compose up monad_execution monad_node monad_rpc
fi

exit 0