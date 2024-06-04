#!/bin/bash

# set environment variables
MONAD_BFT_ROOT=$(git rev-parse --show-toplevel)
export MONAD_BFT_ROOT
export MONAD_EXECUTION_ROOT="${MONAD_BFT_ROOT}/monad-cxx/monad-execution"

# clean up environment
cd $MONAD_BFT_ROOT/docker/devnet
bash clean.sh
cd $MONAD_BFT_ROOT

# build monad execution
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

# build monad consensus
docker build -t monad-node:latest \
    -f $MONAD_BFT_ROOT/docker/devnet/Dockerfile $MONAD_BFT_ROOT

# build monad rpc
docker build -t monad-rpc:latest \
    -f $MONAD_BFT_ROOT/docker/rpc/Dockerfile $MONAD_BFT_ROOT

# start 1) execution -> 2) consensus -> 3) RPC server
mkdir -p $MONAD_BFT_ROOT/docker/devnet/monad/triedb
truncate -s 4GB $MONAD_BFT_ROOT/docker/devnet/monad/triedb/test.db
docker run \
    --security-opt seccomp:$MONAD_BFT_ROOT/docker/devnet/monad/config/profile.json \
    --volume $MONAD_BFT_ROOT/docker/devnet/monad:/monad \
    monad-execution:latest monad_mpt --storage /monad/triedb/test.db --create
echo "TrieDB has been initialized correctly."

EXECUTION_CONTAINER_ID=$(docker run \
    -d --security-opt seccomp:$MONAD_BFT_ROOT/docker/devnet/monad/config/profile.json \
    --volume $MONAD_BFT_ROOT/docker/devnet/monad:/monad \
    monad-execution:latest monad --db /monad/triedb/test.db --block_db /monad/ledger --genesis_file /monad/config/genesis.json)
echo "Execution node has been started."

CONSENSUS_CONTAINER_ID=$(docker run \
    -d --volume $MONAD_BFT_ROOT/docker/devnet/monad:/monad \
    monad-node:latest)
echo "Consensus node has been started."

RPC_CONTAINER_ID=$(docker run \
    -d --security-opt seccomp:$MONAD_BFT_ROOT/docker/devnet/monad/config/profile.json \
    -p 8080:8080 --volume $MONAD_BFT_ROOT/docker/devnet/monad:/monad \
    monad-rpc:latest)
echo "RPC server has been started."

# call to RPC endpoints
function continuous_curl() {
    local url="http://localhost:8080"
    local data='{"jsonrpc":"2.0", "method":"eth_blockNumber", "params":[],"id":1}'

    while true; do
        curl "$url" -X POST --data "$data" > /dev/null 2>&1 || true
        sleep 0.1 # Sleep for 1ms
    done
}

# start the rpc calls in the background
continuous_curl &
# capture the PID of the background job
CURL_PID=$!
echo "Started RPC calls."

# stop and restart RPC, continue sending RPC calls when it's down, make sure that it restarts successfully

# stop the RPC server container
docker stop $RPC_CONTAINER_ID
echo "RPC server has been stopped."
sleep 15

# restart the RPC server container
RPC_CONTAINER_ID=$(docker run \
    -d --security-opt seccomp:$MONAD_BFT_ROOT/docker/devnet/monad/config/profile.json \
    -p 8080:8080 --volume $MONAD_BFT_ROOT/docker/devnet/monad:/monad \
    monad-rpc:latest)
echo "RPC server has been restarted."
sleep 5

# cleanup function to stop docker containers
cleanup() {
    echo "Cleaning up..."
    docker stop $EXEC_CONTAINER_ID $CONS_CONTAINER_ID $RPC_CONTAINER_ID
    kill $CURL_PID
    echo "Stopped execution, consensus, and RPC Docker containers."
}
trap cleanup EXIT

# make sure RPC server is still running
CHAIN_ID=$(curl -X POST --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' http://localhost:8080 | jq -r '.result')
EXPECTED_CHAIN_ID="0x1"
echo "Received chain ID: $CHAIN_ID"

# Assertion
if [ "$CHAIN_ID" == "$EXPECTED_CHAIN_ID" ]; then
    echo "RPC server is running and returned the expected chain ID: $CHAIN_ID"
else
    echo "Unexpected chain ID: $CHAIN_ID"
    exit 1
fi

echo "Test script completed."