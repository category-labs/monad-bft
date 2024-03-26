#!/bin/bash

set -ex

# check the script is called from monad-bft directory
SCRIPT_ROOT=$(dirname -- "${BASH_SOURCE[0]}")

if [ "$SCRIPT_ROOT" != "monad-scripts/jenkins" ]; then
    echo "Not running from monad-bft root directory. Is the script path changed?"
    exit 1
fi

pushd docker/flexnet

rm -rf logs && mkdir -p logs
nets/net0/scripts/net-run.sh --output-dir logs --net-dir nets/net0/ --image-root images --monad-bft-root ../.. 
nets/net1/scripts/net-run.sh --output-dir logs --net-dir nets/net1/ --image-root images --monad-bft-root ../..

# remove artifacts if test succeeds
rm -rf logs

popd # docker/flexnet
