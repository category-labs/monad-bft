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

nets/net2/scripts/net-run.sh --output-dir logs --net-dir nets/net2/ --flexnet-root . --monad-bft-root ../.. 

# clean up the artifacts if the test succeeeds
rm -rf logs

popd # docker/flexnet
