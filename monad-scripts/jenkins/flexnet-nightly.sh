#!/bin/bash

set -ex

# check the script is called from monad-bft directory
SCRIPT_ROOT=$(dirname -- "${BASH_SOURCE[0]}")

if [ "$SCRIPT_ROOT" != "monad-scripts/jenkins" ]; then
    echo "Not running from monad-bft root directory. Is the script path changed?"
    exit 1
fi

pushd docker/flexnet

if ! pgrep -x -u $USER "dockerd" > /dev/null; then
    systemctl --user start docker
fi

git submodule update --init common/blst

rm -rf logs && mkdir -p logs
set +e
nets/net2/scripts/net-run.sh --output-dir logs --net-dir nets/net2/ --flexnet-root . --monad-bft-root ../.. 
net2_status=$?
nets/net2-1/scripts/net-run.sh --output-dir logs --net-dir nets/net2-1/ --flexnet-root . --monad-bft-root ../.. 
net2_1_status=$?

if [[ $net2_status -ne 0 || $net2_1_status -ne 0 ]]; then
    exit 1
fi

# clean up the artifacts if the test succeeeds
rm -rf logs

popd # docker/flexnet
