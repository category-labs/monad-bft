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

git submodule update --init --recursive

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

if [ ! -d "./.venv" ]; then
    python -m venv ./.venv
fi
source ./.venv/bin/activate

pip install ./testing-library

rm -rf logs && mkdir -p logs
python nets/net0/scripts/net-run.py
nets/net1/scripts/net-run.sh --output-dir logs --net-dir nets/net1/ --flexnet-root . --monad-bft-root ../.. 
nets/devnet-integration/scripts/net-run.sh test --output-dir logs --net-dir nets/devnet-integration/ --image-root images --monad-bft-root ../..
nets/full-nodes/scripts/net-run.sh test --output-dir logs --net-dir nets/full-nodes/ --flexnet-root . --monad-bft-root ../..
nets/consensus-nodes-nonce-test-1/scripts/net-run.sh --output-dir logs --net-dir nets/consensus-nodes-nonce-test-1/ --flexnet-root . --monad-bft-root ../..
nets/full-nodes-nonce-test-1/scripts/net-run.sh --output-dir logs --net-dir nets/full-nodes-nonce-test-1/ --flexnet-root . --monad-bft-root ../..
nets/full-nodes-nonce-test-2/scripts/net-run.sh --output-dir logs --net-dir nets/full-nodes-nonce-test-2/ --flexnet-root . --monad-bft-root ../..
nets/full-nodes-nonce-test-3/scripts/net-run.sh --output-dir logs --net-dir nets/full-nodes-nonce-test-3/ --flexnet-root . --monad-bft-root ../..

# remove artifacts if test succeeds
rm -rf logs

popd # docker/flexnet
