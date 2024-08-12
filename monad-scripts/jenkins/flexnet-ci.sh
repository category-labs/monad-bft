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

rm -rf logs && mkdir -p logs
nets/devnet-integration/scripts/net-run.sh test --output-dir logs --net-dir nets/devnet-integration/ --image-root images --monad-bft-root ../..

# remove artifacts if test succeeds
rm -rf logs

popd # docker/flexnet
