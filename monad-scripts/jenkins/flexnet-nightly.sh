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

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

if [ ! -d "./.venv" ]; then
    python -m venv ./.venv
fi
source ./.venv/bin/activate

pip install ./testing-library

rm -rf logs && mkdir -p logs
set +e
nets/net2/scripts/net-run.sh --output-dir logs --net-dir nets/net2/ --flexnet-root . --monad-bft-root ../.. 
net2_status=$?

if [[ $net2_status -ne 0 ]]; then
    exit 1
fi

# clean up the artifacts if the test succeeeds
rm -rf logs

popd # docker/flexnet
