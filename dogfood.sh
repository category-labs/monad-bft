#!/bin/bash


# export FORK_UNIX_TS_S=1
# export FORK_EPOCH_STAKING_REWARDS=2
# export FORK_EPOCH_STAKING=3
# export FORK_ROUND=4


##############################################
export BASE_NAME=staging-dogfood-dn-0.11
export NAME=$BASE_NAME-test3

set -ex

# Use local staging branch instead of pulling from origin
git checkout $BASE_NAME
git submodule update --init

# update execution
cd monad-cxx/monad-execution
sed -i -e "s/FORK_UNIX_TS_S/$FORK_UNIX_TS_S/g" category/execution/monad/chain/monad_devnet.cpp
sed -i -e "s/FORK_UNIX_TS_S/$FORK_UNIX_TS_S/g" category/execution/monad/chain/monad_testnet2.cpp
git add category/execution/monad/chain/monad_devnet.cpp category/execution/monad/chain/monad_testnet2.cpp
git checkout -B $NAME
git commit --amend --no-edit
git push origin $NAME --force
cd ../..

git checkout -B $NAME
git add monad-cxx/monad-execution
sed -i -e "s/FORK_ROUND/$FORK_ROUND/g" monad-chain-config/src/lib.rs
sed -i -e "s/FORK_EPOCH_STAKING_REWARDS/$FORK_EPOCH_STAKING_REWARDS/g" monad-chain-config/src/lib.rs
sed -i -e "s/FORK_EPOCH_STAKING/$FORK_EPOCH_STAKING/g" monad-chain-config/src/lib.rs
sed -i -e "s/FORK_UNIX_TS_S/$FORK_UNIX_TS_S/g" monad-chain-config/src/lib.rs
git add monad-chain-config/src/lib.rs
git commit --amend --no-edit
git push origin $NAME --force
