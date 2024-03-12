#!/bin/bash

groupadd -g $HOST_GID user
useradd -u $HOST_UID -g user -N user

su -c "mkdir /monad/logs" -m user 

# wait for node service to create IPC socket
while [[ ! -S "/monad/mempool.sock" ]]; do
    sleep 1
done

su -c "ethtx \
        --ipc-path /monad/mempool.sock --num-tx 100 --tps 1 > /monad/logs/ethtx.log 2>&1" -m user