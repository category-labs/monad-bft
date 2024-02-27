#!/bin/bash

groupadd -g $HOST_GID user
useradd -u $HOST_UID -g user -N user
# usermod -aG sudo user
# echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

su -c "mkdir /monad/logs" -m user 

# wait for node service to create IPC socket
while [[ ! -S "/monad/mempool.sock" ]]; do
    sleep 1
done

su -c "monad-rpc \
        --ipc-path /monad/mempool.sock > /monad/logs/rpc.log 2>&1" -m user
        