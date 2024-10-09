#!/bin/bash

# Extract the dynamically assigned port for node0-rpc
RPC_PORT=$(docker ps --filter "name=node0-rpc" --format "{{.Ports}}" | awk -F'[:>-]' '{print $2}')

# Run the cargo command with the dynamically extracted port
cargo run --release --package monad-eth-testutil \
    --example tx_generator \
    -- \
    --rpc-url "http://0.0.0.0:$RPC_PORT" \
    --rpc-sender-interval-ms 100 \
    --num-final-accounts 25000000 \
    --num-rpc-senders 16 \
    --txgen-strategy many-to-one \
    --txn-batch-size 500

# cargo run --package monad-eth-testutil \
#     --example tx_generator \
#     -- \
#     --rpc-url "http://127.0.0.1:8545" \
#     --rpc-sender-interval-ms 1000 \
#     --num-final-accounts 100 \
#     --num-rpc-senders 1 \
#     --txgen-strategy many-to-one \
#     --txn-batch-size 100 \
#     --root-private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80