#!/usr/bin/bash

# Get the RPC port from docker
RPC_PORT=$(docker ps --filter "name=node0-rpc" --format "{{.Ports}}" | awk -F'[:>-]' '{print $2}')

# Construct the base command with the RPC URL
RUST_LOG=warn cargo run --release --example txgen -- \
    --config-file ./monad-eth-testutil/examples/txgen/sample_configs/sequential_phases.json
