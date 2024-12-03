#!/usr/bin/bash

# Get the RPC port from docker
RPC_PORT=$(docker ps --filter "name=node0-rpc" --format "{{.Ports}}" | awk -F'[:>-]' '{print $2}')

# Construct the base command with the RPC URL
cargo run --release --example txgen -- \
    --rpc-url "http://localhost:$RPC_PORT" \
    few-to-many \
    "$@"  # Pass all additional arguments
