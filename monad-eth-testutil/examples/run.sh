#!/usr/bin/bash

cargo run --release --example txgen --  --rpc-url http://localhost:$(docker ps --filter "name=node0-rpc" --format "{{.Ports}}" | awk -F'[:>-]' '{print $2}') --tps 5000 --tx-type erc20