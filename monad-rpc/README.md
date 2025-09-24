# monad-rpc

The prerequisite to starting the RPC server is to first [start a Monad consensus client](/monad-node/README.md) and connect the RPC server to it.

Run the following in the repo root directory:
1. `export RUST_LOG=info`
    - The logging level can be adjusted as needed.
2. `CXX=/usr/bin/g++-15 CC=/usr/bin/gcc-15 ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" TRIEDB_TARGET=triedb_driver cargo run --bin monad-rpc -- --ipc-path docker/devnet/monad/mempool.sock --triedb-path <path_to_triedb_directory> --node-config <path_to_node_toml>`
    - The `--ipc-path` must point to the same file and directories passed to the Monad consensus client.
    - The `--triedb-path` must point to a directory containing only a single triedb file.
