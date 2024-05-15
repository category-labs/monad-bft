# monad-rpc

The prerequisite to starting the RPC server is to first [start a Monad consensus client](/monad-node/README.md) and connect the RPC server to it.

Run the following in the repo root directory:
1. `export RUST_LOG=info`
    - The logging level can be adjusted as needed.
2. `CXX=/usr/bin/g++-13 CC=/usr/bin/gcc-13 ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" TRIEDB_TARGET=triedb_driver ETH_CALL_TARGET=monad_shared cargo run --bin monad-rpc -- --ipc-path docker/devnet/monad/mempool.sock --blockdb-path docker/devnet/monad/blockdb --execution-ledger-path docker/devnet/monad/ledger --triedb-path <path_to_triedb_directory>`
    - The `--ipc-path`, `--execution-ledger-path`, and `--blockdb-path` must point to the same file and directories passed to the Monad consensus client.
    - The `--triedb-path` must point to a directory containing only a single triedb file.