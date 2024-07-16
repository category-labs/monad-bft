# monad-node

Starting a Monad consensus node generates a blockdb directory, a ledger directory, a write ahead logging file, and an IPC socket:

Run the following in the repo root directory:
1. `export RUST_LOG=info`
    - The logging level can be adjusted as needed.
2. `cp docker/devnet/monad/forkpoint.genesis.toml docker/devnet/monad/forkpoint.toml`
    - Initialize consensus forkpoint to genesis
3. `CXX=/usr/bin/g++-13 CC=/usr/bin/gcc-13 ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" TRIEDB_TARGET=triedb_driver cargo run --bin monad-node -- --secp-identity docker/devnet/monad/config/id-secp --bls-identity docker/devnet/monad/config/id-bls --node-config docker/devnet/monad/config/node.toml --forkpoint-config docker/devnet/monad/config/forkpoint.toml --wal-path docker/devnet/monad/wal --mempool-ipc-path docker/devnet/monad/mempool.sock --control-panel-ipc-path docker/devnet/monad/controlpanel.sock --execution-ledger-path docker/devnet/monad/ledger --blockdb-path docker/devnet/monad/blockdb --triedb-path <path_to_triedb>`
    - The generated files and directories path (`--wal-path`, `--mempool-ipc-path`, `--control-panel-ipc-path`, `--execution-ledger-path`, `--blockdb-path`) can be changed.
