# monad-node

Starting a Monad consensus node generates a blockdb directory, a ledger directory, a write ahead logging file, and an IPC socket:

Run the following in the repo root directory:
1. `export RUST_LOG=info`
    - The logging level can be adjusted as needed.
2. `cp docker/devnet/monad/config/forkpoint.genesis.toml docker/devnet/monad/config/forkpoint.toml`
    - Initialize consensus forkpoint to genesis
3. `CXX=/usr/bin/g++-15 CC=/usr/bin/gcc-15 ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" TRIEDB_TARGET=triedb_driver cargo run --bin monad-node -- --secp-identity docker/devnet/monad/config/id-secp --bls-identity docker/devnet/monad/config/id-bls --node-config docker/devnet/monad/config/node.toml --forkpoint-config docker/devnet/monad/config/forkpoint.toml --wal-path docker/devnet/monad/wal --mempool-ipc-path docker/devnet/monad/mempool.sock --control-panel-ipc-path docker/devnet/monad/controlpanel.sock --ledger-path docker/devnet/monad/ledger --statesync-ipc-path docker/devnet/monad/statesync.sock --triedb-path <path_to_triedb>`
    - The generated files and directories path (`--wal-path`, `--mempool-ipc-path`, `--control-panel-ipc-path`, `--ledger-path`, `--statesync-ipc-path`) can be changed.

## Encrypted TCP

Set `enable_encrypted_tcp = true` under `[network]` to advertise WireAuth support
in the signed name record. Automatic signing increments the revision when the
capability changes at the same sequence. For a read-only name record, update the
file with `sign-name-record write` before restarting. Pre-signed records must
include the configured capability.

Remove `encrypted_tcp_bind_address_port` and `self_encrypted_tcp_port` from older
node configs. If a pre-signed record included a separate encrypted TCP port,
sign a new record with a higher `self_record_seq_num`.

Deploy readers to all nodes before enabling advertisement. Both protocols are
accepted on the existing TCP listener regardless of the advertisement flag.
Senders choose WireAuth when the destination's signed record advertises it and
use signed TCP for other peers.

WireAuth sessions expire independently of TCP connections. If a peer goes away,
the surviving node may use its old session until it times out. A send with no
established session or handshake in progress starts a handshake to the peer's
advertised TCP address. Messages sent during the interruption can be lost, so
callers that still need the data must request or send it again.
