# monad-node

Starting a Monad consensus node generates a blockdb directory, a ledger directory, a write ahead logging file, and an IPC socket:

Run the following in the repo root directory:
1. `export RUST_LOG=info`
    - The logging level can be adjusted as needed.
2. `cp docker/devnet/monad/config/forkpoint.genesis.toml docker/devnet/monad/config/forkpoint.toml`
    - Initialize consensus forkpoint to genesis
3. `CXX=/usr/bin/g++-15 CC=/usr/bin/gcc-15 ASMFLAGS=-march=haswell CFLAGS="-march=haswell" CXXFLAGS="-march=haswell" TRIEDB_TARGET=triedb_driver cargo run --bin monad-node -- --secp-identity docker/devnet/monad/config/id-secp --bls-identity docker/devnet/monad/config/id-bls --node-config docker/devnet/monad/config/node.toml --forkpoint-config docker/devnet/monad/config/forkpoint.toml --wal-path docker/devnet/monad/wal --mempool-ipc-path docker/devnet/monad/mempool.sock --control-panel-ipc-path docker/devnet/monad/controlpanel.sock --ledger-path docker/devnet/monad/ledger --statesync-ipc-path docker/devnet/monad/statesync.sock --triedb-path <path_to_triedb>`
    - The generated files and directories path (`--wal-path`, `--mempool-ipc-path`, `--control-panel-ipc-path`, `--ledger-path`, `--statesync-ipc-path`) can be changed.

## Signing name records

**Behavior change:** when `--node-config` supplies the sequence, `sign-name-record`
now signs `self_record_seq_num` unchanged. Older versions signed
`self_record_seq_num + 1`, so rerunning a saved command no longer generates the
next sequence. To sign a new operator sequence, update the configured value or
pass it explicitly with `--self-record-seq-num`.

`--name-record-path` reads saved counters without changing the file. Its revision
is reused only when the sequence being signed matches the saved sequence;
otherwise revision defaults to zero. An explicit `--record-revision` overrides
that default.

## Encrypted TCP

`[network].enable_encrypted_tcp = true` advertises the encrypted TCP capability
in the signed name record. Automatic signing persists the changed record with
an increased revision. Pre-signed records must include the same capability.

Both protocols use the existing TCP listener and advertised TCP port. Senders
use WireAuth when the destination's signed record advertises support; otherwise
they use the existing signature protocol. WireAuth failures do not fall back to
plaintext. The old encrypted TCP port fields do not select the protocol.

Deploy readers to all nodes before enabling advertisement. Both frame protocols
are accepted regardless of the advertisement flag. WireAuth session expiry and
garbage collection run independently of TCP connection closure.
