# TX Forward Integration Testing

## Goal

A standalone binary crate (`monad-tx-integration`) with two subcommands:

1. **`node`** — launches a mock validator node: lean UDP listener → fair queue → txpool executor → block commit loop. Writes statistics to stdout and to a log file.
2. **`submit`** — launches a tx submission agent that sends transactions over lean UDP to the node. Multiple submit agents can run concurrently against the same node.

Exercises the full path:

```
Submit agent (real UDP) → LeanUDP (WireAuth session) → Fair Queue → TxPool Executor → Proposal → Block Commit
```

All backed by in-memory mocks that satisfy the production APIs.

---

## WireAuth Authentication Model

WireAuth does **not** use signatures for per-message authentication. The flow is:

1. **Handshake**: Peers exchange static public keys (`monad_secp::PubKey`) inside encrypted handshake messages (`HandshakeInitiation` / `HandshakeResponse`).
2. **Session establishment**: After handshake, both sides derive symmetric `send_key` / `recv_key` from the key exchange. The remote peer's **public key is stored in `SessionState::remote_public_key`**.
3. **Data transport**: All subsequent messages are authenticated via symmetric encryption (AEAD). No per-message signatures — identity is established by the session's public key.

The integration test uses the same model: peer identity comes from the session public key, not from signatures on individual messages.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  monad-tx-integration node                                   │
│                                                              │
│  ┌──────────────┐    real UDP     ┌────────────────────────┐ │
│  │ Submit agent  │───────────────▶│ LeanUDP + WireAuth     │ │
│  │ (separate     │  localhost     │ (session public key     │ │
│  │  process)     │                │  identifies sender)     │ │
│  └──────────────┘                 └───────────┬────────────┘ │
│                                                │              │
│                                   RaptorCastEvent::LeanUdpTx │
│                                                │              │
│                                                ▼              │
│                                   ┌────────────────────────┐  │
│                                   │ Mock Production Router │  │
│                                   │ LeanUdpTx →            │  │
│                                   │   InsertForwardedTxs   │  │
│                                   └───────────┬────────────┘  │
│                                                │              │
│                                                ▼              │
│                                   ┌────────────────────────┐  │
│                                   │ TxPool Executor        │  │
│                                   │ - InMemoryState        │  │
│                                   │ - MockChainConfig      │  │
│                                   │ - ChannelInputStream   │  │
│                                   │ - ForwardingManager    │  │
│                                   └───────────┬────────────┘  │
│                                                │              │
│                                   MempoolEvent::Proposal     │
│                                                │              │
│                                                ▼              │
│                                   ┌────────────────────────┐  │
│                                   │ BlockCommitter         │  │
│                                   │ - commit() callable    │  │
│                                   │ - optional timer loop  │  │
│                                   │ - statistics collector │  │
│                                   └────────────────────────┘  │
│                                                               │
│  Statistics → stdout + log file                               │
└───────────────────────────────────────────────────────────────┘
```

---

## Binary Design

### CLI structure (clap)

```rust
#[derive(Parser)]
#[command(name = "monad-tx-integration")]
enum Cli {
    /// Launch a mock validator node
    Node(NodeArgs),
    /// Launch a transaction submission agent
    Submit(SubmitArgs),
}

#[derive(Args)]
struct NodeArgs {
    /// UDP listen address
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,
    /// Block commit interval in milliseconds (0 = manual only)
    #[arg(long, default_value = "1000")]
    commit_interval_ms: u64,
    /// Statistics log file path
    #[arg(long, default_value = "node-stats.jsonl")]
    stats_file: PathBuf,
    /// Number of genesis accounts
    #[arg(long, default_value = "1000")]
    num_accounts: usize,
}

#[derive(Args)]
struct SubmitArgs {
    /// Node address to send transactions to
    #[arg(long)]
    node_addr: SocketAddr,
    /// Transactions per second target
    #[arg(long, default_value = "100")]
    tps: u64,
    /// Total transactions to send (0 = unlimited)
    #[arg(long, default_value = "0")]
    count: u64,
    /// Sender secret key index (determines account identity)
    #[arg(long, default_value = "0")]
    sender_index: usize,
}
```

---

## Components to Build

### 1. Channel-based `EthTxPoolTxInputStream`

In-memory replacement for the IPC unix socket. Implements the production trait:

```rust
pub struct ChannelInputStream {
    rx: mpsc::UnboundedReceiver<EthTxPoolIpcTx>,
    event_log: Vec<(TxHash, EthTxPoolEventType)>,
}

impl EthTxPoolTxInputStream for ChannelInputStream {
    fn poll_txs(...) -> Poll<Vec<EthTxPoolIpcTx>> { /* drain rx */ }
    fn broadcast_tx_events(...) { /* collect into event_log for stats */ }
}
```

### 2. Mock production router (LeanUdpTx → TxPool)

Implements the conversion that is currently no-oped in `monad-state/src/lib.rs:1323`. This mock mirrors what the production code should do:

```rust
fn route_lean_udp_tx(event: RaptorCastEvent) -> TxPoolCommand {
    let RaptorCastEvent::LeanUdpTx { sender, tx } = event;
    TxPoolCommand::InsertForwardedTxs {
        sender: NodeId::new(sender),
        txs: vec![Bytes::from(tx.encoded_2718())],
    }
}
```

This mock satisfies the production API shape so it can later be replaced by the real monad-state wiring.

### 3. BlockCommitter (modular)

Separated into a callable core and an optional timer wrapper:

```rust
struct BlockCommitter<SB> {
    state: SB,
    seq_num: u64,
    round: u64,
}

impl<SB: StateBackendTest> BlockCommitter<SB> {
    /// Single commit step — callable directly for manual control
    fn commit(&mut self, executor: &mut EthTxPoolExecutor<...>) -> CommitStats {
        // 1. Send CreateProposal → collect MempoolEvent::Proposal
        // 2. Build EthValidatedBlock from proposal
        // 3. ledger_propose() with nonce updates
        // 4. ledger_commit()
        // 5. Send TxPoolCommand::BlockCommit
        // 6. Advance seq_num, round
        // 7. Return stats (tx count, gas used, etc.)
    }
}

/// Timer wrapper — calls commit() on interval
struct TimedBlockCommitter<SB> {
    inner: BlockCommitter<SB>,
    interval: Duration,
}
```

### 4. Statistics collector

Logs to both stdout (summary) and a JSONL file (detailed):

```rust
struct Stats {
    block_number: u64,
    txs_included: usize,
    txs_in_pool: usize,
    txs_received_total: u64,
    txs_forwarded_total: u64,
    commit_latency_us: u64,
    timestamp: SystemTime,
}
```

Written as one JSON line per block commit to the stats file. Periodic summary printed to stdout.

### 5. Submit agent

Separate process that:
1. Establishes WireAuth session with the node (real secp key exchange)
2. Generates transactions from a deterministic account (derived from `sender_index`)
3. Sends `TxEnvelope` over lean UDP at the target TPS rate
4. Logs send statistics (sent count, errors, throughput)

Multiple submit agents can run concurrently with different `--sender-index` values to simulate multi-peer load.

---

## Concrete Type Parameters

```rust
type ST = NopSignature;
type SCT = MockSignatures<NopSignature>;
type SBT = InMemoryState<ST, SCT>;
type CCT = MockChainConfig;
type CRT = MockChainRevision;
type TIS = ChannelInputStream;
```

WireAuth uses `monad_secp::KeyPair` for session establishment. The session public key serves as the peer identity (`NodeId`) throughout the pipeline — from lean socket reception through fair queue to txpool. No signature type bridging needed.

---

## Existing Helpers to Reuse

| Helper | Location | Purpose |
|--------|----------|---------|
| `create_dataplane_for_tests(bool)` | `monad-raptorcast/src/lib.rs:574` | Localhost dataplane with UDP/TCP |
| `new_wireauth_raptorcast_for_tests()` | `monad-raptorcast/src/lib.rs:690` | RaptorCast with WireAuth |
| `ema::create()` | `monad-peer-score` | EMA ScoreProvider + ScoreReader pair |
| `InMemoryStateInner::genesis()` | `monad-state-backend/src/in_memory.rs` | Genesis state with max balance |
| `InMemoryStateInner::new()` | same | State with specific accounts/nonces |
| `StateBackendTest::ledger_propose()` | same | Simulate block proposal on state |
| `StateBackendTest::ledger_commit()` | same | Finalize proposed block |
| `EthBlockPolicy::new()` | `monad-eth-block-policy/src/lib.rs` | Block policy with execution delay |
| `generate_block_with_txs()` | `monad-eth-txpool-executor/tests/executor.rs` | Create test validated blocks |
| `make_legacy_tx()`, `make_eip1559_tx()` | same | Create test transactions |
| `secret_to_eth_address()` | test utils | Derive address from test secret |

---

## Test Scenarios

### Scenario 1: Basic forwarding path
1. Start `node` with default args
2. Start `submit --node-addr <addr> --tps 10 --count 50`
3. Check stats file: all 50 txs received and included in blocks

### Scenario 2: Sustained throughput
1. Start `node --commit-interval-ms 500`
2. Start `submit --tps 1000 --count 0` (unlimited)
3. Let run for 30s, review stats file for throughput and pool depth over time

### Scenario 3: Multi-peer fair queuing
1. Start `node`
2. Start 5 submit agents with different `--sender-index` values
3. Review stats: verify fair distribution of inclusion across senders

### Scenario 4: Manual commit control
1. Start `node --commit-interval-ms 0` (timer disabled)
2. Submit transactions
3. Trigger commits via signal or RPC (design TBD) to control block boundaries precisely

### Scenario 5: Proposal + commit cycle verification
1. Start node, submit transactions
2. Verify from stats: committed txs removed from pool, nonces advance, no duplicate inclusion

---

## Crate Structure

```
monad-tx-integration/
├── Cargo.toml
├── src/
│   ├── main.rs          # clap CLI, subcommand dispatch
│   ├── node.rs          # node subcommand: setup + event loop
│   ├── submit.rs        # submit subcommand: tx generation + sending
│   ├── channel_input.rs # ChannelInputStream impl
│   ├── committer.rs     # BlockCommitter + TimedBlockCommitter
│   ├── router.rs        # LeanUdpTx → InsertForwardedTxs mock
│   └── stats.rs         # Statistics collection + JSONL writer
```

---

## Decisions Made

| Question | Decision |
|----------|----------|
| Signature bridging | No bridging needed. WireAuth uses session public keys, not signatures. The session public key is the peer identity throughout — used as `NodeId` in the txpool. |
| monad-state gap | Mock the production router in the binary. Mirrors the correct production API shape so it can be swapped later. |
| Test location | Separate binary crate `monad-tx-integration/` |
| Block commit | Modular: `commit()` is callable directly; `TimedBlockCommitter` wraps it with an interval. Timer can be disabled (interval=0). |
| Architecture | Two-process model: `node` + `submit` as separate subcommands/processes. |

---

## Implementation Plan

### Step 1: Add generic `start_with_input_stream()` to `EthTxPoolExecutor`

**Crate:** `monad-eth-txpool-executor`

`EthTxPoolExecutor::start()` is hardcoded to `TIS = EthTxPoolIpcServer` (line 100 of `src/lib.rs`). The `exec()`, `poll_next()`, and `run()` methods are already generic over TIS. Add a second constructor that accepts any `TIS: EthTxPoolTxInputStream`:

```rust
impl<ST, SCT, SBT, CCT, CRT, TIS> EthTxPoolExecutor<ST, SCT, SBT, CCT, CRT, TIS>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT> + Send + 'static,
    CCT: ChainConfig<CRT> + Send + 'static,
    CRT: ChainRevision + Send + 'static,
    TIS: EthTxPoolTxInputStream + Send + 'static,
    Self: Unpin,
{
    pub fn start_with_input_stream(
        tx_input_stream: Pin<Box<TIS>>,
        block_policy: EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: SBT,
        soft_tx_expiry: Duration,
        hard_tx_expiry: Duration,
        chain_config: CCT,
        round: Round,
        execution_timestamp_s: u64,
        do_local_insert: bool,
        score_provider: ScoreProvider<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
        score_reader: ScoreReader<NodeId<CertificateSignaturePubKey<ST>>, StdClock>,
    ) -> EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT> {
        // Same body as start() but uses provided tx_input_stream instead of EthTxPoolIpcServer
    }
}
```

Refactor the existing `start()` to call `start_with_input_stream()` internally, passing `Box::pin(EthTxPoolIpcServer::new(ipc_config)?)`.

### Step 2: Move test helpers to `monad-eth-testutil`

**Crate:** `monad-eth-testutil`

`generate_block_with_txs()`, `make_legacy_tx()`, `make_eip1559_tx()`, and `secret_to_eth_address()` currently live in `monad-eth-txpool-executor/tests/executor.rs` (test-only code). These are needed by the integration binary. Either:
- Move them to `monad-eth-testutil/src/lib.rs` (if not already there — check first, some may already be there)
- Or re-export from `monad-eth-testutil`

### Step 3: Implement `ChannelInputStream`

**Crate:** `monad-tx-integration` (new crate, `src/channel_input.rs`)

```rust
use pin_project::pin_project;

#[pin_project]
pub struct ChannelInputStream {
    rx: mpsc::UnboundedReceiver<EthTxPoolIpcTx>,
}

impl EthTxPoolTxInputStream for ChannelInputStream {
    fn poll_txs(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _generate_snapshot: impl Fn() -> EthTxPoolSnapshot,
    ) -> Poll<Vec<EthTxPoolIpcTx>> {
        let this = self.project();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(tx)) => {
                let mut batch = vec![tx];
                while let Ok(tx) = this.rx.try_recv() {
                    batch.push(tx);
                }
                Poll::Ready(batch)
            }
            Poll::Ready(None) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }

    fn broadcast_tx_events(
        self: Pin<&mut Self>,
        _events: BTreeMap<TxHash, EthTxPoolEventType>,
    ) {
        // no-op: no IPC clients to notify
    }
}
```

### Step 4: Implement `BlockCommitter`

**Crate:** `monad-tx-integration` (`src/committer.rs`)

Works against the `EthTxPoolExecutorClient` (not the inner executor — it runs in a spawned task). Interaction is via:
- `client.exec(vec![TxPoolCommand::CreateProposal { ... }])` to request proposal
- `client.next().await` to receive `MonadEvent::MempoolEvent(MempoolEvent::Proposal { ... })`
- `state.ledger_propose()` / `state.ledger_commit()` for state updates
- `client.exec(vec![TxPoolCommand::BlockCommit(vec![block])])` to notify pool

The `commit()` method is `async` since it awaits events from the client stream.

### Step 5: Implement statistics collector

**Crate:** `monad-tx-integration` (`src/stats.rs`)

- `StatsCollector` holds a `BufWriter<File>` for JSONL output
- `record_commit(stats: CommitStats)` serializes to JSON, writes line, flushes
- `print_summary()` prints aggregated stats to stdout (called on interval or shutdown)

### Step 6: Implement `node` subcommand

**Crate:** `monad-tx-integration` (`src/node.rs`)

Setup sequence:
1. Create `InMemoryState` with genesis accounts (addresses derived from deterministic secrets)
2. Create `(ScoreProvider, ScoreReader)` via `ema::create()`
3. Create `(ChannelInputStream, mpsc::UnboundedSender)` pair — sender is unused (no local IPC), but the channel exists
4. Build `DataplaneBuilder` with `UdpSocketId::AuthenticatedRaptorcast` bound to `--listen` address
5. Create `WireAuthProtocol` with a deterministic node keypair
6. Create `LeanUdpSocketHandle` with the authenticated socket and score_reader
7. Call `EthTxPoolExecutor::start_with_input_stream()` to get `EthTxPoolExecutorClient`
8. Send initial `Reset` with genesis block to initialize pool state
9. Print the bound address to stdout (so submit agents know where to connect)
10. Enter main event loop

Main event loop (`tokio::select!`):
```rust
loop {
    tokio::select! {
        biased;
        // Poll lean UDP socket for incoming transactions
        event = lean_socket.recv() => {
            // Decode LeanUdpTx, convert to InsertForwardedTxs, send via client.exec()
        }
        // Poll executor client for events (proposals, forward requests)
        event = client.next() => {
            // Handle MempoolEvent::Proposal — feed to committer
            // Handle MempoolEvent::ForwardTxs — log for stats
        }
        // Timer-based block commit (if interval > 0)
        _ = commit_timer.tick(), if commit_interval > Duration::ZERO => {
            let stats = committer.commit(&mut client).await;
            stats_collector.record_commit(stats);
        }
    }
}
```

### Step 7: Implement `submit` subcommand

**Crate:** `monad-tx-integration` (`src/submit.rs`)

Setup:
1. Generate a `monad_secp::KeyPair` from `sender_index` (deterministic seed)
2. Derive the Ethereum address for this sender (for generating valid transactions)
3. Build dataplane with UDP socket
4. Create `WireAuthProtocol` + `LeanUdpSocketHandle`
5. Initiate handshake with node address

Main loop:
1. Rate-limit to `--tps` using `tokio::time::interval(Duration::from_micros(1_000_000 / tps))`
2. Generate `TxEnvelope` with incrementing nonce (EIP-1559 tx with reasonable gas params)
3. Encode and send via `lean_socket.send(node_addr, encoded_tx, UdpPriority::Regular)`
4. Call `lean_socket.flush()`
5. Periodically print send stats (count, actual TPS, errors)
6. Also poll `lean_socket.recv()` to process WireAuth handshake responses

### Step 8: Create crate and Cargo.toml

**Crate:** `monad-tx-integration/Cargo.toml`

```toml
[package]
name = "monad-tx-integration"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "monad-tx-integration"
path = "src/main.rs"
bench = false

[dependencies]
monad-eth-txpool-executor = { path = "../monad-eth-txpool-executor" }
monad-eth-txpool-types = { path = "../monad-eth-txpool-types" }
monad-eth-txpool = { path = "../monad-eth-txpool" }
monad-eth-testutil = { path = "../monad-eth-testutil" }
monad-eth-block-policy = { path = "../monad-eth-block-policy" }
monad-executor-glue = { path = "../monad-executor-glue" }
monad-state-backend = { path = "../monad-state-backend" }
monad-chain-config = { path = "../monad-chain-config" }
monad-crypto = { path = "../monad-crypto" }
monad-peer-score = { path = "../monad-peer-score" }
monad-leanudp = { path = "../monad-leanudp" }
monad-wireauth = { path = "../monad-wireauth" }
monad-secp = { path = "../monad-secp" }
monad-dataplane = { path = "../monad-dataplane" }
monad-raptorcast = { path = "../monad-raptorcast" }

clap = { workspace = true, features = ["derive"] }
tokio = { workspace = true, features = ["full"] }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
alloy-consensus = { workspace = true }
alloy-primitives = { workspace = true }
bytes = { workspace = true }
pin-project = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
```

### Implementation order

1. **Step 1** — `start_with_input_stream()` in monad-eth-txpool-executor (unblocks everything else)
2. **Step 2** — Move/verify test helpers in monad-eth-testutil
3. **Step 8** — Create crate skeleton with Cargo.toml and `main.rs` (clap dispatch)
4. **Step 3** — `ChannelInputStream`
5. **Step 5** — Stats collector (simple, no dependencies)
6. **Step 4** — `BlockCommitter` (needs executor client API)
7. **Step 6** — `node` subcommand (wires everything together)
8. **Step 7** — `submit` subcommand (independent, uses lean socket only)
