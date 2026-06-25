# monad-mock-swarm

Deterministic, single-threaded simulation of Monad consensus, for testing.

The simulation drives the **real** production consensus code — `MonadState::update`
plus the `Executor` / `Command` / `MonadEvent` contract — so tests exercise the
same logic that runs in production. Only the surrounding environment (clock,
network, ledger/mempool/state-sync) is mocked.

During a transition this crate contains **two** engines:

- **Simulation framework** (`sim`, `sim_verify`) — the going-forward engine: a
  Monad-node harness built on the generic [`monad-sim`](../monad-sim)
  discrete-event scheduler. See [`docs/simulation_framework.md`](docs/simulation_framework.md).
- **Legacy engine** (`mock`, `mock_swarm`, `node`, `verifier`, `swarm`,
  `transformer`, `terminator`) — per-component event queues. Retained until the
  framework above is reviewed, then removed.

## Layout

| Path | Role |
|---|---|
| `src/sim.rs` | Layer B: node / network / harness (`SimNode`, `SimNet`, `Network`, `SimSwarm`) |
| `src/sim_verify.rs` | assertions (`SimVerifier`) |
| `tests/sim_*.rs` | the test suite on the new framework |
| `tests/sim_common`, `tests/eth_swarm_common` | shared test helpers |
| `benches/sim_engine.rs` | engine throughput vs the legacy engine |

`monad-sim` (a separate crate) is Layer A — the generic, Monad-agnostic engine.

## Running

```sh
cargo test  -p monad-sim -p monad-mock-swarm        # full suite (both engines)
cargo test  -p monad-mock-swarm --test sim_two_nodes # one scenario
cargo bench -p monad-mock-swarm --bench sim_engine   # throughput comparison
```

Heavy statistical tests are `#[ignore]`d; run them with `--ignored`.
