# monad-mock-swarm

Deterministic, single-threaded simulation of Monad consensus, for testing.

The simulation drives the **real** production consensus code — `MonadState::update`
plus the `Executor` / `Command` / `MonadEvent` contract — so tests exercise the
same logic that runs in production. Only the surrounding environment (clock,
network, ledger/mempool/state-sync) is mocked.

The harness (`sim`, `sim_verify`) is the Monad-specific top layer of a generic
simulation stack: the [`monad-sim`](../monad-sim) discrete-event engine and the
[`monad-sim-swarm`](../monad-sim-swarm) network/swarm layer. See
[`docs/simulation_framework.md`](docs/simulation_framework.md) for how a Monad
node runs on that stack, what the simulation does and does not model, and the
test surface.

The legacy engine (`mock`, `mock_swarm`, `node`, `verifier`, `swarm`,
`transformer`, `terminator`) and its tests are deprecated and slated for
removal.

## Layout

| Path | Role |
|---|---|
| `src/sim.rs` | Monad adapters: node / network / harness (`SimNode`, `SimNet`, `Network`, `SimSwarm`) |
| `src/sim_verify.rs` | assertions (`SimVerifier`) |
| `tests/sim_*.rs` | the test suite |
| `tests/sim_common`, `tests/eth_swarm_common` | shared test helpers |
| `benches/sim_engine.rs` | engine throughput (vs the legacy engine, while it remains) |

## Running

```sh
cargo test  -p monad-sim -p monad-sim-swarm -p monad-mock-swarm  # full suite
cargo test  -p monad-mock-swarm --test sim_two_nodes             # one scenario
cargo bench -p monad-mock-swarm --bench sim_engine               # engine throughput
```

Heavy statistical tests are `#[ignore]`d; run them with `--ignored`.
