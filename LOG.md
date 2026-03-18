# Executor Metrics Prometheus Migration Log

## Goal

Migrate executor metrics to use the `prometheus` crate with the minimum possible code churn.

Constraints from the task:
- metrics must stay readable in tests
- `ExecutorMetrics` should be registered once and reused across related parts instead of rebuilding snapshots by reading metrics into it
- `ExecutorMetrics` must not own a `Registry`
- registration must happen only at the binary/export boundary
- crates that define metric constants should provide a helper that initializes an `ExecutorMetrics` with all of their metric definitions
- do one commit per migrated crate
- proceed in an order that keeps crate-local tests passing as the migration moves forward

## Migration Scope

Primary crates involved:
1. `monad-executor`
2. `monad-eth-txpool`
3. `monad-updaters`
4. `monad-eth-txpool-executor`

Reasoning:
- `monad-executor` owns `ExecutorMetrics` and is the right place for external registration APIs
- `monad-eth-txpool` defines the shared txpool metric constants and needs a crate-local initializer
- `monad-updaters` contains the txpool mock executor that should reuse the txpool initializer instead of constructing metrics ad hoc
- `monad-eth-txpool-executor` defines additional executor-local metric constants and needs to layer those on top of the txpool initializer

## Plan

1. Refactor `monad-executor` so `ExecutorMetrics` stores metric handles only, exposes external registration helpers, and stays readable in tests without embedding a registry.
2. Add `init_executor_metrics()` in `monad-eth-txpool` and switch the crate to build `EthTxPoolMetrics` from an initialized `ExecutorMetrics`.
3. Rebind `monad-updaters` txpool mock executor to the txpool initializer so it shares one `ExecutorMetrics` instance with its handles.
4. Add a crate-local initializer in `monad-eth-txpool-executor` that extends the txpool initializer with executor-local metric defs, then construct handles from that shared instance.
5. Run crate-local tests after each step and commit once per migrated crate.

## Progress

- [completed] `monad-executor`: removed registry ownership from `ExecutorMetrics`, kept legacy indexed metrics working, added external `register`/`register_all` helpers, and kept Prometheus-backed metrics readable in tests via handles and `get`.
- [completed] `monad-eth-txpool`: added `init_executor_metrics()`, changed metric construction to `from_executor_metrics(&ExecutorMetrics)`, and kept the existing serde snapshot compatibility path.
- [completed] `monad-updaters`: switched the mock txpool executor to create one txpool-initialized `ExecutorMetrics` and derive `EthTxPoolMetrics` handles from it.
- [completed] `monad-eth-txpool-executor`: added a crate-local `init_executor_metrics()` that layers executor-local metrics on top of txpool metrics, then reused that shared `ExecutorMetrics` for both the executor and client sides.

## Notes

- The compatibility requirement matters because many crates still mutate `ExecutorMetrics` through indexed access.
- Helper-initialized metrics are gauge-backed and are meant to be mutated through `ExecutorMetricHandle`s. Plain `ExecutorMetrics::default()` remains the legacy value-backed path.
- Prometheus registration now happens only through `ExecutorMetrics::register` or `ExecutorMetricsChain::register`, which is the boundary intended for the exporting binary.
- Prometheus collector names are sanitized internally because the existing external metric names contain dots and must stay stable for the rest of the codebase.
- Verification completed so far:
  - `cargo test -p monad-executor`
  - `cargo test -p monad-eth-txpool`
  - `cargo test -p monad-updaters`
  - `cargo test -p monad-eth-txpool-executor`
  - `cargo check -p monad-node --bin monad-node`
