# Executor Metrics Prometheus Migration Log

## Goal

Migrate executor metrics to use the `prometheus` crate with the minimum possible code churn.

Constraints from the task:
- metrics must stay readable in tests
- `ExecutorMetrics` should be registered once and reused across related parts instead of rebuilding snapshots by reading metrics into it
- do one commit per migrated crate
- proceed in an order that keeps crate-local tests passing as the migration moves forward

## Migration Scope

Primary crates involved:
1. `monad-executor`
2. `monad-eth-txpool`
3. `monad-updaters`
4. `monad-eth-txpool-executor`

Reasoning:
- `monad-executor` owns `ExecutorMetrics`
- `monad-eth-txpool` and `monad-eth-txpool-executor` are the only crates still mirroring atomic metrics into `ExecutorMetrics`
- `monad-updaters` contains the txpool mock executor that also mirrors `EthTxPoolMetrics` into a separate `ExecutorMetrics`

## Plan

1. Refactor `monad-executor` so `ExecutorMetrics` can register Prometheus-backed metric handles, expose explicit read helpers for tests, and keep current call sites working during the migration.
2. Migrate `monad-eth-txpool` from atomic snapshot export to registered `ExecutorMetrics` handles.
3. Migrate `monad-updaters` txpool wrapper to reuse the registered txpool metrics instead of copying them into a separate `ExecutorMetrics`.
4. Migrate `monad-eth-txpool-executor` to registered `ExecutorMetrics` handles and remove the remaining snapshot-update flow there.
5. Run crate-local tests after each step and commit once per migrated crate.

## Progress

- [completed] `monad-executor`: added Prometheus-backed metric registration, explicit test-readable accessors, and a compatibility path for existing indexed mutation.
- [completed] `monad-eth-txpool`: replaced atomic metric storage with registered handles and kept a temporary compatibility snapshot serializer/update path for downstream crates still being migrated.
- [completed] `monad-updaters`: switched the mock txpool wrapper to register `EthTxPoolMetrics` into its shared `ExecutorMetrics` once at construction time.
- [pending] `monad-eth-txpool-executor`

## Notes

- The compatibility requirement matters because many crates still mutate `ExecutorMetrics` through indexed access.
- The current plan is to land a backwards-compatible base in `monad-executor` first, then switch the atomic/snapshot crates over to registered handles crate by crate.
- Prometheus collector names are sanitized internally because the existing external metric names contain dots and must stay stable for the rest of the codebase.
- Verification for `monad-executor`:
  - `cargo test -p monad-executor`
  - `cargo check -p monad-updaters --tests`
- Verification for `monad-eth-txpool`:
  - `cargo test -p monad-eth-txpool`
  - `cargo check -p monad-eth-txpool-executor --tests`
- Verification for `monad-updaters`:
  - `cargo test -p monad-updaters`
  - `cargo check -p monad-eth-txpool-executor --tests`
