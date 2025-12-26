# Txgen

`txgen` is a configurable traffic generator that continuously produces and submits transactions against a set of Ethereum JSON-RPC endpoints. This README walks through the architecture at a high level and describes how to extend the generator with new traffic patterns, transaction mutations, and deployable contracts.

## Runtime Architecture

The entry point in `run.rs` wires together the major components and loops through the configured workload groups indefinitely. Each workload group can declare one or more `TrafficGen` entries, and every traffic generator spawns the same set of workers (`GeneratorHarness`, `RpcSender`, and `Refresher`). Helper tasks watch chain state and export metrics alongside the workers.

**NOTE:** Batches of accounts flow in a circle between `GeneratorHarness`, `RpcSender`, and `Refresher`

**Components**

- **Workload Scheduler** (`monad-txgen/src/run.rs`) – Reads the `Config`, deploys/loads any contracts required by the `TrafficGen`, and spawns the worker trio for each generator inside the current workload group phase. While a workload group is running, helper tasks keep metrics flowing, watch committed blocks, and build optional JSON reports.
- **Refresher** (`workers/refresher.rs`) – Owns the canonical account state for a worker. It batches `eth_getBalance`, `eth_getTransactionCount`, and (optionally) `balanceOf` calls for the sender set, updates local `SimpleAccount`s, and pushes them back into the generator channel on a configurable cadence.
- **Generator harness** (`workers/gen_harness.rs`) – Wraps a `Generator` implementation and handles balance seeding, nonce management, gas-price jittering, and configurable batch transformations (mutate/drop/convert). It produces `(TxEnvelope, Address, PrivateKey)` triples that the RPC sender can sign and transmit as-is.
- **RPC sender** (`workers/rpc_sender.rs`) – Enforces the target TPS by rate-limiting batches, performs dynamic interval adjustment based on the recent batch history, ensures request payloads stay below server limits, records `tx_hash → send_time` for later confirmation, and hands the processed accounts back to the refresher.
- **Committed transaction watcher** (`workers/committed_tx_watcher.rs`) – Streams blocks, removes seen hashes from `sent_txs`, optionally fetches receipts/logs per block, and updates success/failure counters.

The sample configs in `sample_configs/` illustrate how to compose workload groups, and `config.rs` documents every field that the CLI exposes (RPC URLs, workload definitions, telemetry options, etc.).

## Adding a New Generator

Generators are implementations of the `Generator` trait (`workers/gen_harness.rs`), and each one maps onto a `GenMode` variant (`config.rs`) that users can select in their config files. To introduce a new traffic pattern:

1. **Define the generator**

   - Create a new module under `generators/` (e.g., `generators/burst.rs`) and implement `Generator::handle_acct_group`. Use helpers like `native_transfer`, `erc20_transfer`, or construct custom envelopes directly. Remember to update local nonce/balance fields when you build transactions.
   - If the pattern needs extra configuration, add a struct similar to `FewToManyConfig` and derive `Serialize/Deserialize` so the config loader can parse it.

2. **Expose it through `GenMode` & config defaults**

   - Add a new variant to the `GenMode` enum in `config.rs` along with any supporting config structs, default values (`TxPerSender`, `sender_group_size`, etc.), and update `TrafficGen::required_contract`, `tx_per_sender`, `sender_group_size`, and `senders` so the scheduler knows how to size account pools.

3. **Register the generator**

   - Update `generators/mod.rs::make_generator` to construct your generator when the new `GenMode` variant is selected. If you depend on a deployed contract, plumb it via the `DeployedContract` argument just like the ERC20/Uniswap/ECMul generators do.

4. **Document and test**
   - Extend `sample_configs/` (TOML and JSON) with a minimal workload that exercises the new mode, and add a unit test in `config.rs` if you need to ensure round-trip serialization works.

Once these steps are complete, users can select `gen_mode = "your_new_mode"` in any workload group, and the scheduler will pick up the implementation automatically.

## Adding a Transaction Batch Transformation

The `GeneratorHarness` always passes its signed transactions through `transform_batch` (`workers/transform.rs`) before handing them to the RPC sender. A `TransformOptions` struct (built from `WorkloadGroup` fields) dictates which transformations run. To add a new transformation stage:

1. **Extend `TransformOptions`** – Add a new field (and defaults) representing the option you wish to expose. Update `TransformOptions::new` so CLI/config values flow into the struct, and add helper predicates if needed.
2. **Mutate the pipeline** – Update `transform_batch` to insert the new stage in the correct order (e.g., before dropping transactions). Each stage receives/returns a `Vec<(TxEnvelope, Address, PrivateKey)>`, so cloning costs matter—try to work in-place when possible.
3. **Expose configuration** – Add the knob to `WorkloadGroup` in `config.rs`, ensuring the setting is serialized/deserialized and documented. Workload groups describe phase-wide behavior, so every generator in the same group will inherit the transform.
4. **Test thoroughly** – Follow the pattern in `workers/transform.rs`’s unit tests to lock in the expected behavior.

Because the harness owns mutation/drop state globally, you only need to modify `transform.rs` and `config.rs`; the rest of the pipeline consumes the transformed transactions transparently.

## Adding a New Contract to Deploy

Contracts that generators depend on (ERC20, EC-MUL benchmark, Uniswap, EIP-7702 harness) live under `shared/` and are managed through the `DeployedContract` enum in `config.rs`. The runner ensures the required contract exists before a generator starts. To add another contract:

1. **Add the contract wrapper**

   - Create a module under `shared/` that knows how to deploy the contract (load bytecode, build the deployment transaction, sign it, and verify the bytecode on chain). Reuse `ensure_contract_deployed` for post-deployment confirmation.

2. **Extend configuration enums**

   - Add a new variant to `RequiredContract` and `DeployedContract`, including helper accessors (e.g., `fn my_contract(self) -> Result<MyContract>`). Update `TrafficGen::required_contract` so the scheduler advertises the dependency for any `GenMode` that needs it.

3. **Teach the loader/deployer about it**

   - Update `load_or_deploy_contracts` in `run.rs` to handle the new `RequiredContract` variant. Follow the existing pattern: check CLI overrides, load cached addresses from `deployed_contracts.json`, fall back to deploying via your wrapper, write the cache back out, and verify bytecode via `verify_contract_code`.
   - Include the new address in `DeployedContractFile` so the cache schema stays in sync.

4. **Consume it from generators**
   - Plumb the new `DeployedContract` accessor into the generator constructor in `generators/mod.rs` so the generator can issue calls/mints/etc. against the contract instance.

After these changes, txgen will automatically deploy (or reuse) the contract before starting any workload that depends on it and will persist the address to `deployed_contracts.json` for reuse across runs.

## References

- `monad-txgen/src/cli.rs` – CLI arguments, logging setup, and config loading.
- `monad-txgen/sample_configs/` – Minimal JSON/TOML configs showing multiple workload groups.
- `monad-txgen/src/report/` – Optional JSON reporting pipeline that snapshots metrics, Prometheus stats, and RPC responses when `report_dir` is configured.
