# Plan: Embed Chain-Data Ingest In `monad-archiver`

## Goal

Allow `monad-archiver` to run the chain-data ingest pipeline as an in-process
worker task. Chain-data ingest must read from `block_data_source` only. It must
never read from `archive_sink`.

The normal archive worker should remain independently controllable through the
existing `unsafe_disable_normal_archiving` flag. With that flag enabled,
`monad-archiver` should still be able to run chain-data ingest and any other
configured auxiliary workers.

## Target Modes

Normal archiver only:

```text
block_data_source -> archive_sink
```

Normal archiver plus chain-data:

```text
block_data_source -> archive_sink
block_data_source -> chain_data_store
```

Chain-data only inside `monad-archiver`:

```text
block_data_source -> chain_data_store
```

with:

```toml
unsafe_disable_normal_archiving = true
```

## Relevant Files

Chain-data ingest today:

- `monad-chain-data/src/bin/chain-data-ingest.rs`
  - CLI and defaults
  - fjall/S3/Dynamo store construction
  - archive reader construction
  - source fetch, retry, autotune
  - fetch/planning/write/publish/progress worker pipeline
  - progress logging and metrics

Reusable chain-data service:

- `monad-chain-data/src/api.rs`
  - `MonadChainDataService`
  - `prepare_ingest_planning_state`
  - `plan_ingest_blocks`
  - `apply_ingest_writes_with_retry`
  - `publish_ingest_advance`
  - open-index recovery and background page-count warming

- `monad-chain-data/src/lib.rs`
  - public exports

- `monad-chain-data/Cargo.toml`
  - current optional `archive-ingest` feature depends on `monad-archive`
  - S3/Dynamo/fjall feature gates

Archiver daemon:

- `monad-archive/src/bin/monad-archiver/main.rs`
  - builds `archive_writer`
  - builds `block_data_source`
  - spawns auxiliary workers
  - runs or skips normal archive worker based on
    `unsafe_disable_normal_archiving`

- `monad-archive/src/bin/monad-archiver/cli.rs`
  - TOML config model
  - CLI overrides
  - existing `unsafe_disable_normal_archiving`

- `monad-archive/src/bin/monad-archiver/block_archive_worker.rs`
  - existing normal archive worker

Archive source trait:

- `monad-archive/src/model/mod.rs`
  - `BlockDataReader`
  - `BlockDataReaderErased`

Workspace dependencies:

- `Cargo.toml`
  - workspace dependency declarations

## Dependency Direction

Avoid a crate dependency cycle.

Current state:

- `monad-chain-data` optionally depends on `monad-archive` behind the
  `archive-ingest` feature.
- `monad-archive` does not currently depend on `monad-chain-data`.

Desired embedding:

- `monad-archive` depends on `monad-chain-data`.
- `monad-archive` must not enable `monad-chain-data/archive-ingest`.

Therefore, the reusable ingest runner and store builders added to
`monad-chain-data` must not depend on `monad-archive` types. Archive-specific
conversion belongs either in the standalone ingest binary or in
`monad-archiver`.

Implementation note: Cargo rejects package cycles even when the back edge is an
optional feature. Since the old `chain-data-ingest` binary lived inside the
`monad-chain-data` package and depended on `monad-archive`, embedding
`monad-chain-data` into `monad-archive` requires moving the standalone binary
target into a tiny third package, `monad-chain-data-ingest`, that depends on
both crates. `monad-chain-data` remains the lower-level library; `monad-archive`
can depend on it without a cycle.

## Library Boundary

Add a new chain-data library module:

```text
monad-chain-data/src/ingest_runner.rs
```

Export it from:

```text
monad-chain-data/src/lib.rs
```

The module should own the production ingest pipeline currently embedded in the
binary, but expose it through chain-data-native interfaces.

### Source Trait

Define a source trait that has no dependency on `monad-archive`:

```rust
pub trait ChainDataIngestSource: Clone + Send + Sync + 'static {
    async fn get_latest_uploaded(&self) -> eyre::Result<Option<u64>>;

    async fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> eyre::Result<FinalizedBlock>;
}
```

The standalone ingest binary and `monad-archiver` will each provide an adapter
from `BlockDataReaderErased` to this trait.

### Runner Config

Define a chain-data-native config:

```rust
pub struct ChainDataIngestConfig {
    pub start: Option<u64>,
    pub end: Option<u64>,
    pub count: Option<u64>,
    pub benchmark_synthetic_start: bool,

    pub concurrency: usize,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub autotune: bool,
    pub min_concurrency: usize,
    pub max_concurrency: usize,
    pub fetch_buffer: usize,
    pub fetch_strategy: FetchStrategy,

    pub open_index_snapshot: Option<PathBuf>,

    pub max_batch_size: usize,
    pub min_batch_size: usize,
    pub plan_buffer: usize,
    pub write_buffer: usize,
    pub write_workers: usize,
    pub progress_buffer: usize,

    pub io_retry: IoRetryPolicy,
    pub log_every: u64,
}
```

Move validation from the bin into:

```rust
impl ChainDataIngestConfig {
    pub fn validate(&self) -> eyre::Result<()>;
}
```

Keep defaults aligned with current `chain-data-ingest` CLI defaults.

### Runner Function

Expose:

```rust
pub async fn run_chain_data_ingest<M, B, S>(
    service: Arc<MonadChainDataService<M, B>>,
    source: S,
    config: ChainDataIngestConfig,
    progress: ChainDataProgressSink,
) -> eyre::Result<()>
where
    M: MetaStoreCas,
    B: BlobStore,
    S: ChainDataIngestSource;
```

This should contain the existing pipeline:

- startup range resolution against published head
- lease clock observation from source latest
- lease clock refresh task
- open-index snapshot load/save
- `prepare_ingest_planning_state`
- block-number stream for bounded and follow modes
- fetch retry and jittered exponential backoff
- fetch autotune
- fetch worker
- planning worker
- write workers
- publisher
- progress accounting
- background dictionary pretraining
- worker join/error ordering

The runner should not initialize tracing subscribers and should not parse CLI.

## Cleaner Option B: Reusable Store Builders

Use the cleaner path: move store construction into reusable library APIs rather
than duplicating backend setup in `monad-archiver`.

Add a module such as:

```text
monad-chain-data/src/ingest_config.rs
```

or keep these types in `ingest_runner.rs` if the file stays manageable.

### Store Config Types

Define serializable/deserializable config structs independent of clap:

```rust
pub enum ChainDataMetaBackendConfig {
    Fjall(FjallMetaConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(DynamoMetaConfig),
}

pub enum ChainDataBlobBackendConfig {
    Fjall(FjallBlobConfig),
    #[cfg(feature = "s3")]
    S3(S3BlobConfig),
    #[cfg(feature = "dynamo")]
    Dynamo(DynamoBlobConfig),
}

pub struct ChainDataStoreConfig {
    pub meta: ChainDataMetaBackendConfig,
    pub blob: ChainDataBlobBackendConfig,
    pub cache_mib: Option<usize>,
    pub reader_only: bool,
    pub single_writer: bool,
    pub owner_id: u64,
    pub lease_blocks: u64,
    pub renew_threshold_blocks: u64,
}
```

Include fjall tuning:

```rust
pub struct ChainDataFjallTuningConfig {
    pub journal_mib: u64,
    pub memtable_mib: u64,
    pub workers: Option<usize>,
}
```

Include Scylla profile handling in the Dynamo config:

```rust
pub struct ChainDataDynamoConfig {
    pub table: Option<String>,
    pub table_prefix: Option<String>,
    pub table_layout: DynamoTableLayoutConfig,
    pub endpoint_url: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub create_table: bool,
    pub max_concurrency: usize,
    pub table_max_concurrency: usize,
    pub scylla_profile: bool,
    pub scylla_concurrency: usize,
}
```

The effective Scylla behavior should match the current ingest bin:

- single physical table layout
- global Dynamo BatchWrite concurrency set from `scylla_concurrency`
- per-table Dynamo BatchWrite concurrency set from `scylla_concurrency`

### Store Builder API

Expose a builder that returns a ready service plus optional fjall handles for
stats:

```rust
pub struct BuiltChainDataStore<M, B> {
    pub service: Arc<MonadChainDataService<M, B>>,
    pub stats_context: StoreStatsContext,
}
```

Because `MonadChainDataService<M, B>` is generic over concrete backends, a
single enum-returning builder is awkward. Keep the existing dispatch pattern,
but move it to library functions:

```rust
pub async fn dispatch_chain_data_store<R, Fut>(
    config: ChainDataStoreConfig,
    observed_upstream: Arc<AtomicU64>,
    run: impl FnOnce(BuiltChainDataStore<impl MetaStoreCas, impl BlobStore>) -> Fut,
) -> eyre::Result<R>
where
    Fut: Future<Output = eyre::Result<R>>;
```

If Rust type constraints make that too cumbersome, use the current pattern:

- public `build_dynamo_meta_store`
- public `build_s3_blob_store`
- public `build_dynamo_blob_store`
- public fjall dir resolver/opening helper
- public cache config resolver

Then both the bin and archiver can share the backend-specific builders while
each keeps a small runtime dispatch function.

This still satisfies Option B because backend validation/provisioning logic is
not duplicated.

## Archive-Specific Adapter

In the standalone bin, keep an adapter behind `archive-ingest`:

```rust
#[derive(Clone)]
struct ArchiveChainDataSource {
    reader: BlockDataReaderErased,
}
```

In `monad-archiver`, add the same adapter locally:

```rust
#[derive(Clone)]
struct ArchiverChainDataSource {
    reader: BlockDataReaderErased,
}
```

Implementation:

```rust
impl ChainDataIngestSource for ArchiverChainDataSource {
    async fn get_latest_uploaded(&self) -> eyre::Result<Option<u64>> {
        self.reader.get_latest(LatestKind::Uploaded).await
    }

    async fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> eyre::Result<FinalizedBlock> {
        let (block, receipts, traces) = tokio::try_join!(
            self.reader.get_block_by_number(block_number),
            self.reader.get_block_receipts(block_number),
            async {
                self.reader
                    .try_get_block_traces(block_number)
                    .await
                    .map(|opt| opt.unwrap_or_default())
            },
        )?;

        into_finalized_block(block, receipts, traces)
    }
}
```

The `into_finalized_block` conversion should be available from a small
archive-adapter module or duplicated temporarily with tests. Long term, prefer
one archive-adapter module behind `monad-chain-data/archive-ingest` for the
standalone bin and a local archiver adapter to avoid cycles.

## `monad-archiver` Config

Add an optional nested config:

```rust
#[serde(default)]
pub chain_data_ingest: Option<ArchiverChainDataIngestConfig>,
```

Suggested shape:

```rust
#[derive(Debug, Deserialize)]
pub struct ArchiverChainDataIngestConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(flatten)]
    pub store: ChainDataStoreConfig,

    #[serde(flatten)]
    pub ingest: ChainDataIngestConfig,
}
```

If flattened config becomes ambiguous in TOML, prefer explicit nesting:

```toml
[chain_data_ingest]
enabled = true

[chain_data_ingest.store]
...

[chain_data_ingest.runner]
...
```

Example chain-data-only archiver config:

```toml
unsafe_disable_normal_archiving = true

[block_data_source]
type = "aws"
bucket = "mainnet-deu-009-0"
region = "us-east-2"
concurrency = 50

[archive_sink]
type = "aws"
bucket = "unused-by-chain-data"
region = "us-east-2"
concurrency = 50

[chain_data_ingest]
enabled = true

[chain_data_ingest.store]
reader_only = false
single_writer = false
owner_id = 1
lease_blocks = 64
renew_threshold_blocks = 16
cache_mib = 0

[chain_data_ingest.store.meta]
type = "dynamo"
table = "chain_data_mainnet"
endpoint_url = "http://100.81.221.102:8000"
region = "us-east-1"
access_key_id = "scylla"
secret_access_key = "scylla"
scylla_profile = true
scylla_concurrency = 256

[chain_data_ingest.store.blob]
type = "s3"
bucket = "chain-data-mainnet"
endpoint_urls = [
  "http://100.124.21.75:9000",
  "http://100.81.221.102:9000",
  "http://100.104.77.72:9000",
  "http://100.118.54.55:9000",
]
region = "us-east-1"
access_key_id = "rustfsadmin"
secret_access_key = "..."
force_path_style = true
max_concurrency = 64

[chain_data_ingest.runner]
concurrency = 5000
autotune = true
min_concurrency = 250
max_concurrency = 15000
fetch_buffer = 10000
max_batch_size = 1000
min_batch_size = 1
write_workers = 1
io_retry_forever = true
io_retry_backoff_ms = 250
io_retry_max_backoff_ms = 10000
```

For the first implementation, make the nested TOML config authoritative and
skip CLI overrides for chain-data fields. The existing CLI override machinery is
verbose, and adding dozens of `--chain-data-*` flags is not necessary for the
daemon path.

## `monad-archiver` Main Wiring

Current main builds:

```rust
let archive_writer = args.archive_sink.build_block_data_archive(&metrics).await?;
let block_data_source = args.block_data_source.build(&metrics).await?;
```

Add chain-data spawning after connectivity checks and before normal archiving:

```rust
if let Some(config) = args.chain_data_ingest {
    if config.enabled {
        let source = ArchiverChainDataSource {
            reader: block_data_source.clone(),
        };
        let metrics = metrics.clone();
        let handle = tokio::spawn(async move {
            run_archiver_chain_data_ingest(source, config, metrics).await
        });
        worker_handles.push(handle);
    }
}
```

Then preserve the existing normal archive switch:

```rust
if !args.unsafe_disable_normal_archiving {
    worker_handles.push(tokio::spawn(archive_worker(
        block_data_source,
        fallback_block_data_source,
        archive_writer,
        archive_worker_opts,
        metrics,
    )));
} else {
    info!("Normal archiving disabled");
}
```

This ensures `unsafe_disable_normal_archiving` only disables:

```text
block_data_source -> archive_sink
```

It does not disable:

```text
block_data_source -> chain_data_store
```

## Worker Supervision

The current main awaits the normal archive worker directly, then awaits
auxiliary workers. With chain-data embedded, supervision should be explicit.

Use critical worker handles:

- normal archive worker, if enabled
- chain-data ingest worker, if enabled

Auxiliary infinite workers can stay in the same list, but failure semantics
should be clear:

- if any critical worker returns `Err`, return `Err`
- if any critical worker panics, return `Err`
- for finite one-shot command mode, unchanged

Simplest first pass:

```rust
for handle in worker_handles {
    handle.await??;
}
```

But that waits in insertion order and can hide failures in later handles while
an earlier infinite worker runs. Better:

```rust
let (res, _idx, remaining) = futures::future::select_all(worker_handles).await;
res??;
```

If the first completed worker exits successfully but was expected to run
forever, treat that as unexpected unless it is a configured finite mode.

## Metrics And Logging

The runner should keep the existing structured progress fields:

- `blocks_per_sec`
- `txs_per_sec`
- `logs_per_sec`
- `traces_per_sec`
- fetch latency percentiles
- planning queue/apply/publish timings
- retries
- bottleneck and pipeline limiter classification
- open-index stats

Because `monad-archiver` already has `Metrics`, the embedded worker should reuse
the same OTel endpoint but emit chain-data-specific metric names or attributes.

Do not initialize tracing subscribers in the library runner. The outer binary
owns tracing.

## Implementation Steps

1. Add `src/ingest_runner.rs`.
2. Move fetch strategy enum, concurrency controller, fetch stats, pipeline
   progress structs, phase stats, bottleneck classification, and helper
   functions from `chain-data-ingest.rs` into the new module.
3. Add `ChainDataIngestSource`.
4. Add `ChainDataIngestConfig` with defaults and validation.
5. Add `run_chain_data_ingest`.
6. Move reusable store config/builders out of the bin:
   - fjall dir resolution and opening
   - cache config scaling
   - S3 blob store builder
   - Dynamo meta store builder
   - Dynamo blob store builder
   - effective Scylla profile helpers
7. Update `chain-data-ingest.rs` to become a thin CLI wrapper:
   - parse CLI
   - initialize tracing
   - raise NOFILE
   - build metrics
   - build archive reader
   - build chain-data service through reusable builders
   - call `run_chain_data_ingest`
8. Add `monad-archive` dependency on `monad-chain-data` without
   `archive-ingest`.
9. Add `ArchiverChainDataIngestConfig` to `monad-archiver/cli.rs`.
10. Add TOML deserialization tests for `[chain_data_ingest]`.
11. Add local `ArchiverChainDataSource` adapter.
12. Add chain-data worker spawning in `monad-archiver/main.rs`.
13. Adjust worker supervision so normal archiving and chain-data ingest are
    independently runnable and failures are surfaced.
14. Validate standalone ingest still builds and its help/defaults remain
    accurate.
15. Validate `monad-archiver` builds with chain-data support.

## Tests

Unit/config tests:

- `chain_data_ingest` config deserializes from TOML.
- Missing chain-data config leaves archiver behavior unchanged.
- `unsafe_disable_normal_archiving = true` plus enabled chain-data config is
  accepted.
- Scylla profile config resolves to single-table layout and concurrency from
  `scylla_concurrency`.
- Source adapter test proves `get_latest_uploaded` and block fetch call the
  source reader.

Runner tests:

- In-memory source + in-memory chain-data store ingests a short contiguous
  range.
- Follow mode waits when source latest is behind.
- Explicit `start` must equal published head + 1.
- `count` and `end` conflicts are still enforced by the CLI wrapper, and config
  validation catches impossible ranges.

Integration smoke:

```bash
cargo +nightly-2025-12-09 build --release \
  -p monad-chain-data-ingest \
  --bin chain-data-ingest \
  --features archive-ingest,s3,dynamo
```

```bash
cargo +nightly-2025-12-09 build --release \
  -p monad-archive \
  --bin monad-archiver
```

Small run:

- use `unsafe_disable_normal_archiving = true`
- enable chain-data ingest
- set a short `count`
- confirm logs show chain-data source latest polling
- confirm no chain-data path reads from or writes archive data to `archive_sink`

## Open Decisions

1. Should `archive_sink` remain required when normal archiving is disabled and
   no sink-backed auxiliary workers are configured?

   Recommendation: keep it required for the first implementation to reduce
   archiver CLI/config churn. Relax later if operationally annoying.

2. Should chain-data embedded mode expose CLI overrides?

   Recommendation: no for the first implementation. Use TOML-only nested config.

3. Should `monad-archive` depend on all chain-data remote backends
   unconditionally?

   Recommendation: start with:

   ```toml
   monad-chain-data = { workspace = true, features = ["fjall", "s3", "dynamo"] }
   ```

   If build weight becomes a concern, add a `chain-data-ingest` feature to
   `monad-archive`.

4. Should the standalone ingest bin continue to support every current CLI flag?

   Recommendation: yes. The first extraction should be behavior-preserving for
   the bin.

## Main Risks

- Large extraction surface: the bin currently holds most of the operational
  ingest runner.
- Dependency cycle risk if archive-specific code is moved into the wrong crate
  layer.
- Generic backend dispatch may be awkward to expose as one clean public builder.
  If needed, expose backend-specific builders first and keep small dispatch code
  in each binary.
- Worker supervision must not hide a chain-data failure behind an infinite
  normal archive worker.
- Embedded chain-data and normal archive worker can double source read pressure
  when both are enabled. This is expected, but config defaults may need to be
  conservative for triedb sources.
