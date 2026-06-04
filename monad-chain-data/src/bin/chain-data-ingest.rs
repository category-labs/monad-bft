// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! Reads blocks + receipts from any `monad-archive` backend and feeds them
//! into the chain-data ingest pipeline. Source selection (FS, S3, MongoDB,
//! DynamoDB, TrieDB) reuses `monad_archive::cli::BlockDataReaderArgs` so
//! the same `--block-data-source "fs /path"` / `"aws bucket"` / etc.
//! syntax works as in the other archive bins.

#![recursion_limit = "256"]

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::Bytes;
use clap::{Parser, ValueEnum};
use eyre::{bail, eyre, Context, Result};
use futures::{stream, StreamExt};
use monad_archive::{
    archive_reader::LatestKind,
    cli::BlockDataReaderArgs,
    metrics::{MetricNames, Metrics},
    model::{
        block_data_archive::{
            decode_trace, Block, BlockReceipts, BlockTraces, CallFrame, CallKind as ArchiveCallKind,
        },
        BlockDataReader, BlockDataReaderErased,
    },
};
use monad_chain_data::{
    compute_trace_addresses,
    store::{CacheConfig, FjallStore, FjallTuning, MetaStoreCas},
    CallKind, Family, FinalizedBlock, IngestPlan, IngestTrace, IngestTx, IoRetryPolicy,
    MonadChainDataService, PublicationAdvance, QueryLimits,
};
use opentelemetry::KeyValue;
use rand::Rng;
use tokio::sync::{mpsc, Mutex as TokioMutex, Semaphore};
use tracing::{debug, info, warn, Level};

const SLOW_FETCH_LOG_THRESHOLD_MS: u64 = 500;
const FETCH_FINAL_RETRY_BACKOFF_ROUNDS: u32 = 3;
const FETCH_CONTROL_TICK: Duration = Duration::from_secs(2);
const FOLLOW_POLL_INTERVAL: Duration = Duration::from_millis(50);

fn parse_family_shard(spec: &str) -> Result<(Family, u64)> {
    let (family, shard) = spec
        .split_once(':')
        .ok_or_else(|| eyre!("expected <family>:<shard>, e.g. log:6"))?;
    let family = match family {
        "log" | "logs" => Family::Log,
        "tx" | "txs" => Family::Tx,
        "trace" | "traces" => Family::Trace,
        other => bail!("unknown family {other:?}; expected log, tx, or trace"),
    };
    let shard = shard
        .parse::<u64>()
        .with_context(|| format!("invalid bitmap shard in {spec:?}"))?;
    Ok((family, shard))
}

#[derive(Parser, Debug)]
#[command(
    name = "chain-data-ingest",
    about = "Stream blocks + receipts from a monad-archive source into a local chain-data store"
)]
struct Cli {
    /// fjall data directory shared by the meta and blob stores. Requires both
    /// `--meta-backend fjall` and `--blob-backend fjall`. Use `--meta-data-dir`
    /// and `--blob-data-dir` to place them in separate DBs, or for mixed
    /// fjall/remote setups.
    #[arg(long, conflicts_with_all = ["meta_data_dir", "blob_data_dir"])]
    data_dir: Option<PathBuf>,

    /// fjall data directory for metadata tables. Created on first run. Required
    /// when `--meta-backend fjall` (and `--data-dir` is not given); rejected
    /// otherwise.
    #[arg(long)]
    meta_data_dir: Option<PathBuf>,

    /// fjall data directory for blob tables. Created on first run. Required when
    /// `--blob-backend fjall` (and `--data-dir` is not given); rejected
    /// otherwise.
    #[arg(long)]
    blob_data_dir: Option<PathBuf>,

    /// Archive source. Examples: `"fs /var/lib/monad-archive"`,
    /// `"aws my-bucket"`, `"mongodb mongodb://host:27017 dbname"`.
    /// See `monad_archive::cli::BlockDataReaderArgs` for the full grammar.
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    block_data_source: BlockDataReaderArgs,

    /// First block to ingest (inclusive). Defaults to `published_head + 1`
    /// (i.e. `1` on a fresh data directory). When provided explicitly, must
    /// match that value — the flag exists for assertion in scripted runs.
    #[arg(long)]
    start: Option<u64>,

    /// Last block to ingest (inclusive). Mutually exclusive with `--count`.
    /// When neither `--end` nor `--count` is set, ingest follows the archive
    /// latest block indefinitely.
    #[arg(long, conflicts_with = "count")]
    end: Option<u64>,

    /// Number of blocks to ingest from the resolved start (i.e.
    /// `published_head + 1`). End block is derived as `start + count - 1`.
    /// Mutually exclusive with `--end`. When neither `--end` nor `--count` is
    /// set, ingest follows the archive latest block indefinitely.
    #[arg(long, conflicts_with = "end")]
    count: Option<u64>,

    /// Benchmark-only: allow a fresh store to start at an arbitrary block by
    /// seeding a synthetic predecessor from the first fetched block's parent
    /// hash. The resulting store is a suffix profile fixture, not a full-chain
    /// index.
    #[arg(long)]
    benchmark_synthetic_start: bool,

    /// Optional OTel collector endpoint for archive-side metrics. Off by
    /// default; archive readers still build without it.
    #[arg(long)]
    otel_endpoint: Option<String>,

    /// How often to log progress, in applied ingest batches.
    #[arg(long, default_value_t = 1000)]
    log_every: u64,

    /// Enable Tokio console task/resource instrumentation. Requires
    /// `--features tokio-console` plus `RUSTFLAGS=--cfg tokio_unstable`; the
    /// bench wrapper sets this automatically when passed --tokio-console.
    #[cfg(feature = "tokio-console")]
    #[arg(long)]
    tokio_console: bool,

    /// Maximum number of block fetches in flight against the archive
    /// concurrently. Each in-flight slot issues block + receipts + traces
    /// in parallel via `try_join!`, so peak archive request concurrency
    /// is roughly `3 * concurrency`. Increase for high-latency archives
    /// (e.g. S3); leave low for local FS where queueing buys nothing.
    /// Memory ceiling is bounded by this many fetched-but-not-yet-ingested
    /// blocks held in the prefetch buffer.
    #[arg(long, default_value_t = 5000)]
    concurrency: usize,

    /// Number of retry attempts after a fetch failure, per block. The
    /// first attempt is not counted, so `--max-retries 5` means up to 6
    /// total attempts. Set to 0 to disable retry. Only fetches retry —
    /// transform and ingest errors are deterministic and bail
    /// immediately.
    #[arg(long, default_value_t = 5)]
    max_retries: u32,

    /// Initial backoff between retry attempts, in milliseconds. Doubles
    /// after each failure (exponential, no jitter), so the default 200ms
    /// with 5 retries waits ~6.2s in total worst case.
    #[arg(long, default_value_t = 200)]
    retry_backoff_ms: u64,

    /// Disable adaptive concurrency. By default, `--concurrency` is the
    /// starting value and an AIMD controller scales it within
    /// `[--min-concurrency, --max-concurrency]` based on retry rate per window
    /// (halve above 5% retries, additive increase below 1%).
    #[arg(long = "no-autotune", action = clap::ArgAction::SetFalse, default_value_t = true)]
    autotune: bool,

    /// Lower bound on adaptive concurrency. Ignored with `--no-autotune`.
    #[arg(long, default_value_t = 250)]
    min_concurrency: usize,

    /// Upper bound on adaptive concurrency. Ignored with `--no-autotune`.
    #[arg(long, default_value_t = 15000)]
    max_concurrency: usize,

    /// Number of ordered fetch futures to keep buffered ahead of ingest.
    /// This is separate from `--concurrency`: the buffer may hold many
    /// pending/completed block fetches while the concurrency semaphore caps
    /// active archive requests.
    #[arg(long, default_value_t = 10000)]
    fetch_buffer: usize,

    /// Fetch pipeline shape. `semaphore-buffer` keeps `--fetch-buffer` ordered
    /// futures alive while a semaphore caps active archive reads at
    /// `--concurrency`. `bounded-channel` starts only `--concurrency` ordered
    /// fetch futures and materializes ordered results into a bounded channel
    /// sized by `--fetch-buffer`. `spawned-tasks` is like `bounded-channel`, but
    /// each active block fetch runs in its own Tokio task. `spawned-semaphore-buffer`
    /// combines large ordered lookahead, per-block Tokio tasks, and the active
    /// fetch semaphore.
    #[arg(long, value_enum, default_value_t = FetchStrategyArg::SpawnedSemaphoreBuffer)]
    fetch_strategy: FetchStrategyArg,

    /// Local bincode snapshot of the write-side open-index frontier. If the
    /// file exists, it is loaded before startup planning recovery. After a
    /// successful ingest, the updated frontier is written back to the same path.
    /// A stale snapshot is harmless: recovery still rebuilds when its recorded
    /// head does not match the current published head.
    #[arg(long)]
    open_index_snapshot: Option<PathBuf>,

    /// One-off maintenance mode: scan deterministic open-bitmap partitions
    /// below the published frontier and write any missing compacted page
    /// artifacts derivable from published fragments.
    #[arg(long, hide = true)]
    repair_bitmap_artifacts: bool,

    /// One-off maintenance mode: rewrite sealed-shard page-count manifests
    /// from durable compacted bitmap page artifacts. Format: log:<shard>,
    /// tx:<shard>, or trace:<shard>.
    #[arg(long, hide = true)]
    rewrite_bitmap_page_counts: Option<String>,

    /// Maximum number of consecutive blocks coalesced into one ingest batch.
    /// The fetch worker dispatches once `--min-batch-size` blocks are
    /// available, then drains only already-ready ordered blocks up to this
    /// limit rather than waiting for a full batch.
    #[arg(long = "max-batch-size", alias = "batch-size", default_value_t = 1000)]
    max_batch_size: usize,

    /// Minimum number of consecutive fetched blocks to collect before sending
    /// a batch to planning. Defaults to 1 so planning starts as soon as the
    /// next ordered block is ready. Must be <= `--max-batch-size`.
    #[arg(long, default_value_t = 1)]
    min_batch_size: usize,

    /// Number of fully planned ingest batches allowed to queue in front of
    /// the IO worker. This is the backpressure boundary between planning
    /// and durable writes.
    #[arg(long, default_value_t = 8)]
    plan_buffer: usize,

    /// Number of successfully written, unpublished batches allowed to queue
    /// in front of the publish worker.
    #[arg(long, default_value_t = 8)]
    write_buffer: usize,

    /// Number of durable write workers applying planned batches concurrently.
    /// Publication still advances only through contiguous successful batches.
    #[arg(long, default_value_t = 1)]
    write_workers: usize,

    /// Number of published batches allowed to queue in front of progress
    /// accounting/logging.
    #[arg(long, default_value_t = 16)]
    progress_buffer: usize,

    /// Number of retries for non-CAS meta/blob write failures. CAS is never
    /// retried; a CAS conflict means this writer lost the publication lease.
    #[arg(long, default_value_t = 5)]
    io_max_retries: usize,

    /// Retry non-CAS IO failures forever. Overrides --io-max-retries.
    #[arg(long)]
    io_retry_forever: bool,

    /// Initial non-CAS IO retry backoff, in milliseconds.
    #[arg(long, default_value_t = 200)]
    io_retry_backoff_ms: u64,

    /// Maximum non-CAS IO retry backoff, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    io_retry_max_backoff_ms: u64,

    /// fjall total-journal cap in MiB. Default 512 matches fjall's default;
    /// raise (e.g. 4096 or 8192) for high-throughput backfill to reduce
    /// journal-rotation pressure. Must be >= 64.
    #[arg(long, default_value_t = 512)]
    fjall_journal_mib: u64,

    /// Per-keyspace memtable cap in MiB. Default 64 matches fjall's default;
    /// raise (e.g. 256) so each keyspace amortizes more writes per flush.
    /// Total memtable footprint at steady state is roughly N_keyspaces * this.
    #[arg(long, default_value_t = 64)]
    fjall_memtable_mib: u64,

    /// fjall flush/compaction worker thread count. None lets fjall pick
    /// (min(CPU, 4)). Raise for many-core boxes under sustained write load.
    #[arg(long)]
    fjall_workers: Option<usize>,

    /// Per-table read-cache budget in MiB. Defaults to the budget implied by
    /// `CacheConfig::default()`; raising linearly scales every table's entry
    /// count. Sizing uses a per-table estimate of metadata + payload (bitmap
    /// fragments and bitmap page blobs dominate at ~8 KiB each; other tables
    /// are dominated by small metadata), so the actual cache footprint should
    /// roughly match the requested budget. `--cache-mib 0` disables caches
    /// entirely (compile-time skip).
    #[arg(long)]
    cache_mib: Option<usize>,

    /// Run as a reader only — never take write authority and never ingest.
    /// The ingest bin defaults to reader+writer; this flag is for running the
    /// binary purely to observe the published head without contending for the
    /// lease.
    #[arg(long)]
    reader_only: bool,

    /// Ingest as the sole writer: publish via a plain version-CAS on the head
    /// with no lease to acquire, renew, or release (and no `--owner-id` /
    /// `--lease-blocks` machinery). Use this only when exactly one process ever
    /// writes — e.g. a backfill — to skip the lease round-trips. Mutually
    /// exclusive with `--reader-only`.
    #[arg(long, conflicts_with = "reader_only")]
    single_writer: bool,

    /// Stable node identity for the write-authority lease. Two processes that
    /// must not both write share nothing *except* this when they are the same
    /// logical node across restarts; distinct nodes use distinct ids. A restart
    /// reuses `owner_id` but generates a fresh per-process session, so it always
    /// takes the reacquire (recovery) path.
    #[arg(long, default_value_t = 1)]
    owner_id: u64,

    /// Lease span in upstream finalized blocks. A lease acquired at observed
    /// block `b` is valid through `b + lease_blocks - 1`; a standby may take
    /// over once the observed upstream block passes that bound. Must be >= 1 and
    /// greater than `--renew-threshold-blocks`.
    #[arg(long, default_value_t = 64)]
    lease_blocks: u64,

    /// Renew the lease once `lease_valid_through_block - observed <=
    /// renew_threshold_blocks`. Must be strictly less than `--lease-blocks`.
    #[arg(long, default_value_t = 16)]
    renew_threshold_blocks: u64,

    /// Backend for the metadata store (primary directory, bitmap index,
    /// dictionaries, block records, and the publication-head CAS row).
    /// `fjall` keeps metadata in the local embedded LSM; `dynamo` stores it in a
    /// DynamoDB-API-compatible store (AWS DynamoDB or ScyllaDB Alternator).
    /// `dynamo` requires the binary to be built with `--features dynamo`.
    #[arg(long, value_enum, default_value_t = MetaBackendArg::Fjall)]
    meta_backend: MetaBackendArg,

    /// Backend for the blob store (compressed row payloads for each family).
    /// `fjall` keeps blobs in the local embedded LSM; `s3` stores them in an
    /// S3-API-compatible object store. `s3` requires the binary to be built with
    /// `--features s3`. Combine `--meta-backend dynamo --blob-backend s3` for a
    /// fully remote deployment with no local fjall DB.
    #[arg(long, value_enum, default_value_t = BlobBackendArg::Fjall)]
    blob_backend: BlobBackendArg,

    /// S3 bucket holding the blob objects. Required when `--blob-backend s3`.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_bucket: Option<String>,

    /// AWS region for the S3 blob bucket. Defaults to the ambient AWS region
    /// chain when unset.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_region: Option<String>,

    /// Override S3 endpoint URLs for compatible services (MinIO, R2, Ceph).
    /// Repeat to client-shard PUT/GET traffic across N endpoints. Leave unset
    /// to target real AWS S3.
    #[cfg(feature = "s3")]
    #[arg(long = "s3-endpoint-url")]
    s3_endpoint_urls: Vec<String>,

    /// Object-key prefix prepended to every blob object, e.g. `chain-data`.
    #[cfg(feature = "s3")]
    #[arg(long, default_value = "")]
    s3_prefix: String,

    /// Use path-style S3 addressing (`endpoint/bucket/key`). Required by
    /// MinIO/Ceph; leave off for real AWS S3 and R2.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_force_path_style: bool,

    /// Max in-flight S3 PUTs per ingest batch.
    #[cfg(feature = "s3")]
    #[arg(long, default_value_t = 64)]
    s3_max_concurrency: usize,

    /// Create the S3 bucket if it does not already exist before ingesting.
    /// Existing buckets owned by the caller are accepted. For real AWS S3,
    /// non-us-east-1 buckets are created with the configured --s3-region.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_create_bucket: bool,

    /// Static S3 access key id. Must be paired with --s3-secret-access-key.
    /// Leave both unset to use the ambient AWS credential chain.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_access_key_id: Option<String>,

    /// Static S3 secret access key. Must be paired with --s3-access-key-id.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_secret_access_key: Option<String>,

    /// DynamoDB/Alternator table holding metadata rows in the single-table
    /// layout. Required when `--meta-backend dynamo` and
    /// `--dynamo-table-layout=single`.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_table: Option<String>,

    /// Physical DynamoDB table layout for metadata. `single` maps every logical
    /// kv/scan/cas row to `--dynamo-table`; `per-logical-table` maps each
    /// logical table id to `{--dynamo-table-prefix}-{logical-table-name}`.
    #[cfg(feature = "dynamo")]
    #[arg(long, value_enum, default_value_t = DynamoTableLayoutArg::Single)]
    dynamo_table_layout: DynamoTableLayoutArg,

    /// Prefix for `--dynamo-table-layout=per-logical-table`.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_table_prefix: Option<String>,

    /// Apply Scylla Alternator-friendly Dynamo defaults: single physical table
    /// layout and both global/per-table BatchWrite concurrency set from
    /// `--scylla-concurrency`.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    scylla_profile: bool,

    /// BatchWrite concurrency used by `--scylla-profile`.
    #[cfg(feature = "dynamo")]
    #[arg(long, default_value_t = 256)]
    scylla_concurrency: usize,

    /// Override the DynamoDB endpoint URL for a compatible service: DynamoDB
    /// Local or ScyllaDB Alternator (point this at the Alternator port). Leave
    /// unset to target real AWS DynamoDB.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_endpoint_url: Option<String>,

    /// AWS region for the DynamoDB table. Defaults to the ambient AWS region
    /// chain when unset. Alternator accepts any value (commonly `us-east-1`).
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_region: Option<String>,

    /// Max in-flight BatchWriteItem calls per metadata write batch.
    #[cfg(feature = "dynamo")]
    #[arg(long, default_value_t = 256)]
    dynamo_max_concurrency: usize,

    /// Max in-flight BatchWriteItem calls per physical DynamoDB table.
    #[cfg(feature = "dynamo")]
    #[arg(long, default_value_t = 256)]
    dynamo_table_max_concurrency: usize,

    /// Create the DynamoDB/Alternator table if it does not already exist before
    /// ingesting. Convenience for dev/test; production tables are usually
    /// provisioned out of band.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_create_table: bool,

    /// Static DynamoDB access key id. Must be paired with
    /// --dynamo-secret-access-key. Leave both unset to use the ambient AWS
    /// credential chain. DynamoDB Local / Alternator accept any non-empty pair.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_access_key_id: Option<String>,

    /// Static DynamoDB secret access key. Must be paired with
    /// --dynamo-access-key-id.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_secret_access_key: Option<String>,

    /// Optional DynamoDB session token, used alongside the static credential
    /// pair above.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_session_token: Option<String>,

    /// DynamoDB/Alternator table holding chunked block blobs. Required when
    /// `--blob-backend dynamo`. May be the same physical table as
    /// `--dynamo-table` (the `"blob"` key-kind prefix keeps blob rows disjoint
    /// from metadata) or a dedicated table.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_blob_table: Option<String>,

    /// Bytes per blob chunk for `--blob-backend dynamo`. A wire contract: do not
    /// change for a table that already holds blobs. Defaults to 64 KiB.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_blob_chunk_size: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum MetaBackendArg {
    Fjall,
    #[cfg(feature = "dynamo")]
    Dynamo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum BlobBackendArg {
    Fjall,
    #[cfg(feature = "s3")]
    S3,
    #[cfg(feature = "dynamo")]
    Dynamo,
}

/// A DynamoDB client shared from the meta backend to the blob backend so a
/// fully-dynamo deployment uses one connection pool. Resolves to the real SDK
/// client under the `dynamo` feature, and to `()` otherwise (the threaded value
/// is then always `None`), keeping the generic `dispatch_blob` signature
/// feature-agnostic without cfg-gating a parameter.
#[cfg(feature = "dynamo")]
type SharedDynamoClient = aws_sdk_dynamodb::Client;
#[cfg(not(feature = "dynamo"))]
type SharedDynamoClient = ();

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum FetchStrategyArg {
    SemaphoreBuffer,
    BoundedChannel,
    SpawnedTasks,
    SpawnedSemaphoreBuffer,
}

#[cfg(feature = "dynamo")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DynamoTableLayoutArg {
    Single,
    PerLogicalTable,
}

#[cfg(feature = "dynamo")]
fn effective_dynamo_max_concurrency(cli: &Cli) -> usize {
    if cli.scylla_profile {
        cli.scylla_concurrency
    } else {
        cli.dynamo_max_concurrency
    }
}

#[cfg(feature = "dynamo")]
fn effective_dynamo_table_max_concurrency(cli: &Cli) -> usize {
    if cli.scylla_profile {
        cli.scylla_concurrency
    } else {
        cli.dynamo_table_max_concurrency
    }
}

#[cfg(feature = "dynamo")]
fn effective_dynamo_table_layout(cli: &Cli) -> DynamoTableLayoutArg {
    if cli.scylla_profile {
        DynamoTableLayoutArg::Single
    } else {
        cli.dynamo_table_layout
    }
}

/// fjall data directories actually required by the chosen backends. A field is
/// `Some` only when the corresponding store is fjall-backed; a fully remote
/// deployment (dynamo meta + s3 blobs) leaves both `None` and touches no disk.
struct FjallDirs {
    meta: Option<PathBuf>,
    blob: Option<PathBuf>,
    /// True when meta and blob share one fjall `Database` (`--data-dir`). Only
    /// possible when both stores are fjall.
    same_dir: bool,
}

/// Backend handles + labels threaded into the pipeline for stats and logging.
/// The `*_fjall` handles feed `collect_fjall_stats` (which can only inspect
/// fjall keyspaces); they are `None` for remote backends. The labels are purely
/// for the "starting ingest" log line.
struct StoreContext {
    meta_fjall: Option<FjallStore>,
    blob_fjall: Option<FjallStore>,
    same_dir: bool,
    meta_backend: &'static str,
    blob_backend: &'static str,
}

struct FetchedBatch {
    last_block: u64,
    blocks: Vec<FinalizedBlock>,
    first_wait_ms: u64,
    fetch_batch_collect_ms: u64,
    ingest_started: Instant,
    fetched_ready_at: Instant,
}

struct PlannedBatch {
    seq: u64,
    last_block: u64,
    plan: IngestPlan,
    first_wait_ms: u64,
    fetch_batch_collect_ms: u64,
    plan_queue_wait_ms: u64,
    plan_ms: u64,
    ingest_started: Instant,
    planned_ready_at: Instant,
}

struct AppliedBatch {
    seq: u64,
    last_block: u64,
    outcomes: Vec<monad_chain_data::IngestOutcome>,
    timings: monad_chain_data::IngestBatchTimings,
    first_wait_ms: u64,
    fetch_batch_collect_ms: u64,
    plan_queue_wait_ms: u64,
    plan_ms: u64,
    io_queue_wait_ms: u64,
    io_apply_ms: u64,
    publish_queue_wait_ms: u64,
    publish_group_batches: u64,
    applied_ready_at: Instant,
    total_ingest_ms: u64,
}

struct WrittenBatch {
    seq: u64,
    last_block: u64,
    outcomes: Vec<monad_chain_data::IngestOutcome>,
    timings: monad_chain_data::IngestBatchTimings,
    publication: PublicationAdvance,
    first_wait_ms: u64,
    fetch_batch_collect_ms: u64,
    plan_queue_wait_ms: u64,
    plan_ms: u64,
    io_queue_wait_ms: u64,
    io_apply_ms: u64,
    ingest_started: Instant,
    written_ready_at: Instant,
}

type TimedFetchResult = Result<(u64, Block, BlockReceipts, BlockTraces, FetchTimings)>;

#[derive(Debug, Clone, Copy, Default)]
struct FetchTimings {
    total_ms: u64,
    block_ms: u64,
    receipts_ms: u64,
    traces_ms: u64,
}

/// Resolves the fjall directories required by the selected backends. A fjall
/// store needs a directory; a remote store (dynamo/s3) needs none. `--data-dir`
/// is the shared-DB shortcut and is only valid when both stores are fjall;
/// otherwise each fjall store takes its own `--meta-data-dir`/`--blob-data-dir`.
/// A dir flag set for a non-fjall store is rejected so misconfigurations fail
/// fast rather than silently opening an unused DB.
fn resolve_fjall_dirs(cli: &Cli, meta_is_fjall: bool, blob_is_fjall: bool) -> Result<FjallDirs> {
    if let Some(data_dir) = &cli.data_dir {
        if !(meta_is_fjall && blob_is_fjall) {
            bail!(
                "--data-dir shares one fjall DB and requires --meta-backend fjall and \
                 --blob-backend fjall; use --meta-data-dir/--blob-data-dir for mixed backends"
            );
        }
        return Ok(FjallDirs {
            meta: Some(data_dir.clone()),
            blob: Some(data_dir.clone()),
            same_dir: true,
        });
    }

    let meta = match (meta_is_fjall, &cli.meta_data_dir) {
        (true, Some(p)) => Some(p.clone()),
        (true, None) => bail!("--meta-backend fjall requires --meta-data-dir (or --data-dir)"),
        (false, Some(_)) => bail!("--meta-data-dir is set but --meta-backend is not fjall"),
        (false, None) => None,
    };
    let blob = match (blob_is_fjall, &cli.blob_data_dir) {
        (true, Some(p)) => Some(p.clone()),
        (true, None) => bail!("--blob-backend fjall requires --blob-data-dir (or --data-dir)"),
        (false, Some(_)) => bail!("--blob-data-dir is set but --blob-backend is not fjall"),
        (false, None) => None,
    };
    let same_dir = match (&meta, &blob) {
        (Some(meta), Some(blob)) => same_store_dir(meta, blob),
        _ => false,
    };
    Ok(FjallDirs {
        meta,
        blob,
        same_dir,
    })
}

fn data_dir_fresh(path: &Path) -> bool {
    !path.exists()
        || path
            .read_dir()
            .map(|mut d| d.next().is_none())
            .unwrap_or(true)
}

fn same_store_dir(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    #[cfg(feature = "tokio-console")]
    if cli.tokio_console {
        console_subscriber::init();
    }
    #[cfg(not(feature = "tokio-console"))]
    {
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(Level::INFO.into())
                    .from_env_lossy(),
            )
            .init();
    }
    #[cfg(feature = "tokio-console")]
    if !cli.tokio_console {
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(Level::INFO.into())
                    .from_env_lossy(),
            )
            .init();
    }

    // High concurrency + fjall's per-SST FDs blow past the default 1024
    // soft NOFILE limit. Bump to the hard limit before any sockets or
    // keyspace files open. Best-effort: a warning here just means the
    // operator should `ulimit -n` themselves.
    match raise_nofile_to_hard_limit() {
        Ok((from, to)) if from < to => {
            info!(from, to, "raised NOFILE soft limit to hard limit");
        }
        Ok(_) => {}
        Err(e) => warn!(
            error = %e,
            "failed to raise NOFILE soft limit; high --concurrency may exhaust file descriptors"
        ),
    }

    if cli.concurrency == 0 {
        bail!("--concurrency must be >= 1");
    }
    if matches!(cli.count, Some(0)) {
        bail!("--count must be >= 1");
    }
    if cli.max_batch_size == 0 {
        bail!("--max-batch-size must be >= 1");
    }
    if cli.min_batch_size == 0 {
        bail!("--min-batch-size must be >= 1");
    }
    if cli.min_batch_size > cli.max_batch_size {
        bail!("--min-batch-size must be <= --max-batch-size");
    }
    if cli.plan_buffer == 0 {
        bail!("--plan-buffer must be >= 1");
    }
    if cli.write_buffer == 0 {
        bail!("--write-buffer must be >= 1");
    }
    if cli.write_workers == 0 {
        bail!("--write-workers must be >= 1");
    }
    #[cfg(feature = "dynamo")]
    if cli.dynamo_max_concurrency == 0 {
        bail!("--dynamo-max-concurrency must be >= 1");
    }
    #[cfg(feature = "dynamo")]
    if cli.dynamo_table_max_concurrency == 0 {
        bail!("--dynamo-table-max-concurrency must be >= 1");
    }
    #[cfg(feature = "dynamo")]
    if cli.scylla_concurrency == 0 {
        bail!("--scylla-concurrency must be >= 1");
    }
    if cli.progress_buffer == 0 {
        bail!("--progress-buffer must be >= 1");
    }
    if cli.io_retry_max_backoff_ms < cli.io_retry_backoff_ms {
        bail!("--io-retry-max-backoff-ms must be >= --io-retry-backoff-ms");
    }
    if cli.fjall_journal_mib < 64 {
        bail!("--fjall-journal-mib must be >= 64");
    }
    if cli.autotune {
        if cli.min_concurrency == 0 {
            bail!("--min-concurrency must be >= 1");
        }
        if cli.max_concurrency < cli.min_concurrency {
            bail!(
                "--max-concurrency ({}) must be >= --min-concurrency ({})",
                cli.max_concurrency,
                cli.min_concurrency
            );
        }
        if cli.concurrency < cli.min_concurrency || cli.concurrency > cli.max_concurrency {
            bail!(
                "--concurrency ({}) must be within [--min-concurrency ({}), --max-concurrency ({})]",
                cli.concurrency,
                cli.min_concurrency,
                cli.max_concurrency
            );
        }
    }
    let meta_is_fjall = matches!(cli.meta_backend, MetaBackendArg::Fjall);
    let blob_is_fjall = matches!(cli.blob_backend, BlobBackendArg::Fjall);
    let dirs = resolve_fjall_dirs(&cli, meta_is_fjall, blob_is_fjall)?;

    // Open the fjall stores actually required by the chosen backends. A fully
    // remote deployment (dynamo meta + s3 blobs) opens nothing here and never
    // touches local disk. The returned handles also feed `collect_fjall_stats`,
    // so a backend with no fjall keyspaces simply contributes no stats.
    let (meta_fjall, blob_fjall) = if meta_is_fjall || blob_is_fjall {
        let tuning = FjallTuning {
            max_journaling_size_bytes: cli.fjall_journal_mib * 1024 * 1024,
            max_memtable_size_bytes: cli.fjall_memtable_mib * 1024 * 1024,
            worker_threads: cli.fjall_workers,
        };
        // fjall persists max_memtable_size per-keyspace at creation, so the
        // value baked in at first open wins on every subsequent reopen — the
        // flag silently does nothing on existing data dirs. Journal cap and
        // worker_threads are not persisted and always take effect.
        let meta_dir_fresh = dirs.meta.as_deref().map(data_dir_fresh);
        let blob_dir_fresh = dirs.blob.as_deref().map(data_dir_fresh);
        info!(
            fjall_journal_mib = cli.fjall_journal_mib,
            fjall_memtable_mib = cli.fjall_memtable_mib,
            fjall_workers = ?cli.fjall_workers,
            meta_data_dir = ?dirs.meta.as_ref().map(|p| p.display().to_string()),
            blob_data_dir = ?dirs.blob.as_ref().map(|p| p.display().to_string()),
            ?meta_dir_fresh,
            ?blob_dir_fresh,
            same_dir = dirs.same_dir,
            "fjall tuning"
        );
        if cli.fjall_memtable_mib != 64
            && (meta_dir_fresh == Some(false) || blob_dir_fresh == Some(false))
        {
            warn!(
                requested_memtable_mib = cli.fjall_memtable_mib,
                ?meta_dir_fresh,
                ?blob_dir_fresh,
                "--fjall-memtable-mib is persisted per-keyspace at first creation; existing data dirs will keep their baked-in values. Journal cap and worker_threads still apply."
            );
        }
        let meta_fjall = match &dirs.meta {
            Some(path) => Some(
                FjallStore::open(path, tuning)
                    .with_context(|| format!("opening fjall meta store at {}", path.display()))?,
            ),
            None => None,
        };
        let blob_fjall = match &dirs.blob {
            // A shared `--data-dir` puts blobs in the same Database as meta.
            Some(_) if dirs.same_dir => meta_fjall.clone(),
            Some(path) => Some(
                FjallStore::open(path, tuning)
                    .with_context(|| format!("opening fjall blob store at {}", path.display()))?,
            ),
            None => None,
        };
        (meta_fjall, blob_fjall)
    } else {
        info!("meta + blob backends are both remote; skipping local fjall setup");
        (None, None)
    };

    let baseline_cache = CacheConfig::default();
    let baseline_total_mib = baseline_cache.approx_total_mib().max(1);
    let cache_config = match cli.cache_mib {
        None => baseline_cache,
        Some(0) => baseline_cache.scale(0, 1),
        Some(target_mib) => baseline_cache.scale(target_mib, baseline_total_mib),
    };
    info!(
        cache_mib_requested = ?cli.cache_mib,
        baseline_total_mib,
        scaled_total_entries = cache_config.total_entries(),
        "cache config"
    );

    // Select the meta backend, then the blob backend, at runtime. Each concrete
    // backend pair is handed to the generic `run_ingest`, so the service's
    // `M`/`B` type parameters are the only thing that differs between arms — no
    // enum over the service type. Compression happens per row at the
    // family/codec layer, not in the store.
    match cli.meta_backend {
        MetaBackendArg::Fjall => {
            info!("meta backend: fjall");
            let meta_store = meta_fjall
                .clone()
                .expect("fjall meta store opened for --meta-backend fjall");
            dispatch_blob(
                &cli,
                meta_store,
                // Fjall meta backend: no dynamo client to share.
                None,
                meta_fjall,
                blob_fjall,
                dirs.same_dir,
                "fjall",
                cache_config,
            )
            .await
        }
        #[cfg(feature = "dynamo")]
        MetaBackendArg::Dynamo => {
            let meta_store = build_dynamo_meta_store(&cli).await?;
            // Offer the meta store's client to a dynamo blob backend so both
            // share one connection pool (clone is cheap; it is Arc-backed).
            let dynamo_client = Some(meta_store.client());
            dispatch_blob(
                &cli,
                meta_store,
                dynamo_client,
                None,
                blob_fjall,
                dirs.same_dir,
                "dynamo",
                cache_config,
            )
            .await
        }
    }
}

/// Selects the blob backend and drives the pipeline. Generic over the concrete
/// meta store `M` so the same body serves every meta backend; the chosen blob
/// store fixes `run_ingest`'s `B` type parameter.
#[allow(clippy::too_many_arguments)]
async fn dispatch_blob<M: MetaStoreCas>(
    cli: &Cli,
    meta_store: M,
    // `Some` only when the meta backend is dynamo: its client, offered to a
    // dynamo blob backend so the two share one connection pool.
    dynamo_client: Option<SharedDynamoClient>,
    meta_fjall: Option<FjallStore>,
    blob_fjall: Option<FjallStore>,
    same_dir: bool,
    meta_backend: &'static str,
    cache_config: CacheConfig,
) -> Result<()> {
    // Without the dynamo blob backend compiled in there is nothing to hand the
    // shared client to; bind it so it is never an unused-variable warning.
    let _ = &dynamo_client;
    match cli.blob_backend {
        BlobBackendArg::Fjall => {
            info!("blob backend: fjall");
            let blob_store = blob_fjall
                .clone()
                .expect("fjall blob store opened for --blob-backend fjall");
            let ctx = StoreContext {
                meta_fjall,
                blob_fjall,
                same_dir,
                meta_backend,
                blob_backend: "fjall",
            };
            run_ingest(cli, meta_store, blob_store, ctx, cache_config).await
        }
        #[cfg(feature = "s3")]
        BlobBackendArg::S3 => {
            let blob_store = build_s3_blob_store(cli).await?;
            let ctx = StoreContext {
                meta_fjall,
                // S3 blobs have no fjall keyspaces to report.
                blob_fjall: None,
                same_dir,
                meta_backend,
                blob_backend: "s3",
            };
            run_ingest(cli, meta_store, blob_store, ctx, cache_config).await
        }
        #[cfg(feature = "dynamo")]
        BlobBackendArg::Dynamo => {
            let blob_store = build_dynamo_blob_store(cli, dynamo_client).await?;
            let ctx = StoreContext {
                meta_fjall,
                // Dynamo blobs have no fjall keyspaces to report.
                blob_fjall: None,
                same_dir,
                meta_backend,
                blob_backend: "dynamo",
            };
            run_ingest(cli, meta_store, blob_store, ctx, cache_config).await
        }
    }
}

/// Builds the service over the chosen meta + blob backends and drives the full
/// ingest pipeline. Generic over both store types (`M`/`B`) so the same body
/// serves every backend combination without an enum over the service type.
///
/// `ctx` carries the optional fjall handles (`Some` only for fjall-backed
/// stores) that feed `collect_fjall_stats`, plus backend labels for logging.
async fn run_ingest<M: MetaStoreCas, B: monad_chain_data::store::BlobStore>(
    cli: &Cli,
    meta_store: M,
    blob_store: B,
    ctx: StoreContext,
    cache_config: CacheConfig,
) -> Result<()> {
    if cli.lease_blocks == 0 {
        bail!("--lease-blocks must be >= 1");
    }
    if cli.renew_threshold_blocks >= cli.lease_blocks {
        bail!(
            "--renew-threshold-blocks ({}) must be less than --lease-blocks ({})",
            cli.renew_threshold_blocks,
            cli.lease_blocks
        );
    }

    // Lease clock cell. Holds the latest observed upstream finalized block
    // (`u64::MAX` = unknown → the callback returns `None`, failing closed). It
    // is seeded from the archive's latest available block once the reader is
    // built, then refreshed by a background task that re-polls the archive
    // periodically (see the lease-clock refresh task below), so lease
    // renewal/expiry track a moving upstream head rather than a value frozen at
    // startup.
    let observed_upstream = Arc::new(AtomicU64::new(u64::MAX));
    let service = if cli.reader_only {
        info!("--reader-only set: this process will not take write authority or ingest");
        Arc::new(MonadChainDataService::new_reader_only(
            meta_store.clone(),
            blob_store,
            QueryLimits::UNLIMITED,
            cache_config,
        ))
    } else if cli.single_writer {
        info!("single-writer mode: version-CAS publish, no lease to acquire/renew/release");
        Arc::new(MonadChainDataService::with_cache_config(
            meta_store.clone(),
            blob_store,
            QueryLimits::UNLIMITED,
            cache_config,
        ))
    } else {
        let observe = observed_upstream.clone();
        let observe_upstream: monad_chain_data::ObserveUpstream =
            Arc::new(move || match observe.load(Ordering::SeqCst) {
                u64::MAX => None,
                block => Some(block),
            });
        info!(
            owner_id = cli.owner_id,
            lease_blocks = cli.lease_blocks,
            renew_threshold_blocks = cli.renew_threshold_blocks,
            "reader+writer mode: acquiring write authority lease"
        );
        Arc::new(MonadChainDataService::new_reader_writer_with_cache_config(
            meta_store.clone(),
            blob_store,
            QueryLimits::UNLIMITED,
            cache_config,
            cli.owner_id,
            cli.lease_blocks,
            cli.renew_threshold_blocks,
            observe_upstream,
        ))
    };

    if cli.reader_only {
        let head = service
            .publication()
            .load_published_head()
            .await
            .context("loading current publication head")?;
        info!(?head, "reader-only: current published head");
        return Ok(());
    }

    if cli.repair_bitmap_artifacts {
        let report = service
            .repair_missing_bitmap_artifacts()
            .await
            .context("repairing missing bitmap artifacts")?;
        info!(
            published_head = ?report.published_head,
            scanned_page_partitions = report.scanned_page_partitions,
            open_stream_markers = report.open_stream_markers,
            existing_artifacts = report.existing_artifacts,
            repaired_artifacts = report.repaired_artifacts,
            rewritten_page_count_manifests = report.rewritten_page_count_manifests,
            "bitmap artifact repair completed"
        );
        println!(
            "bitmap artifact repair completed: head={:?} scanned_page_partitions={} open_stream_markers={} existing_artifacts={} repaired_artifacts={} rewritten_page_count_manifests={}",
            report.published_head,
            report.scanned_page_partitions,
            report.open_stream_markers,
            report.existing_artifacts,
            report.repaired_artifacts,
            report.rewritten_page_count_manifests
        );
        return Ok(());
    }

    if let Some(spec) = &cli.rewrite_bitmap_page_counts {
        let (family, shard) = parse_family_shard(spec)?;
        let report = service
            .rewrite_bitmap_page_count_manifests(family, shard)
            .await
            .with_context(|| format!("rewriting bitmap page counts for {spec}"))?;
        info!(
            family = ?family,
            shard,
            scanned_page_partitions = report.scanned_page_partitions,
            open_stream_markers = report.open_stream_markers,
            loaded_artifacts = report.loaded_artifacts,
            rewritten_page_count_manifests = report.rewritten_page_count_manifests,
            "bitmap page count manifest rewrite completed"
        );
        println!(
            "bitmap page count manifest rewrite completed: family={family:?} shard={shard} scanned_page_partitions={} open_stream_markers={} loaded_artifacts={} rewritten_page_count_manifests={}",
            report.scanned_page_partitions,
            report.open_stream_markers,
            report.loaded_artifacts,
            report.rewritten_page_count_manifests,
        );
        return Ok(());
    }

    // Resolve the block range against the current publication head before
    // any archive I/O, so a misconfigured range fails fast rather than
    // burning a fetch.
    let head = service
        .publication()
        .load_published_head()
        .await
        .context("loading current publication head")?;
    let expected_start = head.map_or(1, |h| h + 1);
    let start = cli.start.unwrap_or(expected_start);
    if cli.benchmark_synthetic_start {
        if head.is_some() {
            bail!(
                "--benchmark-synthetic-start requires an empty publication head (current head: {:?})",
                head
            );
        }
        if cli.start.is_none() || start <= 1 {
            bail!("--benchmark-synthetic-start requires explicit --start > 1");
        }
        warn!(
            start,
            "benchmark synthetic start enabled: seeding a suffix-only ingest fixture; output is not a full-chain index"
        );
    } else if start != expected_start {
        bail!(
            "start={} does not match expected next block {} (current head: {:?})",
            start,
            expected_start,
            head
        );
    }
    let end = match (cli.end, cli.count) {
        (Some(e), None) => Some(e),
        (None, Some(c)) => {
            let Some(e) = start.checked_add(c - 1) else {
                bail!("start + count - 1 overflows u64 (start={start}, count={c})");
            };
            Some(e)
        }
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("clap enforces --end conflicts with --count"),
    };
    if let Some(end) = end {
        if start > end {
            bail!("start ({}) must be <= end ({})", start, end);
        }
    }

    let metrics_started = Instant::now();
    let metrics = Metrics::new(
        cli.otel_endpoint.as_deref(),
        "chain-data-ingest",
        "0".to_string(),
        Duration::from_secs(60),
    )
    .context("building metrics")?;
    info!(
        elapsed_ms = metrics_started.elapsed().as_millis() as u64,
        "startup validation: metrics initialized"
    );
    let reader_started = Instant::now();
    let reader = cli
        .block_data_source
        .build(&metrics)
        .await
        .context("building block data reader")?;
    info!(
        elapsed_ms = reader_started.elapsed().as_millis() as u64,
        "startup validation: block data reader is ready"
    );

    // Seed the lease clock from the archive's latest available finalized
    // block. The lease decisions are made against this observed upstream head;
    // when the source has no current head the cell stays `u64::MAX` (unknown),
    // so the authority fails closed rather than acting on a stale clock.
    let latest_started = Instant::now();
    match reader
        .get_latest(LatestKind::Uploaded)
        .await
        .context("reading archive latest available block")?
    {
        Some(latest) => {
            observed_upstream.store(latest, Ordering::SeqCst);
            info!(
                latest,
                elapsed_ms = latest_started.elapsed().as_millis() as u64,
                "seeded lease clock from archive latest available block"
            );
        }
        None => warn!(
            elapsed_ms = latest_started.elapsed().as_millis() as u64,
            "archive reports no latest available block; the write lease will fail closed until a block is observed"
        ),
    }

    // Lease-clock refresh task. The seed above is a point-in-time snapshot; for
    // a long-running ingest the upstream head advances, so re-poll the archive
    // periodically and publish the max into the clock cell. `fetch_max` keeps
    // the observation monotonic (a transient lower/None reading never rewinds
    // the clock). Aborted once the ingest pipeline drains.
    const LEASE_CLOCK_REFRESH: Duration = Duration::from_secs(30);
    let clock_refresh_handle = {
        let reader = reader.clone();
        let observed_upstream = observed_upstream.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(LEASE_CLOCK_REFRESH);
            // The first tick fires immediately; skip it since we just seeded.
            tick.tick().await;
            loop {
                tick.tick().await;
                match reader.get_latest(LatestKind::Uploaded).await {
                    Ok(Some(latest)) => {
                        observed_upstream.fetch_max(latest, Ordering::SeqCst);
                    }
                    Ok(None) => {}
                    Err(error) => warn!(
                        %error,
                        "lease-clock refresh: failed to re-poll archive latest; keeping last observation"
                    ),
                }
            }
        })
    };

    // Concurrency controller. When `--no-autotune` is set, min == max == initial
    // and wall-clock fetch-side adjustments become no-ops, so the controller
    // degenerates into a fixed semaphore equivalent to the old `buffered(N)`.
    let initial_concurrency = cli.concurrency;
    let (min_concurrency, max_concurrency) = if cli.autotune {
        (cli.min_concurrency, cli.max_concurrency)
    } else {
        (initial_concurrency, initial_concurrency)
    };
    let control = Arc::new(ConcurrencyControl::new(
        initial_concurrency,
        min_concurrency,
        max_concurrency,
    ));

    if let Some(snapshot_path) = &cli.open_index_snapshot {
        let snapshot_started = Instant::now();
        match std::fs::read(snapshot_path) {
            Ok(bytes) => {
                let status = service
                    .load_open_index_snapshot_bytes(&bytes)
                    .with_context(|| {
                        format!("loading open-index snapshot {}", snapshot_path.display())
                    })?;
                info!(
                    path = %snapshot_path.display(),
                    rebuilt_for_head = ?status.rebuilt_for_head,
                    directory_keys = status.stats.directory_keys,
                    directory_blocks = status.stats.directory_blocks,
                    bitmap_stream_pages = status.stats.bitmap_stream_pages,
                    bitmap_blocks = status.stats.bitmap_blocks,
                    bitmap_open_pages = status.stats.bitmap_open_pages,
                    bitmap_open_streams = status.stats.bitmap_open_streams,
                    approx_bytes = status.stats.approx_bytes,
                    elapsed_ms = snapshot_started.elapsed().as_millis() as u64,
                    "startup validation: loaded open-index snapshot"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    path = %snapshot_path.display(),
                    elapsed_ms = snapshot_started.elapsed().as_millis() as u64,
                    "startup validation: open-index snapshot not found; recovery may rebuild"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reading open-index snapshot {}", snapshot_path.display())
                });
            }
        }
    }

    if cli.benchmark_synthetic_start {
        warn!(
            "startup validation: deferring ingest planning state preparation until first fetched block seeds synthetic predecessor"
        );
    } else {
        let planning_prepare_started = Instant::now();
        info!("startup validation: preparing ingest planning state");
        service
            .prepare_ingest_planning_state()
            .await
            .context("preparing ingest planning state")?;
        info!(
            elapsed_ms = planning_prepare_started.elapsed().as_millis() as u64,
            "startup validation: ingest planning state is ready"
        );
    }

    let fetch_buffer = cli.fetch_buffer;
    if fetch_buffer == 0 {
        bail!("--fetch-buffer must be >= 1");
    }
    if fetch_buffer < max_concurrency {
        warn!(
            fetch_buffer,
            max_concurrency,
            "--fetch-buffer is below the maximum fetch concurrency; active concurrency may be capped by the smaller buffer"
        );
    }
    let estimated_fetch_lookahead_bound_blocks = fetch_buffer as u64
        + max_concurrency as u64
        + cli.max_batch_size as u64
            * (cli.plan_buffer as u64
                + cli.write_buffer as u64
                + cli.write_workers as u64
                + cli.progress_buffer as u64);
    info!(
        start,
        end = ?end,
        follow = end.is_none(),
        concurrency = cli.concurrency,
        fetch_buffer,
        estimated_fetch_lookahead_bound_blocks,
        max_batch_size = cli.max_batch_size,
        min_batch_size = cli.min_batch_size,
        plan_buffer = cli.plan_buffer,
        write_buffer = cli.write_buffer,
        write_workers = cli.write_workers,
        progress_buffer = cli.progress_buffer,
        io_retry_forever = cli.io_retry_forever,
        io_max_retries = cli.io_max_retries,
        meta_backend = ctx.meta_backend,
        blob_backend = ctx.blob_backend,
        fetch_strategy = ?cli.fetch_strategy,
        "starting ingest"
    );

    let log_every_batches = cli.log_every.max(1);
    // Wall-clock cap on window length. When throughput collapses (e.g.
    // throttled at high concurrency) the modulus trigger can take an
    // hour to fire; the wall-clock fallback keeps feedback flowing.
    const WINDOW_MAX_WALL: Duration = Duration::from_secs(30);

    let fetch_stats = Arc::new(Mutex::new(FetchStats::default()));
    let fetch_progress = Arc::new(FetchProgress::default());
    let pipeline_progress = Arc::new(PipelineProgress::default());
    let (fetched_tx, mut fetched_rx) = mpsc::channel::<FetchedBatch>(cli.plan_buffer);
    let (planned_tx, planned_rx) = mpsc::channel::<PlannedBatch>(cli.plan_buffer);
    let (written_tx, mut written_rx) = mpsc::channel::<WrittenBatch>(cli.write_buffer);
    let (applied_tx, mut applied_rx) = mpsc::channel::<AppliedBatch>(cli.progress_buffer);
    let fetch_consumed_total = Arc::new(AtomicU64::new(0));
    let ingest_heartbeat_handle = {
        let fetch_progress = fetch_progress.clone();
        let pipeline_progress = pipeline_progress.clone();
        let fetch_consumed_total = fetch_consumed_total.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WINDOW_MAX_WALL);
            tick.tick().await;
            loop {
                tick.tick().await;
                let snapshot = fetch_progress.snapshot();
                let pipeline = pipeline_progress.snapshot();
                let consumed = fetch_consumed_total.load(Ordering::Relaxed);
                let completed_backlog = snapshot.completed.saturating_sub(consumed);
                let in_flight = snapshot.started.saturating_sub(snapshot.completed);
                info!(
                    fetch_started_total = snapshot.started,
                    fetch_completed_total = snapshot.completed,
                    fetch_consumed_total = consumed,
                    fetch_completed_backlog = completed_backlog,
                    fetch_in_flight = in_flight,
                    planned_batches_total = pipeline.planned,
                    write_started_batches_total = pipeline.write_started,
                    write_apply_completed_batches_total = pipeline.write_apply_completed,
                    write_sent_to_publisher_batches_total = pipeline.write_sent_to_publisher,
                    publish_started_groups_total = pipeline.publish_started,
                    publish_completed_groups_total = pipeline.publish_completed,
                    published_batches_total = pipeline.published_batches,
                    applied_sent_batches_total = pipeline.applied_sent,
                    "ingest heartbeat"
                );
            }
        })
    };

    let fetch_control_handle = {
        let fetch_progress = fetch_progress.clone();
        let control = control.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(FETCH_CONTROL_TICK);
            tick.tick().await;
            let mut previous = fetch_progress.snapshot();
            loop {
                tick.tick().await;
                let current = fetch_progress.snapshot();
                let started_delta = current.started.saturating_sub(previous.started);
                let completed_delta = current.completed.saturating_sub(previous.completed);
                let retry_delta = current
                    .retry_attempts
                    .saturating_sub(previous.retry_attempts);
                let in_flight = current.started.saturating_sub(current.completed);
                let denominator = completed_delta.max(started_delta).max(1);
                let retry_rate = retry_delta as f64 / denominator as f64;
                let before = control.current();
                let after = control.adjust_from_fetch_window(
                    retry_rate,
                    completed_delta,
                    started_delta,
                    in_flight,
                );
                if after != before {
                    info!(
                        from = before,
                        to = after,
                        retry_rate = round_2(retry_rate),
                        retry_attempts = retry_delta,
                        fetch_started = started_delta,
                        fetch_completed = completed_delta,
                        fetch_in_flight = in_flight,
                        "fetch autotune: adjusted concurrency"
                    );
                } else {
                    debug!(
                        concurrency = after,
                        retry_rate = round_2(retry_rate),
                        retry_attempts = retry_delta,
                        fetch_started = started_delta,
                        fetch_completed = completed_delta,
                        fetch_in_flight = in_flight,
                        "fetch autotune: held concurrency"
                    );
                }
                previous = current;
            }
        })
    };

    let fetch_handle = {
        let reader = reader.clone();
        let fetch_stats = fetch_stats.clone();
        let fetch_progress = fetch_progress.clone();
        let permits = control.permits();
        let fetched_tx = fetched_tx.clone();
        let fetch_consumed_total = fetch_consumed_total.clone();
        let max_retries = cli.max_retries;
        let initial_backoff = Duration::from_millis(cli.retry_backoff_ms);
        let max_batch_size = cli.max_batch_size;
        let min_batch_size = cli.min_batch_size;
        let fetch_strategy = cli.fetch_strategy;
        tokio::spawn(async move {
            let (ordered_tx, mut ordered_rx) = mpsc::channel::<TimedFetchResult>(fetch_buffer);
            let produce_handle = match fetch_strategy {
                FetchStrategyArg::SemaphoreBuffer => {
                    let reader = reader.clone();
                    let fetch_stats = fetch_stats.clone();
                    let fetch_progress = fetch_progress.clone();
                    let permits = permits.clone();
                    let ordered_tx = ordered_tx.clone();
                    tokio::spawn(async move {
                        // Existing strategy: large ordered lookahead plus a
                        // semaphore that caps active archive reads.
                        let fetch_stream = block_number_stream(reader.clone(), start, end)
                            .map(move |n| {
                                let reader = reader.clone();
                                let stats = fetch_stats.clone();
                                let progress = fetch_progress.clone();
                                let permits = permits.clone();
                                async move {
                                    let _permit = permits
                                        .acquire_owned()
                                        .await
                                        .expect("concurrency semaphore should never close");
                                    fetch_block_with_retry(
                                        &reader,
                                        n,
                                        max_retries,
                                        initial_backoff,
                                        &stats,
                                        &progress,
                                    )
                                    .await
                                }
                            })
                            .buffered(fetch_buffer);
                        futures::pin_mut!(fetch_stream);
                        while let Some(item) = fetch_stream.next().await {
                            if ordered_tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    })
                }
                FetchStrategyArg::BoundedChannel => {
                    let reader = reader.clone();
                    let fetch_stats = fetch_stats.clone();
                    let fetch_progress = fetch_progress.clone();
                    let permits = permits.clone();
                    let ordered_tx = ordered_tx.clone();
                    tokio::spawn(async move {
                        // Ordered outputs materialize into the bounded
                        // fetch-buffer channel while the adaptive semaphore
                        // gates active archive reads.
                        let fetch_stream = block_number_stream(reader.clone(), start, end)
                            .map(move |n| {
                                let reader = reader.clone();
                                let stats = fetch_stats.clone();
                                let progress = fetch_progress.clone();
                                let permits = permits.clone();
                                async move {
                                    let _permit = permits
                                        .acquire_owned()
                                        .await
                                        .expect("concurrency semaphore should never close");
                                    fetch_block_with_retry(
                                        &reader,
                                        n,
                                        max_retries,
                                        initial_backoff,
                                        &stats,
                                        &progress,
                                    )
                                    .await
                                }
                            })
                            .buffered(fetch_buffer);
                        futures::pin_mut!(fetch_stream);
                        while let Some(item) = fetch_stream.next().await {
                            if ordered_tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    })
                }
                FetchStrategyArg::SpawnedTasks => {
                    let reader = reader.clone();
                    let fetch_stats = fetch_stats.clone();
                    let fetch_progress = fetch_progress.clone();
                    let permits = permits.clone();
                    let ordered_tx = ordered_tx.clone();
                    tokio::spawn(async move {
                        // Spawn each active block fetch so archive reader
                        // polling/decode work can run across Tokio workers,
                        // then drain ordered results into the bounded read
                        // buffer before the batch builder. The permit is
                        // acquired before spawning so fetch_buffer does not
                        // turn into one Tokio task per looked-ahead block.
                        let fetch_stream = block_number_stream(reader.clone(), start, end)
                            .map(move |n| {
                                let reader = reader.clone();
                                let stats = fetch_stats.clone();
                                let progress = fetch_progress.clone();
                                let permits = permits.clone();
                                async move {
                                    let permit = permits
                                        .acquire_owned()
                                        .await
                                        .expect("concurrency semaphore should never close");
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        fetch_block_with_retry(
                                            &reader,
                                            n,
                                            max_retries,
                                            initial_backoff,
                                            &stats,
                                            &progress,
                                        )
                                        .await
                                    })
                                    .await
                                }
                            })
                            .buffered(fetch_buffer);
                        futures::pin_mut!(fetch_stream);
                        while let Some(item) = fetch_stream.next().await {
                            let item = match item {
                                Ok(Ok(item)) => Ok(item),
                                Ok(Err(e)) => Err(e),
                                Err(e) => Err(eyre!("spawned fetch task panicked: {e}")),
                            };
                            if ordered_tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    })
                }
                FetchStrategyArg::SpawnedSemaphoreBuffer => {
                    let reader = reader.clone();
                    let fetch_stats = fetch_stats.clone();
                    let fetch_progress = fetch_progress.clone();
                    let permits = permits.clone();
                    let ordered_tx = ordered_tx.clone();
                    tokio::spawn(async move {
                        // Hybrid strategy: cap active spawned block tasks with
                        // the concurrency window, move the semaphore permit
                        // into each task, and let the bounded ordered channel
                        // provide read-ahead without creating a task per
                        // buffered block.
                        let fetch_stream = block_number_stream(reader.clone(), start, end)
                            .map(move |n| {
                                let reader = reader.clone();
                                let stats = fetch_stats.clone();
                                let progress = fetch_progress.clone();
                                let permits = permits.clone();
                                async move {
                                    let permit = permits
                                        .acquire_owned()
                                        .await
                                        .expect("concurrency semaphore should never close");
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        fetch_block_with_retry(
                                            &reader,
                                            n,
                                            max_retries,
                                            initial_backoff,
                                            &stats,
                                            &progress,
                                        )
                                        .await
                                    })
                                    .await
                                }
                            })
                            .buffered(fetch_buffer);
                        futures::pin_mut!(fetch_stream);
                        while let Some(item) = fetch_stream.next().await {
                            let item = match item {
                                Ok(Ok(item)) => Ok(item),
                                Ok(Err(e)) => Err(e),
                                Err(e) => Err(eyre!("spawned fetch task panicked: {e}")),
                            };
                            if ordered_tx.send(item).await.is_err() {
                                break;
                            }
                        }
                    })
                }
            };
            drop(ordered_tx);

            let mut pending: Vec<(u64, FinalizedBlock)> = Vec::with_capacity(max_batch_size);
            loop {
                pending.clear();
                let wait_started = Instant::now();
                let Some(first_item) = ordered_rx.recv().await else {
                    break;
                };
                let first_wait_ms = wait_started.elapsed().as_millis() as u64;
                let push_fetched = |item: TimedFetchResult,
                                    pending: &mut Vec<(u64, FinalizedBlock)>|
                 -> Result<()> {
                    let (n, block, receipts, traces, _fetch_timings) = item?;
                    fetch_consumed_total.fetch_add(1, Ordering::Relaxed);
                    let transform_started = Instant::now();
                    let finalized = into_finalized_block(block, receipts, traces)
                        .with_context(|| format!("transforming block {n}"))?;
                    fetch_stats
                        .lock()
                        .expect("fetch stats poisoned")
                        .transform_ms
                        .push(transform_started.elapsed().as_millis() as u64);
                    pending.push((n, finalized));
                    Ok(())
                };

                push_fetched(first_item, &mut pending)?;
                while pending.len() < min_batch_size {
                    let Some(item) = ordered_rx.recv().await else {
                        break;
                    };
                    push_fetched(item, &mut pending)?;
                }
                while pending.len() < max_batch_size {
                    match ordered_rx.try_recv() {
                        Ok(item) => push_fetched(item, &mut pending)?,
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                let last_block = pending.last().expect("pending non-empty").0;
                let blocks = pending.iter().map(|(_, b)| b.clone()).collect();
                let fetched_ready_at = Instant::now();
                fetched_tx
                    .send(FetchedBatch {
                        last_block,
                        blocks,
                        first_wait_ms,
                        fetch_batch_collect_ms: wait_started.elapsed().as_millis() as u64,
                        ingest_started: fetched_ready_at,
                        fetched_ready_at,
                    })
                    .await
                    .map_err(|_| eyre!("planning worker stopped before fetch completed"))?;
            }
            produce_handle
                .await
                .map_err(|e| eyre!("fetch producer panicked: {e}"))?;
            Ok::<_, eyre::Report>(())
        })
    };

    drop(fetched_tx);

    let plan_handle = {
        let service = service.clone();
        let pipeline_progress = pipeline_progress.clone();
        let benchmark_synthetic_start = cli.benchmark_synthetic_start;
        tokio::spawn(async move {
            let mut next_plan_seq = 0u64;
            while let Some(batch) = fetched_rx.recv().await {
                let plan_queue_wait_ms = batch.fetched_ready_at.elapsed().as_millis() as u64;
                if benchmark_synthetic_start && next_plan_seq == 0 {
                    let first = batch
                        .blocks
                        .first()
                        .expect("fetch worker never sends empty batches");
                    service
                        .seed_synthetic_ingest_start(first.block_number(), first.parent_hash())
                        .await
                        .with_context(|| {
                            format!(
                                "seeding synthetic ingest predecessor for block {}",
                                first.block_number()
                            )
                        })?;
                    warn!(
                        start = first.block_number(),
                        synthetic_head = first.block_number() - 1,
                        "seeded benchmark synthetic ingest predecessor"
                    );
                }
                let plan_started = Instant::now();
                info!(
                    seq = next_plan_seq,
                    last_block = batch.last_block,
                    blocks = batch.blocks.len(),
                    plan_queue_wait_ms,
                    first_wait_ms = batch.first_wait_ms,
                    fetch_batch_collect_ms = batch.fetch_batch_collect_ms,
                    "planning worker starting batch"
                );
                let plan = service
                    .plan_ingest_blocks(batch.blocks)
                    .await
                    .with_context(|| {
                        format!("planning batch ending at block {}", batch.last_block)
                    })?
                    .expect("fetch worker never sends empty batches");
                let planned_ready_at = Instant::now();
                info!(
                    seq = next_plan_seq,
                    last_block = batch.last_block,
                    plan_ms = plan_started.elapsed().as_millis() as u64,
                    stage_a_ms = plan.timings.stage_a_ms,
                    reads_ms = plan.timings.reads_ms,
                    stage_b_ms = plan.timings.stage_b_ms,
                    phase_b_skipped = plan.timings.phase_b_skipped,
                    phase_a_write_ops = plan.timings.phase_a_write_counts.total_ops(),
                    phase_b_write_ops = plan.timings.phase_b_write_counts.total_ops(),
                    "planning worker sending planned batch"
                );
                planned_tx
                    .send(PlannedBatch {
                        seq: next_plan_seq,
                        last_block: batch.last_block,
                        plan,
                        first_wait_ms: batch.first_wait_ms,
                        fetch_batch_collect_ms: batch.fetch_batch_collect_ms,
                        plan_queue_wait_ms,
                        plan_ms: plan_started.elapsed().as_millis() as u64,
                        ingest_started: batch.ingest_started,
                        planned_ready_at,
                    })
                    .await
                    .map_err(|_| eyre!("IO worker stopped before planning completed"))?;
                next_plan_seq = next_plan_seq.saturating_add(1);
                pipeline_progress.planned.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<_, eyre::Report>(())
        })
    };

    let io_retry = if cli.io_retry_forever {
        IoRetryPolicy::infinite(
            Duration::from_millis(cli.io_retry_backoff_ms),
            Duration::from_millis(cli.io_retry_max_backoff_ms),
        )
    } else {
        IoRetryPolicy::bounded(
            cli.io_max_retries,
            Duration::from_millis(cli.io_retry_backoff_ms),
            Duration::from_millis(cli.io_retry_max_backoff_ms),
        )
    };
    let planned_rx = Arc::new(TokioMutex::new(planned_rx));
    let mut io_handles = Vec::with_capacity(cli.write_workers);
    for worker_idx in 0..cli.write_workers {
        let service = service.clone();
        let pipeline_progress = pipeline_progress.clone();
        let planned_rx = planned_rx.clone();
        let written_tx = written_tx.clone();
        io_handles.push(tokio::spawn(async move {
            loop {
                let batch = {
                    let mut planned_rx = planned_rx.lock().await;
                    planned_rx.recv().await
                };
                let Some(batch) = batch else {
                    break;
                };
                pipeline_progress
                    .write_started
                    .fetch_add(1, Ordering::Relaxed);
                let io_queue_wait_ms = batch.planned_ready_at.elapsed().as_millis() as u64;
                info!(
                    worker_idx,
                    seq = batch.seq,
                    last_block = batch.last_block,
                    io_queue_wait_ms,
                    phase_a_write_ops = batch.plan.timings.phase_a_write_counts.total_ops(),
                    phase_a_write_bytes = batch.plan.timings.phase_a_write_counts.total_bytes(),
                    phase_a_write_ops_by_table = %batch.plan.timings.phase_a_write_counts.top_by_ops(8),
                    phase_b_write_ops = batch.plan.timings.phase_b_write_counts.total_ops(),
                    phase_b_write_bytes = batch.plan.timings.phase_b_write_counts.total_bytes(),
                    phase_b_write_ops_by_table = %batch.plan.timings.phase_b_write_counts.top_by_ops(8),
                    "write worker starting staged write apply"
                );
                let io_apply_started = Instant::now();
                let (outcomes, timings, publication) = service
                    .apply_ingest_writes_with_retry(batch.plan, io_retry)
                    .await
                    .with_context(|| {
                        format!(
                            "write worker {worker_idx} applying writes for batch {} ending at block {}",
                            batch.seq, batch.last_block
                        )
                    })?;
                let io_apply_ms = io_apply_started.elapsed().as_millis() as u64;
                info!(
                    worker_idx,
                    seq = batch.seq,
                    last_block = batch.last_block,
                    io_apply_ms,
                    "write worker completed staged write apply"
                );
                pipeline_progress
                    .write_apply_completed
                    .fetch_add(1, Ordering::Relaxed);
                written_tx
                    .send(WrittenBatch {
                        seq: batch.seq,
                        last_block: batch.last_block,
                        outcomes,
                        timings,
                        publication,
                        first_wait_ms: batch.first_wait_ms,
                        fetch_batch_collect_ms: batch.fetch_batch_collect_ms,
                        plan_queue_wait_ms: batch.plan_queue_wait_ms,
                        plan_ms: batch.plan_ms,
                        io_queue_wait_ms,
                        io_apply_ms,
                        ingest_started: batch.ingest_started,
                        written_ready_at: Instant::now(),
                    })
                    .await
                    .map_err(|_| eyre!("publisher stopped before IO completed"))?;
                pipeline_progress
                    .write_sent_to_publisher
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok::<_, eyre::Report>(())
        }));
    }
    drop(written_tx);
    let publisher_handle = {
        let service = service.clone();
        let pipeline_progress = pipeline_progress.clone();
        tokio::spawn(async move {
            let mut completed_writes: BTreeMap<u64, WrittenBatch> = BTreeMap::new();
            let mut next_publish_seq = 0u64;
            while let Some(first) = written_rx.recv().await {
                completed_writes.insert(first.seq, first);
                while let Ok(written) = written_rx.try_recv() {
                    completed_writes.insert(written.seq, written);
                }
                debug!(
                    buffered_written_batches = completed_writes.len(),
                    next_publish_seq, "publisher received written batch"
                );
                let mut pending_publish: Vec<WrittenBatch> = Vec::new();
                while let Some(written) = completed_writes.remove(&next_publish_seq) {
                    pending_publish.push(written);
                    next_publish_seq = next_publish_seq.saturating_add(1);
                }
                if pending_publish.is_empty() {
                    continue;
                }
                let publication = pending_publish
                    .last()
                    .expect("pending publish non-empty")
                    .publication
                    .clone();
                let publish_started = Instant::now();
                info!(
                    publish_group_batches = pending_publish.len(),
                    publish_last_seq = pending_publish
                        .last()
                        .expect("pending publish non-empty")
                        .seq,
                    publish_last_block = pending_publish
                        .last()
                        .expect("pending publish non-empty")
                        .last_block,
                    "publisher starting publication advance"
                );
                pipeline_progress
                    .publish_started
                    .fetch_add(1, Ordering::Relaxed);
                let cas_ms = service
                    .publish_ingest_advance(publication)
                    .await
                    .with_context(|| {
                        format!(
                            "publishing written batch group ending at seq {} block {}",
                            pending_publish
                                .last()
                                .expect("pending publish non-empty")
                                .seq,
                            pending_publish
                                .last()
                                .expect("pending publish non-empty")
                                .last_block
                        )
                    })?;
                let publish_elapsed_ms = publish_started.elapsed().as_millis() as u64;
                let publish_idx = pending_publish.len().saturating_sub(1);
                let publish_group_batches = pending_publish.len() as u64;
                pipeline_progress
                    .publish_completed
                    .fetch_add(1, Ordering::Relaxed);
                pipeline_progress
                    .published_batches
                    .fetch_add(publish_group_batches, Ordering::Relaxed);
                let applied_ready_at = Instant::now();
                for (idx, mut written) in pending_publish.drain(..).enumerate() {
                    let publish_queue_wait_ms = publish_started
                        .duration_since(written.written_ready_at)
                        .as_millis() as u64;
                    if idx == publish_idx {
                        written.timings.cas_ms = cas_ms;
                        written.timings.commit_b_ms =
                            written.timings.commit_b_ms.saturating_add(cas_ms);
                        written.io_apply_ms =
                            written.io_apply_ms.saturating_add(publish_elapsed_ms);
                    }
                    let total_ingest_ms = written.ingest_started.elapsed().as_millis() as u64;
                    applied_tx
                        .send(AppliedBatch {
                            seq: written.seq,
                            last_block: written.last_block,
                            outcomes: written.outcomes,
                            timings: written.timings,
                            first_wait_ms: written.first_wait_ms,
                            fetch_batch_collect_ms: written.fetch_batch_collect_ms,
                            plan_queue_wait_ms: written.plan_queue_wait_ms,
                            plan_ms: written.plan_ms,
                            io_queue_wait_ms: written.io_queue_wait_ms,
                            io_apply_ms: written.io_apply_ms,
                            publish_queue_wait_ms,
                            publish_group_batches: if idx == publish_idx {
                                publish_group_batches
                            } else {
                                0
                            },
                            applied_ready_at,
                            total_ingest_ms,
                        })
                        .await
                        .map_err(|_| eyre!("progress consumer stopped before publish completed"))?;
                    pipeline_progress
                        .applied_sent
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok::<_, eyre::Report>(())
        })
    };

    let mut total_logs: u64 = 0;
    let mut total_txs: u64 = 0;
    let mut total_traces: u64 = 0;
    let mut applied_batches: u64 = 0;
    let mut window_start = Instant::now();
    let mut window_batches: u64 = 0;
    let mut window_blocks: u64 = 0;
    let mut window_txs: u64 = 0;
    let mut window_logs: u64 = 0;
    let mut window_traces: u64 = 0;
    let mut ingest_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut wait_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut fetch_batch_collect_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut plan_queue_wait_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut plan_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut io_queue_wait_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut io_apply_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut publish_queue_wait_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut publish_group_batches: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut progress_queue_wait_ms: Vec<u64> = Vec::with_capacity(log_every_batches as usize);
    let mut phase = PhaseStats::default();
    // Background dictionary pre-training: once an epoch passes its sampling
    // window, kick off training for the *next* epoch's dictionary so it is
    // published before the writer reaches that epoch. `ensure_epoch_dict` is
    // single-flight, so this is safe even if it races the plan-path ensure.
    let dict_config = *service.tables().dicts().config();
    let mut last_pretrained: u32 = 0;
    while let Some(applied) = applied_rx.recv().await {
        let AppliedBatch {
            seq: _seq,
            last_block: n,
            outcomes,
            timings,
            first_wait_ms,
            fetch_batch_collect_ms: batch_fetch_batch_collect_ms,
            plan_queue_wait_ms: batch_plan_queue_wait_ms,
            plan_ms: batch_plan_ms,
            io_queue_wait_ms: batch_io_queue_wait_ms,
            io_apply_ms: batch_io_apply_ms,
            publish_queue_wait_ms: batch_publish_queue_wait_ms,
            publish_group_batches: batch_publish_group_batches,
            applied_ready_at,
            total_ingest_ms,
        } = applied;
        let batch_progress_queue_wait_ms = applied_ready_at.elapsed().as_millis() as u64;
        ingest_ms.push(total_ingest_ms);
        wait_ms.push(first_wait_ms);
        fetch_batch_collect_ms.push(batch_fetch_batch_collect_ms);
        plan_queue_wait_ms.push(batch_plan_queue_wait_ms);
        plan_ms.push(batch_plan_ms);
        io_queue_wait_ms.push(batch_io_queue_wait_ms);
        io_apply_ms.push(batch_io_apply_ms);
        publish_queue_wait_ms.push(batch_publish_queue_wait_ms);
        if batch_publish_group_batches > 0 {
            publish_group_batches.push(batch_publish_group_batches);
        }
        progress_queue_wait_ms.push(batch_progress_queue_wait_ms);
        phase.record(&timings);
        emit_phase_metrics(&metrics, &timings, total_ingest_ms);
        applied_batches += 1;
        window_batches += 1;

        for outcome in &outcomes {
            total_logs += outcome.written_logs as u64;
            total_txs += outcome.written_txs as u64;
            total_traces += outcome.written_traces as u64;
            window_blocks += 1;
            window_txs += outcome.written_txs as u64;
            window_logs += outcome.written_logs as u64;
            window_traces += outcome.written_traces as u64;
        }

        // Pre-train the next epoch's dictionary once the current epoch is past
        // its sampling window. Training reads the already-published, sampled
        // blocks of the current epoch, so the next epoch's dict is ready before
        // the writer crosses the boundary.
        if n % dict_config.epoch_blocks >= dict_config.sample_span {
            let next_v = (n / dict_config.epoch_blocks + 1) as u32;
            if next_v > last_pretrained {
                last_pretrained = next_v;
                let service = service.clone();
                tokio::spawn(async move {
                    if let Err(e) = service.ensure_epoch_dicts(next_v).await {
                        warn!(error = %e, version = next_v, "background dict pre-train failed");
                    }
                });
            }
        }

        let flush = end.is_some_and(|end| n == end)
            || (window_blocks > 0
                && (window_batches >= log_every_batches
                    || window_start.elapsed() >= WINDOW_MAX_WALL));
        if flush {
            let elapsed_secs = window_start.elapsed().as_secs_f64().max(1e-9);
            let fetch_window =
                std::mem::take(&mut *fetch_stats.lock().expect("fetch stats poisoned"));
            let fetch_progress_snapshot = fetch_progress.snapshot();
            let fetch_consumed_snapshot = fetch_consumed_total.load(Ordering::Relaxed);
            let fetch_completed_backlog = fetch_progress_snapshot
                .completed
                .saturating_sub(fetch_consumed_snapshot);
            let fetch_in_flight = fetch_progress_snapshot
                .started
                .saturating_sub(fetch_progress_snapshot.completed);
            let concurrency_observed = control.current();
            let total_fetch_blocks = end.map(|end| end.saturating_sub(start).saturating_add(1));
            let fetch_source_exhausted = total_fetch_blocks.is_some_and(|total_fetch_blocks| {
                fetch_progress_snapshot.started >= total_fetch_blocks
            });
            let fetch_results_drained = total_fetch_blocks.is_some_and(|total_fetch_blocks| {
                fetch_progress_snapshot.completed >= total_fetch_blocks
                    && fetch_completed_backlog == 0
                    && fetch_in_flight == 0
            });
            let fetch_supply_state = if fetch_results_drained {
                "drained"
            } else if fetch_source_exhausted {
                "draining_prefetch"
            } else {
                "live_fetching"
            };
            let fetch_active_utilization =
                round_2(fetch_in_flight as f64 / concurrency_observed.max(1) as f64);
            let fetch_buffer_fill_ratio =
                round_2(fetch_completed_backlog as f64 / fetch_buffer.max(1) as f64);
            let fetch_completed_per_sec = round_2(fetch_window.completed as f64 / elapsed_secs);
            let fetch_p50 = percentile(&fetch_window.durations_ms, 0.50);
            let fetch_p99 = percentile(&fetch_window.durations_ms, 0.99);
            let fetch_join_p50 = percentile(&fetch_window.join_ms, 0.50);
            let fetch_join_p99 = percentile(&fetch_window.join_ms, 0.99);
            let fetch_block_p50 = percentile(&fetch_window.block_ms, 0.50);
            let fetch_block_p99 = percentile(&fetch_window.block_ms, 0.99);
            let fetch_receipts_p50 = percentile(&fetch_window.receipts_ms, 0.50);
            let fetch_receipts_p99 = percentile(&fetch_window.receipts_ms, 0.99);
            let fetch_traces_p50 = percentile(&fetch_window.traces_ms, 0.50);
            let fetch_traces_p99 = percentile(&fetch_window.traces_ms, 0.99);
            let transform_p50 = percentile(&fetch_window.transform_ms, 0.50);
            let transform_p99 = percentile(&fetch_window.transform_ms, 0.99);
            let fetch_request_wall_total_ms = sum_u64(&fetch_window.durations_ms);
            let fetch_join_wall_total_ms = sum_u64(&fetch_window.join_ms);
            let fetch_block_wall_total_ms = sum_u64(&fetch_window.block_ms);
            let fetch_receipts_wall_total_ms = sum_u64(&fetch_window.receipts_ms);
            let fetch_traces_wall_total_ms = sum_u64(&fetch_window.traces_ms);
            let transform_wall_total_ms = sum_u64(&fetch_window.transform_ms);
            let ingest_p50 = percentile(&ingest_ms, 0.50);
            let ingest_p99 = percentile(&ingest_ms, 0.99);
            let ingest_batch_wall_total_ms = sum_u64(&ingest_ms);
            let wait_avg_ms = if wait_ms.is_empty() {
                0.0
            } else {
                wait_ms.iter().sum::<u64>() as f64 / wait_ms.len() as f64
            };
            let wait_max_ms = wait_ms.iter().copied().max().unwrap_or(0);
            let consumer_wait_wall_total_ms = sum_u64(&wait_ms);
            let fetch_batch_collect_wall_total_ms = sum_u64(&fetch_batch_collect_ms);
            let plan_queue_wait_wall_total_ms = sum_u64(&plan_queue_wait_ms);
            let plan_wall_total_ms = sum_u64(&plan_ms);
            let io_queue_wait_wall_total_ms = sum_u64(&io_queue_wait_ms);
            let io_apply_wall_total_ms = sum_u64(&io_apply_ms);
            let publish_queue_wait_wall_total_ms = sum_u64(&publish_queue_wait_ms);
            let progress_queue_wait_wall_total_ms = sum_u64(&progress_queue_wait_ms);
            let fetch_batch_collect_p50 = percentile(&fetch_batch_collect_ms, 0.50);
            let fetch_batch_collect_p99 = percentile(&fetch_batch_collect_ms, 0.99);
            let plan_queue_wait_p50 = percentile(&plan_queue_wait_ms, 0.50);
            let plan_queue_wait_p99 = percentile(&plan_queue_wait_ms, 0.99);
            let plan_p50 = percentile(&plan_ms, 0.50);
            let plan_p99 = percentile(&plan_ms, 0.99);
            let io_queue_wait_p50 = percentile(&io_queue_wait_ms, 0.50);
            let io_queue_wait_p99 = percentile(&io_queue_wait_ms, 0.99);
            let io_apply_p50 = percentile(&io_apply_ms, 0.50);
            let io_apply_p99 = percentile(&io_apply_ms, 0.99);
            let publish_queue_wait_p50 = percentile(&publish_queue_wait_ms, 0.50);
            let publish_queue_wait_p99 = percentile(&publish_queue_wait_ms, 0.99);
            let publish_group_max = publish_group_batches.iter().copied().max().unwrap_or(0);
            let publish_group_avg = if publish_group_batches.is_empty() {
                0.0
            } else {
                publish_group_batches.iter().sum::<u64>() as f64
                    / publish_group_batches.len() as f64
            };
            let progress_queue_wait_p50 = percentile(&progress_queue_wait_ms, 0.50);
            let progress_queue_wait_p99 = percentile(&progress_queue_wait_ms, 0.99);
            let retry_rate = if window_blocks == 0 {
                0.0
            } else {
                fetch_window.retry_attempts as f64 / window_blocks as f64
            };
            let (bottleneck, hint) = classify_bottleneck(
                fetch_p50,
                ingest_p50,
                wait_avg_ms,
                retry_rate,
                concurrency_observed,
            );
            let (pipeline_limiter, pipeline_hint) = classify_pipeline_limiter(
                fetch_batch_collect_p50,
                plan_queue_wait_p50,
                plan_p50,
                io_queue_wait_p50,
                io_apply_p50,
                publish_queue_wait_p50,
                progress_queue_wait_p50,
                fetch_supply_state,
            );

            let phase_summary = std::mem::take(&mut phase).summary();
            let fjall_stats = match collect_fjall_stats(
                ctx.meta_fjall.as_ref(),
                ctx.blob_fjall.as_ref(),
                ctx.same_dir,
            ) {
                Ok(stats) => stats,
                Err(e) => {
                    warn!(error = %e, "fjall keyspace_stats failed");
                    Vec::new()
                }
            };
            let fjall_agg = FjallAggregates::from(fjall_stats.as_slice());
            emit_fjall_metrics(&metrics, &fjall_stats);

            let cache_window = service.tables().take_cache_window_stats();
            let cache_hit_ratio_agg = aggregate_cache_hit_ratio(&cache_window);
            emit_cache_metrics(&metrics, &cache_window);
            let open_index_stats = service.open_index_stats();

            let blocks_per_sec = round_2(window_blocks as f64 / elapsed_secs);
            let txs_per_sec = round_2(window_txs as f64 / elapsed_secs);
            let logs_per_sec = round_2(window_logs as f64 / elapsed_secs);
            let traces_per_sec = round_2(window_traces as f64 / elapsed_secs);

            info!(
                block = n,
                total_txs,
                total_logs,
                total_traces,
                applied_batches_total = applied_batches,
                window_batches,
                log_every_batches,
                bottleneck,
                hint,
                pipeline_limiter,
                pipeline_hint,
                concurrency = concurrency_observed,
                blocks_per_sec,
                fetch_completed_per_sec,
                fetch_started_total = fetch_progress_snapshot.started,
                fetch_completed_total = fetch_progress_snapshot.completed,
                fetch_consumed_total = fetch_consumed_snapshot,
                fetch_completed_backlog,
                fetch_in_flight,
                fetch_buffer_capacity = fetch_buffer,
                estimated_fetch_lookahead_bound_blocks,
                fetch_source_exhausted,
                fetch_supply_state,
                fetch_active_utilization,
                fetch_buffer_fill_ratio,
                txs_per_sec,
                logs_per_sec,
                traces_per_sec,
                fetch_p50_ms = fetch_p50,
                fetch_p99_ms = fetch_p99,
                fetch_join_p50_ms = fetch_join_p50,
                fetch_join_p99_ms = fetch_join_p99,
                fetch_block_p50_ms = fetch_block_p50,
                fetch_block_p99_ms = fetch_block_p99,
                fetch_receipts_p50_ms = fetch_receipts_p50,
                fetch_receipts_p99_ms = fetch_receipts_p99,
                fetch_traces_p50_ms = fetch_traces_p50,
                fetch_traces_p99_ms = fetch_traces_p99,
                transform_p50_ms = transform_p50,
                transform_p99_ms = transform_p99,
                ingest_p50_ms = ingest_p50,
                ingest_p99_ms = ingest_p99,
                window_wall_ms = round_2(elapsed_secs * 1_000.0),
                fetch_request_wall_total_ms,
                fetch_join_wall_total_ms,
                fetch_block_wall_total_ms,
                fetch_receipts_wall_total_ms,
                fetch_traces_wall_total_ms,
                transform_wall_total_ms,
                fetch_batch_collect_wall_total_ms,
                ingest_batch_wall_total_ms,
                plan_queue_wait_wall_total_ms,
                plan_wall_total_ms,
                io_queue_wait_wall_total_ms,
                io_apply_wall_total_ms,
                publish_queue_wait_wall_total_ms,
                progress_queue_wait_wall_total_ms,
                stage_a_wall_total_ms = phase_summary.stage_a_wall_ms_total,
                phase_a_write_ops_total = phase_summary.phase_a_write_ops_total,
                phase_a_write_bytes_total = phase_summary.phase_a_write_bytes_total,
                phase_a_write_ops_by_table = %phase_summary.phase_a_write_ops_by_table,
                phase_a_write_bytes_by_table = %phase_summary.phase_a_write_bytes_by_table,
                phase_b_write_ops_total = phase_summary.phase_b_write_ops_total,
                phase_b_write_bytes_total = phase_summary.phase_b_write_bytes_total,
                phase_b_write_ops_by_table = %phase_summary.phase_b_write_ops_by_table,
                phase_b_write_bytes_by_table = %phase_summary.phase_b_write_bytes_by_table,
                commit_a_meta_wall_total_ms = phase_summary.commit_a_meta_wall_ms_total,
                commit_a_blob_wall_total_ms = phase_summary.commit_a_blob_wall_ms_total,
                reads_wall_total_ms = phase_summary.reads_wall_ms_total,
                stage_b_wall_total_ms = phase_summary.stage_b_wall_ms_total,
                commit_b_wall_total_ms = phase_summary.commit_b_wall_ms_total,
                cas_wall_total_ms = phase_summary.cas_wall_ms_total,
                consumer_wait_wall_total_ms,
                fetch_batch_collect_p50_ms = fetch_batch_collect_p50,
                fetch_batch_collect_p99_ms = fetch_batch_collect_p99,
                plan_queue_wait_p50_ms = plan_queue_wait_p50,
                plan_queue_wait_p99_ms = plan_queue_wait_p99,
                plan_p50_ms = plan_p50,
                plan_p99_ms = plan_p99,
                io_queue_wait_p50_ms = io_queue_wait_p50,
                io_queue_wait_p99_ms = io_queue_wait_p99,
                io_apply_p50_ms = io_apply_p50,
                io_apply_p99_ms = io_apply_p99,
                publish_queue_wait_p50_ms = publish_queue_wait_p50,
                publish_queue_wait_p99_ms = publish_queue_wait_p99,
                publish_group_max,
                publish_group_avg = round_2(publish_group_avg),
                progress_queue_wait_p50_ms = progress_queue_wait_p50,
                progress_queue_wait_p99_ms = progress_queue_wait_p99,
                stage_a_p50_ms = phase_summary.stage_a_p50,
                stage_a_p99_ms = phase_summary.stage_a_p99,
                commit_a_meta_p50_ms = phase_summary.commit_a_meta_p50,
                commit_a_meta_p99_ms = phase_summary.commit_a_meta_p99,
                commit_a_blob_p50_ms = phase_summary.commit_a_blob_p50,
                commit_a_blob_p99_ms = phase_summary.commit_a_blob_p99,
                reads_p50_ms = phase_summary.reads_p50,
                reads_p99_ms = phase_summary.reads_p99,
                stage_b_p50_ms = phase_summary.stage_b_p50,
                stage_b_p99_ms = phase_summary.stage_b_p99,
                commit_b_p50_ms = phase_summary.commit_b_p50,
                commit_b_p99_ms = phase_summary.commit_b_p99,
                cas_p50_ms = phase_summary.cas_p50,
                phase_b_skipped_ratio = round_2(phase_summary.phase_b_skipped_ratio),
                fjall_keyspaces = fjall_agg.keyspaces,
                fjall_disk_space_bytes = fjall_agg.disk_space_bytes,
                fjall_l0_tables = fjall_agg.l0_tables,
                fjall_sealed_memtables = fjall_agg.sealed_memtables,
                fjall_blob_files = fjall_agg.blob_files,
                consumer_wait_avg_ms = round_2(wait_avg_ms),
                consumer_wait_max_ms = wait_max_ms,
                retries = fetch_window.retry_attempts,
                cache_hit_ratio = round_2(cache_hit_ratio_agg),
                open_index_directory_keys = open_index_stats.directory_keys,
                open_index_directory_blocks = open_index_stats.directory_blocks,
                open_index_bitmap_stream_pages = open_index_stats.bitmap_stream_pages,
                open_index_bitmap_blocks = open_index_stats.bitmap_blocks,
                open_index_bitmap_open_pages = open_index_stats.bitmap_open_pages,
                open_index_bitmap_open_streams = open_index_stats.bitmap_open_streams,
                open_index_fragment_value_bytes = open_index_stats.fragment_value_bytes,
                open_index_approx_bytes = open_index_stats.approx_bytes,
                "ingest progress"
            );
            for ks in &fjall_stats {
                debug!(
                    keyspace = %ks.name,
                    sealed_memtable_count = ks.sealed_memtable_count,
                    l0_table_count = ks.l0_table_count,
                    table_count = ks.table_count,
                    blob_file_count = ks.blob_file_count,
                    disk_space_bytes = ks.disk_space_bytes,
                    approximate_len = ks.approximate_len,
                    "fjall keyspace state"
                );
            }

            window_start = Instant::now();
            window_batches = 0;
            window_blocks = 0;
            window_txs = 0;
            window_logs = 0;
            window_traces = 0;
            ingest_ms.clear();
            wait_ms.clear();
            fetch_batch_collect_ms.clear();
            plan_queue_wait_ms.clear();
            plan_ms.clear();
            io_queue_wait_ms.clear();
            io_apply_ms.clear();
            publish_queue_wait_ms.clear();
            publish_group_batches.clear();
            progress_queue_wait_ms.clear();
        }
    }

    // Await all workers, then surface the deepest-stage failure first.
    // When a planning/IO worker fails it drops its channel, which makes the
    // upstream worker fail with a cascade ("X stopped before Y completed").
    // Reporting the downstream (root-cause) error ahead of those cascades keeps
    // the real failure from being masked. A panic (JoinError) always wins.
    let (fetch_res, plan_res, io_res, publisher_res) = tokio::join!(
        fetch_handle,
        plan_handle,
        futures::future::join_all(io_handles),
        publisher_handle
    );
    let publisher_res = publisher_res.context("publish worker panicked")?;
    let plan_res = plan_res.context("planning worker panicked")?;
    let fetch_res = fetch_res.context("fetch worker panicked")?;
    publisher_res.context("publish worker failed")?;
    for (idx, io_res) in io_res.into_iter().enumerate() {
        io_res
            .with_context(|| format!("IO worker {idx} panicked"))?
            .with_context(|| format!("IO worker {idx} failed"))?;
    }
    plan_res.context("planning worker failed")?;
    fetch_res.context("fetch worker failed")?;

    // The ingest pipeline has drained; stop refreshing the lease clock.
    ingest_heartbeat_handle.abort();
    fetch_control_handle.abort();
    clock_refresh_handle.abort();

    if let Some(snapshot_path) = &cli.open_index_snapshot {
        let bytes = service
            .open_index_snapshot_bytes()
            .context("encoding open-index snapshot")?;
        if let Some(parent) = snapshot_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating snapshot directory {}", parent.display()))?;
        }
        let tmp_path = snapshot_path.with_file_name(format!(
            "{}.tmp",
            snapshot_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("open-index.snapshot")
        ));
        std::fs::write(&tmp_path, bytes)
            .with_context(|| format!("writing open-index snapshot {}", tmp_path.display()))?;
        std::fs::rename(&tmp_path, snapshot_path).with_context(|| {
            format!(
                "renaming open-index snapshot {} to {}",
                tmp_path.display(),
                snapshot_path.display()
            )
        })?;
        let stats = service.open_index_stats();
        info!(
            path = %snapshot_path.display(),
            directory_keys = stats.directory_keys,
            directory_blocks = stats.directory_blocks,
            bitmap_stream_pages = stats.bitmap_stream_pages,
            bitmap_blocks = stats.bitmap_blocks,
            bitmap_open_pages = stats.bitmap_open_pages,
            bitmap_open_streams = stats.bitmap_open_streams,
            approx_bytes = stats.approx_bytes,
            "saved open-index snapshot"
        );
    }

    info!(end = ?end, total_txs, total_logs, total_traces, "ingest complete");
    Ok(())
}

fn block_number_stream(
    reader: BlockDataReaderErased,
    start: u64,
    end: Option<u64>,
) -> impl futures::Stream<Item = u64> {
    stream::unfold(
        (reader, start, end, None::<u64>),
        |(reader, next, end, mut latest)| async move {
            loop {
                if let Some(end) = end {
                    return (next <= end).then_some((next, (reader, next + 1, end.into(), latest)));
                }

                if latest.is_some_and(|latest| next <= latest) {
                    return Some((next, (reader, next + 1, None, latest)));
                }

                match reader.get_latest(LatestKind::Uploaded).await {
                    Ok(Some(new_latest)) => {
                        latest = Some(new_latest);
                        if next <= new_latest {
                            continue;
                        }
                        debug!(
                            next,
                            latest = new_latest,
                            "open-ended ingest caught archive tip; waiting for more blocks"
                        );
                    }
                    Ok(None) => debug!(
                        next,
                        "open-ended ingest saw no archive latest; waiting for blocks"
                    ),
                    Err(error) => warn!(
                        %error,
                        next,
                        "open-ended ingest failed to poll archive latest; retrying"
                    ),
                }
                tokio::time::sleep(FOLLOW_POLL_INTERVAL).await;
            }
        },
    )
}

/// Builds an [`S3BlobStore`] from the `--s3-*` flags. Mirrors
/// `monad-archive/src/aws_cli.rs`: a partial static credential pair is rejected
/// rather than silently falling through to the ambient AWS credential chain.
#[cfg(feature = "s3")]
async fn build_s3_blob_store(cli: &Cli) -> Result<monad_chain_data::store::S3BlobStore> {
    use monad_chain_data::store::{S3BlobStore, S3BlobStoreConfig, S3Credentials};

    let Some(bucket) = cli.s3_bucket.clone() else {
        bail!("--blob-backend s3 requires --s3-bucket");
    };
    let credentials = match (&cli.s3_access_key_id, &cli.s3_secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(S3Credentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: None,
        }),
        (Some(_), None) => bail!("--s3-access-key-id requires --s3-secret-access-key"),
        (None, Some(_)) => bail!("--s3-secret-access-key requires --s3-access-key-id"),
        // No static credentials: fall through to the ambient AWS chain.
        (None, None) => None,
    };
    let s3_static_credentials = credentials.is_some();
    info!(
        s3_bucket = %bucket,
        s3_region = ?cli.s3_region,
        s3_endpoint_urls = ?cli.s3_endpoint_urls,
        s3_prefix = %cli.s3_prefix,
        s3_force_path_style = cli.s3_force_path_style,
        s3_max_concurrency = cli.s3_max_concurrency,
        s3_create_bucket = cli.s3_create_bucket,
        s3_static_credentials,
        "blob backend: s3"
    );
    info!(
        s3_bucket = %bucket,
        s3_region = ?cli.s3_region,
        s3_endpoint_urls = ?cli.s3_endpoint_urls,
        s3_create_bucket = cli.s3_create_bucket,
        s3_credentials = if s3_static_credentials { "static flags" } else { "ambient AWS credential chain" },
        "startup validation: ensuring S3 bucket is reachable"
    );
    let config = S3BlobStoreConfig {
        bucket: bucket.clone(),
        root_prefix: cli.s3_prefix.clone(),
        endpoint_urls: cli.s3_endpoint_urls.clone(),
        region: cli.s3_region.clone(),
        force_path_style: cli.s3_force_path_style,
        max_concurrency: cli.s3_max_concurrency,
        create_bucket: cli.s3_create_bucket,
        credentials,
    };
    let started = Instant::now();
    let store = S3BlobStore::new(config)
        .await
        .context("building and validating S3 blob store")?;
    info!(
        s3_bucket = %bucket,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "startup validation: S3 bucket is reachable"
    );
    Ok(store)
}

/// Builds a [`DynamoBlobStore`] (chunked block blobs) from the `--dynamo-blob-*`
/// flags plus the shared `--dynamo-*` connection/credential flags. Mirrors
/// [`build_dynamo_meta_store`]: a partial static credential pair is rejected,
/// and `--dynamo-create-table` provisions the table for dev/test. Targets AWS
/// DynamoDB by default, or ScyllaDB Alternator via `--dynamo-endpoint-url`.
#[cfg(feature = "dynamo")]
async fn build_dynamo_blob_store(
    cli: &Cli,
    shared_client: Option<aws_sdk_dynamodb::Client>,
) -> Result<monad_chain_data::store::DynamoBlobStore> {
    use monad_chain_data::store::{DynamoBlobStore, DynamoBlobStoreConfig, DynamoCredentials};

    let Some(table_name) = cli.dynamo_blob_table.clone() else {
        bail!("--blob-backend dynamo requires --dynamo-blob-table");
    };
    let credentials = match (&cli.dynamo_access_key_id, &cli.dynamo_secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(DynamoCredentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: cli.dynamo_session_token.clone(),
        }),
        (Some(_), None) => bail!("--dynamo-access-key-id requires --dynamo-secret-access-key"),
        (None, Some(_)) => bail!("--dynamo-secret-access-key requires --dynamo-access-key-id"),
        (None, None) => None,
    };
    let dynamo_static_credentials = credentials.is_some();
    let mut config = DynamoBlobStoreConfig::new(table_name.clone());
    config.endpoint_url = cli.dynamo_endpoint_url.clone();
    config.region = cli.dynamo_region.clone();
    config.batch_max_concurrency = effective_dynamo_max_concurrency(cli);
    config.credentials = credentials;
    if let Some(chunk_size) = cli.dynamo_blob_chunk_size {
        config.chunk_size = chunk_size;
    }
    info!(
        dynamo_blob_table = %table_name,
        dynamo_region = ?cli.dynamo_region,
        dynamo_endpoint_url = ?cli.dynamo_endpoint_url,
        scylla_profile = cli.scylla_profile,
        scylla_concurrency = cli.scylla_concurrency,
        dynamo_max_concurrency = effective_dynamo_max_concurrency(cli),
        chunk_size = config.chunk_size,
        dynamo_create_table = cli.dynamo_create_table,
        dynamo_static_credentials,
        reuses_meta_client = shared_client.is_some(),
        "blob backend: dynamo"
    );
    // When the meta backend is dynamo, reuse its client (one connection pool /
    // credential provider for the whole process); otherwise open our own from
    // the same `--dynamo-*` connection flags. Either way the table name, chunk
    // size, and concurrency come from the blob config built above.
    let build_started = Instant::now();
    let store = match shared_client {
        Some(client) => DynamoBlobStore::new_with_client(
            client,
            table_name.clone(),
            config.batch_max_concurrency,
            config.chunk_size,
        ),
        None => DynamoBlobStore::new(config)
            .await
            .context("building DynamoDB blob store")?,
    };
    info!(
        dynamo_blob_table = %table_name,
        elapsed_ms = build_started.elapsed().as_millis() as u64,
        "startup validation: DynamoDB blob store client is ready"
    );
    if cli.dynamo_create_table {
        info!("--dynamo-create-table set: ensuring DynamoDB blob table exists");
        let create_started = Instant::now();
        store
            .create_table()
            .await
            .context("creating DynamoDB blob table")?;
        info!(
            dynamo_blob_table = %table_name,
            elapsed_ms = create_started.elapsed().as_millis() as u64,
            "startup validation: DynamoDB blob table create/active check completed"
        );
    }
    info!(
        dynamo_blob_table = %table_name,
        dynamo_credentials = if dynamo_static_credentials { "static flags" } else { "ambient AWS credential chain" },
        "startup validation: checking DynamoDB blob table connectivity and schema"
    );
    let validate_started = Instant::now();
    store
        .validate_table()
        .await
        .context("validating DynamoDB blob table")?;
    info!(
        dynamo_blob_table = %table_name,
        elapsed_ms = validate_started.elapsed().as_millis() as u64,
        "startup validation: DynamoDB blob table is reachable and schema matches"
    );
    Ok(store)
}

/// Builds a [`DynamoMetaStore`] from the `--dynamo-*` flags. Mirrors
/// [`build_s3_blob_store`]: a partial static credential pair is rejected rather
/// than silently falling through to the ambient AWS credential chain. Targets
/// AWS DynamoDB by default, or any DynamoDB-API-compatible service (DynamoDB
/// Local, ScyllaDB Alternator) via `--dynamo-endpoint-url`.
#[cfg(feature = "dynamo")]
async fn build_dynamo_meta_store(cli: &Cli) -> Result<monad_chain_data::store::DynamoMetaStore> {
    use monad_chain_data::store::{
        DynamoCredentials, DynamoMetaStore, DynamoMetaStoreConfig, DynamoTableLayout,
    };

    let effective_table_layout = effective_dynamo_table_layout(cli);
    let table_layout = match effective_table_layout {
        DynamoTableLayoutArg::Single => {
            if cli.dynamo_table_prefix.is_some() {
                bail!(
                    "--dynamo-table-prefix requires --dynamo-table-layout=per-logical-table and cannot be combined with --scylla-profile"
                );
            }
            let Some(table_name) = cli.dynamo_table.clone() else {
                bail!("--meta-backend dynamo requires --dynamo-table when --dynamo-table-layout=single");
            };
            DynamoTableLayout::single(table_name)
        }
        DynamoTableLayoutArg::PerLogicalTable => {
            if cli.dynamo_table.is_some() {
                bail!("--dynamo-table cannot be combined with --dynamo-table-layout=per-logical-table; use --dynamo-table-prefix");
            }
            let Some(prefix) = cli.dynamo_table_prefix.clone() else {
                bail!("--dynamo-table-layout=per-logical-table requires --dynamo-table-prefix");
            };
            DynamoTableLayout::PerLogicalTable { prefix }
        }
    };
    let credentials = match (&cli.dynamo_access_key_id, &cli.dynamo_secret_access_key) {
        (Some(access_key_id), Some(secret_access_key)) => Some(DynamoCredentials {
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: cli.dynamo_session_token.clone(),
        }),
        (Some(_), None) => bail!("--dynamo-access-key-id requires --dynamo-secret-access-key"),
        (None, Some(_)) => bail!("--dynamo-secret-access-key requires --dynamo-access-key-id"),
        // No static credentials: fall through to the ambient AWS chain.
        (None, None) => None,
    };
    let dynamo_static_credentials = credentials.is_some();
    let dynamo_max_concurrency = effective_dynamo_max_concurrency(cli);
    let dynamo_table_max_concurrency = effective_dynamo_table_max_concurrency(cli);
    info!(
        dynamo_table = ?cli.dynamo_table,
        dynamo_table_prefix = ?cli.dynamo_table_prefix,
        dynamo_table_layout = ?effective_table_layout,
        scylla_profile = cli.scylla_profile,
        scylla_concurrency = cli.scylla_concurrency,
        dynamo_region = ?cli.dynamo_region,
        dynamo_endpoint_url = ?cli.dynamo_endpoint_url,
        dynamo_max_concurrency,
        dynamo_table_max_concurrency,
        dynamo_create_table = cli.dynamo_create_table,
        dynamo_static_credentials,
        "meta backend: dynamo"
    );
    let config = DynamoMetaStoreConfig {
        table_layout,
        endpoint_url: cli.dynamo_endpoint_url.clone(),
        region: cli.dynamo_region.clone(),
        batch_max_concurrency: dynamo_max_concurrency,
        batch_table_max_concurrency: dynamo_table_max_concurrency,
        credentials,
    };
    let build_started = Instant::now();
    let store = DynamoMetaStore::new(config)
        .await
        .context("building DynamoDB meta store")?;
    info!(
        dynamo_table = ?cli.dynamo_table,
        dynamo_table_prefix = ?cli.dynamo_table_prefix,
        dynamo_table_layout = ?effective_table_layout,
        elapsed_ms = build_started.elapsed().as_millis() as u64,
        "startup validation: DynamoDB meta store client is ready"
    );
    if cli.dynamo_create_table {
        info!("--dynamo-create-table set: ensuring DynamoDB metadata table(s) exist");
        let create_started = Instant::now();
        store
            .create_table()
            .await
            .context("creating DynamoDB meta table(s)")?;
        info!(
            dynamo_table = ?cli.dynamo_table,
            dynamo_table_prefix = ?cli.dynamo_table_prefix,
            dynamo_table_layout = ?effective_table_layout,
            elapsed_ms = create_started.elapsed().as_millis() as u64,
            "startup validation: DynamoDB metadata table create/active checks completed"
        );
    }
    info!(
        dynamo_table = ?cli.dynamo_table,
        dynamo_table_prefix = ?cli.dynamo_table_prefix,
        dynamo_table_layout = ?effective_table_layout,
        dynamo_region = ?cli.dynamo_region,
        dynamo_endpoint_url = ?cli.dynamo_endpoint_url,
        dynamo_create_table = cli.dynamo_create_table,
        dynamo_credentials = if dynamo_static_credentials { "static flags" } else { "ambient AWS credential chain" },
        "startup validation: checking DynamoDB table connectivity and schema"
    );
    let validate_started = Instant::now();
    store
        .validate_table()
        .await
        .context("validating DynamoDB meta table(s)")?;
    info!(
        dynamo_table = ?cli.dynamo_table,
        dynamo_table_prefix = ?cli.dynamo_table_prefix,
        dynamo_table_layout = ?effective_table_layout,
        elapsed_ms = validate_started.elapsed().as_millis() as u64,
        "startup validation: DynamoDB table(s) are reachable and schema matches"
    );
    Ok(store)
}

async fn fetch_block(
    reader: &BlockDataReaderErased,
    n: u64,
) -> Result<(u64, Block, BlockReceipts, BlockTraces, FetchTimings)> {
    let total_started = Instant::now();
    let (block, receipts, traces) = tokio::try_join!(
        async {
            let started = Instant::now();
            reader
                .get_block_by_number(n)
                .await
                .map(|block| (block, started.elapsed().as_millis() as u64))
        },
        async {
            let started = Instant::now();
            reader
                .get_block_receipts(n)
                .await
                .map(|receipts| (receipts, started.elapsed().as_millis() as u64))
        },
        async {
            let started = Instant::now();
            reader
                .try_get_block_traces(n)
                .await
                .map(|opt| opt.unwrap_or_default())
                .map(|traces| (traces, started.elapsed().as_millis() as u64))
        },
    )
    .with_context(|| format!("fetching block {n}"))?;
    let timings = FetchTimings {
        total_ms: total_started.elapsed().as_millis() as u64,
        block_ms: block.1,
        receipts_ms: receipts.1,
        traces_ms: traces.1,
    };
    Ok((n, block.0, receipts.0, traces.0, timings))
}

/// Retries `fetch_block` with exponential backoff. Total attempts =
/// `max_retries + 1`. Retries every error indiscriminately — we don't
/// have a transient-vs-permanent classification on the archive side, so
/// genuinely permanent failures (e.g. block-not-found past the
/// archive's tip) waste a fixed retry budget before propagating.
async fn fetch_block_with_retry(
    reader: &BlockDataReaderErased,
    n: u64,
    max_retries: u32,
    initial_backoff: Duration,
    stats: &Mutex<FetchStats>,
    progress: &FetchProgress,
) -> TimedFetchResult {
    let mut backoff = initial_backoff;
    let max_attempts = max_retries + 1 + FETCH_FINAL_RETRY_BACKOFF_ROUNDS;
    // Wall time across all attempts: a retried fetch's "latency" is what
    // the consumer actually waits for, not just the successful attempt.
    let started = Instant::now();
    progress.started.fetch_add(1, Ordering::Relaxed);
    for attempt in 0..max_attempts {
        match fetch_block(reader, n).await {
            Ok(item) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                progress.completed.fetch_add(1, Ordering::Relaxed);
                if elapsed_ms >= SLOW_FETCH_LOG_THRESHOLD_MS || attempt > 0 {
                    let timings = item.4;
                    warn!(
                        block = n,
                        attempts = attempt + 1,
                        total_ms = elapsed_ms,
                        fetch_join_ms = timings.total_ms,
                        fetch_block_ms = timings.block_ms,
                        fetch_receipts_ms = timings.receipts_ms,
                        fetch_traces_ms = timings.traces_ms,
                        slow_threshold_ms = SLOW_FETCH_LOG_THRESHOLD_MS,
                        "archive fetch completed slowly"
                    );
                }
                let mut stats = stats.lock().expect("fetch stats poisoned");
                stats.completed = stats.completed.saturating_add(1);
                stats.durations_ms.push(elapsed_ms);
                stats.block_ms.push(item.4.block_ms);
                stats.receipts_ms.push(item.4.receipts_ms);
                stats.traces_ms.push(item.4.traces_ms);
                stats.join_ms.push(item.4.total_ms);
                return Ok(item);
            }
            Err(e) if attempt + 1 < max_attempts => {
                stats.lock().expect("fetch stats poisoned").retry_attempts += 1;
                progress.retry_attempts.fetch_add(1, Ordering::Relaxed);
                if attempt >= max_retries + 1 {
                    warn!(
                        block = n,
                        attempt = attempt + 1,
                        max_attempts,
                        "fetch retry budget exhausted; extending retry under fetch autotune"
                    );
                }
                let sleep = jittered_backoff(backoff);
                warn!(
                    block = n,
                    attempt = attempt + 1,
                    max_attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    sleep_ms = sleep.as_millis() as u64,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    error = %e,
                    "fetch failed, retrying after backoff"
                );
                tokio::time::sleep(sleep).await;
                backoff = backoff.saturating_mul(2);
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("fetch_block({n}) failed after {max_attempts} attempts")
                });
            }
        }
    }
    unreachable!("loop exits via Ok or final Err arm")
}

fn jittered_backoff(backoff: Duration) -> Duration {
    let base_ms = backoff.as_millis() as u64;
    if base_ms <= 1 {
        return backoff;
    }
    let jittered_ms = rand::thread_rng().gen_range((base_ms / 2).max(1)..=base_ms);
    Duration::from_millis(jittered_ms)
}

#[derive(Default)]
struct FetchStats {
    durations_ms: Vec<u64>,
    join_ms: Vec<u64>,
    block_ms: Vec<u64>,
    receipts_ms: Vec<u64>,
    traces_ms: Vec<u64>,
    transform_ms: Vec<u64>,
    completed: u64,
    retry_attempts: u64,
}

#[derive(Default)]
struct FetchProgress {
    started: AtomicU64,
    completed: AtomicU64,
    retry_attempts: AtomicU64,
}

#[derive(Clone, Copy)]
struct FetchProgressSnapshot {
    started: u64,
    completed: u64,
    retry_attempts: u64,
}

impl FetchProgress {
    fn snapshot(&self) -> FetchProgressSnapshot {
        FetchProgressSnapshot {
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            retry_attempts: self.retry_attempts.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct PipelineProgress {
    planned: AtomicU64,
    write_started: AtomicU64,
    write_apply_completed: AtomicU64,
    write_sent_to_publisher: AtomicU64,
    publish_started: AtomicU64,
    publish_completed: AtomicU64,
    published_batches: AtomicU64,
    applied_sent: AtomicU64,
}

#[derive(Clone, Copy)]
struct PipelineProgressSnapshot {
    planned: u64,
    write_started: u64,
    write_apply_completed: u64,
    write_sent_to_publisher: u64,
    publish_started: u64,
    publish_completed: u64,
    published_batches: u64,
    applied_sent: u64,
}

impl PipelineProgress {
    fn snapshot(&self) -> PipelineProgressSnapshot {
        PipelineProgressSnapshot {
            planned: self.planned.load(Ordering::Relaxed),
            write_started: self.write_started.load(Ordering::Relaxed),
            write_apply_completed: self.write_apply_completed.load(Ordering::Relaxed),
            write_sent_to_publisher: self.write_sent_to_publisher.load(Ordering::Relaxed),
            publish_started: self.publish_started.load(Ordering::Relaxed),
            publish_completed: self.publish_completed.load(Ordering::Relaxed),
            published_batches: self.published_batches.load(Ordering::Relaxed),
            applied_sent: self.applied_sent.load(Ordering::Relaxed),
        }
    }
}

/// Per-window accumulator for the per-phase timings returned by
/// `ingest_blocks`. One sample per batch; percentiles compute over all
/// samples in the current window and reset on flush.
#[derive(Default)]
struct PhaseStats {
    stage_a: Vec<u64>,
    phase_a_write_counts: monad_chain_data::WriteOpCounts,
    phase_b_write_counts: monad_chain_data::WriteOpCounts,
    commit_a_meta: Vec<u64>,
    commit_a_blob: Vec<u64>,
    reads: Vec<u64>,
    stage_b: Vec<u64>,
    commit_b: Vec<u64>,
    cas: Vec<u64>,
    phase_b_skipped: u64,
    phase_b_total: u64,
}

struct PhaseSummary {
    stage_a_wall_ms_total: u64,
    commit_a_meta_wall_ms_total: u64,
    commit_a_blob_wall_ms_total: u64,
    reads_wall_ms_total: u64,
    stage_b_wall_ms_total: u64,
    commit_b_wall_ms_total: u64,
    cas_wall_ms_total: u64,
    phase_a_write_ops_total: u64,
    phase_a_write_bytes_total: u64,
    phase_a_write_ops_by_table: String,
    phase_a_write_bytes_by_table: String,
    phase_b_write_ops_total: u64,
    phase_b_write_bytes_total: u64,
    phase_b_write_ops_by_table: String,
    phase_b_write_bytes_by_table: String,
    stage_a_p50: u64,
    stage_a_p99: u64,
    commit_a_meta_p50: u64,
    commit_a_meta_p99: u64,
    commit_a_blob_p50: u64,
    commit_a_blob_p99: u64,
    reads_p50: u64,
    reads_p99: u64,
    stage_b_p50: u64,
    stage_b_p99: u64,
    commit_b_p50: u64,
    commit_b_p99: u64,
    cas_p50: u64,
    phase_b_skipped_ratio: f64,
}

impl PhaseStats {
    fn record(&mut self, t: &monad_chain_data::IngestBatchTimings) {
        self.stage_a.push(t.stage_a_ms);
        self.phase_a_write_counts.merge(&t.phase_a_write_counts);
        self.phase_b_write_counts.merge(&t.phase_b_write_counts);
        self.commit_a_meta.push(t.commit_a_meta_ms);
        self.commit_a_blob.push(t.commit_a_blob_ms);
        self.reads.push(t.reads_ms);
        self.stage_b.push(t.stage_b_ms);
        self.commit_b.push(t.commit_b_ms);
        self.cas.push(t.cas_ms);
        self.phase_b_total += 1;
        if t.phase_b_skipped {
            self.phase_b_skipped += 1;
        }
    }

    fn summary(self) -> PhaseSummary {
        let ratio = if self.phase_b_total == 0 {
            0.0
        } else {
            self.phase_b_skipped as f64 / self.phase_b_total as f64
        };
        PhaseSummary {
            stage_a_wall_ms_total: sum_u64(&self.stage_a),
            commit_a_meta_wall_ms_total: sum_u64(&self.commit_a_meta),
            commit_a_blob_wall_ms_total: sum_u64(&self.commit_a_blob),
            reads_wall_ms_total: sum_u64(&self.reads),
            stage_b_wall_ms_total: sum_u64(&self.stage_b),
            commit_b_wall_ms_total: sum_u64(&self.commit_b),
            cas_wall_ms_total: sum_u64(&self.cas),
            phase_a_write_ops_total: self.phase_a_write_counts.total_ops(),
            phase_a_write_bytes_total: self.phase_a_write_counts.total_bytes(),
            phase_a_write_ops_by_table: self.phase_a_write_counts.top_by_ops(12).to_string(),
            phase_a_write_bytes_by_table: self.phase_a_write_counts.top_by_bytes(12).to_string(),
            phase_b_write_ops_total: self.phase_b_write_counts.total_ops(),
            phase_b_write_bytes_total: self.phase_b_write_counts.total_bytes(),
            phase_b_write_ops_by_table: self.phase_b_write_counts.top_by_ops(12).to_string(),
            phase_b_write_bytes_by_table: self.phase_b_write_counts.top_by_bytes(12).to_string(),
            stage_a_p50: percentile(&self.stage_a, 0.50),
            stage_a_p99: percentile(&self.stage_a, 0.99),
            commit_a_meta_p50: percentile(&self.commit_a_meta, 0.50),
            commit_a_meta_p99: percentile(&self.commit_a_meta, 0.99),
            commit_a_blob_p50: percentile(&self.commit_a_blob, 0.50),
            commit_a_blob_p99: percentile(&self.commit_a_blob, 0.99),
            reads_p50: percentile(&self.reads, 0.50),
            reads_p99: percentile(&self.reads, 0.99),
            stage_b_p50: percentile(&self.stage_b, 0.50),
            stage_b_p99: percentile(&self.stage_b, 0.99),
            commit_b_p50: percentile(&self.commit_b, 0.50),
            commit_b_p99: percentile(&self.commit_b, 0.99),
            cas_p50: percentile(&self.cas, 0.50),
            phase_b_skipped_ratio: ratio,
        }
    }
}

#[derive(Default)]
struct FjallAggregates {
    keyspaces: usize,
    disk_space_bytes: u64,
    l0_tables: usize,
    sealed_memtables: usize,
    blob_files: usize,
}

impl FjallAggregates {
    fn from(stats: &[monad_chain_data::store::FjallKeyspaceStats]) -> Self {
        let mut out = FjallAggregates {
            keyspaces: stats.len(),
            ..FjallAggregates::default()
        };
        for ks in stats {
            out.disk_space_bytes = out.disk_space_bytes.saturating_add(ks.disk_space_bytes);
            out.l0_tables = out.l0_tables.saturating_add(ks.l0_table_count);
            out.sealed_memtables = out
                .sealed_memtables
                .saturating_add(ks.sealed_memtable_count);
            out.blob_files = out.blob_files.saturating_add(ks.blob_file_count);
        }
        out
    }
}

fn collect_fjall_stats(
    meta_store: Option<&FjallStore>,
    blob_store: Option<&FjallStore>,
    same_dir: bool,
) -> Result<Vec<monad_chain_data::store::FjallKeyspaceStats>> {
    // Each store contributes keyspace stats only when it is fjall-backed; a
    // remote backend (dynamo/s3) has no fjall keyspaces at all.
    let mut stats = Vec::new();
    if let Some(meta_store) = meta_store {
        stats.extend(meta_store.keyspace_stats()?);
    }
    // A shared dir means the blob store is the same Database as meta and is
    // already covered by the meta stats above.
    if let Some(blob_store) = blob_store {
        if !same_dir {
            stats.extend(blob_store.keyspace_stats()?);
        }
    }
    Ok(stats)
}

fn emit_phase_metrics(metrics: &Metrics, t: &monad_chain_data::IngestBatchTimings, total_ms: u64) {
    metrics.histogram(
        MetricNames::CHAIN_DATA_INGEST_STAGE_A_DURATION_MS,
        t.stage_a_ms as f64,
    );
    metrics.histogram(
        MetricNames::CHAIN_DATA_INGEST_COMMIT_A_META_DURATION_MS,
        t.commit_a_meta_ms as f64,
    );
    metrics.histogram(
        MetricNames::CHAIN_DATA_INGEST_COMMIT_A_BLOB_DURATION_MS,
        t.commit_a_blob_ms as f64,
    );
    metrics.histogram(
        MetricNames::CHAIN_DATA_INGEST_READS_DURATION_MS,
        t.reads_ms as f64,
    );
    metrics.histogram(
        MetricNames::CHAIN_DATA_INGEST_STAGE_B_DURATION_MS,
        t.stage_b_ms as f64,
    );
    if t.phase_b_skipped {
        metrics.histogram(
            MetricNames::CHAIN_DATA_INGEST_CAS_DURATION_MS,
            t.cas_ms as f64,
        );
    } else {
        metrics.histogram(
            MetricNames::CHAIN_DATA_INGEST_COMMIT_B_DURATION_MS,
            t.commit_b_ms as f64,
        );
    }
    metrics.histogram(
        MetricNames::CHAIN_DATA_INGEST_BATCH_TOTAL_DURATION_MS,
        total_ms as f64,
    );
    metrics.counter(MetricNames::CHAIN_DATA_INGEST_BATCH_SIZE, t.blocks as u64);
}

fn emit_cache_metrics(metrics: &Metrics, stats: &[(&'static str, u64, u64)]) {
    for (table, hits, misses) in stats {
        let total = hits.saturating_add(*misses);
        if total == 0 {
            continue;
        }
        let ratio = *hits as f64 / total as f64;
        let attrs = [KeyValue::new("table", (*table).to_string())];
        metrics.f64_gauge_with_attrs(MetricNames::CHAIN_DATA_CACHE_HIT_RATIO, ratio, &attrs);
    }
}

fn aggregate_cache_hit_ratio(stats: &[(&'static str, u64, u64)]) -> f64 {
    let (h, m) = stats
        .iter()
        .fold((0u64, 0u64), |(h, m), (_, hi, mi)| (h + hi, m + mi));
    let total = h + m;
    if total == 0 {
        0.0
    } else {
        h as f64 / total as f64
    }
}

fn round_2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn emit_fjall_metrics(metrics: &Metrics, stats: &[monad_chain_data::store::FjallKeyspaceStats]) {
    for ks in stats {
        let attrs = [KeyValue::new("keyspace", ks.name.clone())];
        metrics.gauge_with_attrs(
            MetricNames::CHAIN_DATA_FJALL_KEYSPACE_DISK_SPACE_BYTES,
            ks.disk_space_bytes,
            &attrs,
        );
        metrics.gauge_with_attrs(
            MetricNames::CHAIN_DATA_FJALL_KEYSPACE_L0_TABLES,
            ks.l0_table_count as u64,
            &attrs,
        );
        metrics.gauge_with_attrs(
            MetricNames::CHAIN_DATA_FJALL_KEYSPACE_SEALED_MEMTABLES,
            ks.sealed_memtable_count as u64,
            &attrs,
        );
    }
}

/// Dynamic concurrency limiter driven by wall-clock fetch retry windows.
///
/// The semaphore is the actual gate: fetchers `acquire_owned()` a permit
/// before issuing the request and drop it on return. Controller ticks mutate
/// the permit pool: `add_permits` grows, spawned `acquire_many.forget` shrinks
/// as in-flight requests finish.
struct ConcurrencyControl {
    permits: Arc<Semaphore>,
    current: AtomicUsize,
    min: usize,
    max: usize,
}

impl ConcurrencyControl {
    fn new(initial: usize, min: usize, max: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(initial)),
            current: AtomicUsize::new(initial),
            min,
            max,
        }
    }

    fn permits(&self) -> Arc<Semaphore> {
        self.permits.clone()
    }

    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Wall-clock fetch-side adjustment. This intentionally uses only fetch
    /// deltas, not downstream ingest progress, so archive health cannot lag
    /// behind large ordered lookahead buffers or IO queues.
    fn adjust_from_fetch_window(
        &self,
        retry_rate: f64,
        completed_delta: u64,
        started_delta: u64,
        _in_flight: u64,
    ) -> usize {
        let cur = self.current.load(Ordering::Relaxed);
        let new = if retry_rate > 0.20 {
            ((cur as f64) * 0.70).floor() as usize
        } else if retry_rate > 0.05 {
            ((cur as f64) * 0.85).floor() as usize
        } else if retry_rate > 0.01 {
            ((cur as f64) * 0.95).floor() as usize
        } else if retry_rate < 0.002 && completed_delta > 0 && cur < self.max {
            cur.saturating_add((cur / 8).max(250))
        } else if retry_rate < 0.005 && started_delta > 0 && cur < self.max {
            cur.saturating_add((cur / 16).max(100))
        } else {
            cur
        }
        .clamp(self.min, self.max);
        if new == cur {
            return cur;
        }
        self.current.store(new, Ordering::Relaxed);
        self.resize(cur, new);
        new
    }

    fn resize(&self, cur: usize, new: usize) {
        if new > cur {
            self.permits.add_permits(new - cur);
        } else {
            let diff = (cur - new) as u32;
            let permits = self.permits.clone();
            tokio::spawn(async move {
                if let Ok(p) = permits.acquire_many_owned(diff).await {
                    p.forget();
                }
            });
        }
    }
}

#[cfg(unix)]
fn raise_nofile_to_hard_limit() -> Result<(u64, u64)> {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) } != 0 {
        return Err(std::io::Error::last_os_error()).context("getrlimit(NOFILE)");
    }
    let was = rl.rlim_cur as u64;
    if rl.rlim_cur < rl.rlim_max {
        rl.rlim_cur = rl.rlim_max;
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) } != 0 {
            return Err(std::io::Error::last_os_error()).context("setrlimit(NOFILE)");
        }
    }
    Ok((was, rl.rlim_cur as u64))
}

#[cfg(not(unix))]
fn raise_nofile_to_hard_limit() -> Result<(u64, u64)> {
    Ok((0, 0))
}

fn percentile(samples: &[u64], p: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx]
}

fn sum_u64(samples: &[u64]) -> u64 {
    samples
        .iter()
        .fold(0u64, |acc, sample| acc.saturating_add(*sample))
}

/// Maps the per-window timing signals to a single bottleneck label so the
/// operator does not have to interpret raw numbers. Returns `(label, hint)`
/// where `hint` names the action that label points at.
fn classify_bottleneck(
    fetch_p50_ms: u64,
    ingest_p50_ms: u64,
    consumer_wait_avg_ms: f64,
    retry_rate: f64,
    concurrency: usize,
) -> (&'static str, &'static str) {
    if retry_rate > 0.05 {
        return ("throttled", "back off --concurrency");
    }
    let per_slot_ms = (fetch_p50_ms as f64) / (concurrency.max(1) as f64);
    if consumer_wait_avg_ms > (fetch_p50_ms as f64) * 0.7 {
        return (
            "s3_head_of_line",
            "tail latency dragging head; check fetch_p99 vs p50",
        );
    }
    if (ingest_p50_ms as f64) > per_slot_ms * 1.5 {
        return (
            "ingest_bound",
            "parallelize writers (fjall ingest > per-slot fetch budget)",
        );
    }
    ("s3_pipelined", "raise --concurrency if retries stay low")
}

/// Classifies the limiter inside the already-pipelined ingest path. Queue-wait
/// labels mean a downstream stage is slower than its producer; service-time
/// labels mean the stage itself is setting batch cadence.
fn classify_pipeline_limiter(
    fetch_batch_collect_p50_ms: u64,
    plan_queue_wait_p50_ms: u64,
    plan_p50_ms: u64,
    io_queue_wait_p50_ms: u64,
    io_apply_p50_ms: u64,
    publish_queue_wait_p50_ms: u64,
    progress_queue_wait_p50_ms: u64,
    fetch_supply_state: &'static str,
) -> (&'static str, &'static str) {
    if fetch_supply_state == "draining_prefetch" {
        return (
            "prefetch_drain",
            "archive fetch is exhausted; queue waits are downstream drain of buffered results",
        );
    }
    if fetch_supply_state == "drained" {
        return (
            "post_fetch_drain",
            "all archive fetch results are consumed; remaining timings are downstream flush",
        );
    }

    let candidates = [
        (
            "fetch_collect",
            fetch_batch_collect_p50_ms,
            "archive fetch/order/transform is setting batch cadence",
        ),
        (
            "plan_queue",
            plan_queue_wait_p50_ms,
            "planner is backlogged behind fetched batches",
        ),
        (
            "plan",
            plan_p50_ms,
            "planning CPU/read path is setting batch cadence",
        ),
        (
            "io_queue",
            io_queue_wait_p50_ms,
            "IO worker is backlogged behind planned batches",
        ),
        (
            "io_apply",
            io_apply_p50_ms,
            "remote writes/CAS are setting batch cadence",
        ),
        (
            "publish_queue",
            publish_queue_wait_p50_ms,
            "publisher is backlogged behind durable writes",
        ),
        (
            "progress_queue",
            progress_queue_wait_p50_ms,
            "progress consumer/logging is backlogged behind applied batches",
        ),
    ];
    let (label, value, hint) = candidates
        .into_iter()
        .max_by_key(|(_, value, _)| *value)
        .expect("non-empty limiter candidates");
    if value == 0 {
        ("unknown", "all p50 pipeline stage timings rounded to 0ms")
    } else {
        (label, hint)
    }
}

fn into_finalized_block(
    block: Block,
    receipts: BlockReceipts,
    traces: BlockTraces,
) -> Result<FinalizedBlock> {
    let header = block.header;
    let block_number = header.number;
    let txs: Vec<IngestTx> = block
        .body
        .transactions
        .into_iter()
        .map(|tx_w| IngestTx {
            tx_hash: *tx_w.tx.tx_hash(),
            sender: tx_w.sender,
            signed_tx_bytes: Bytes::from(tx_w.tx.encoded_2718()),
        })
        .collect();

    if !txs.is_empty() && txs.len() != receipts.len() {
        bail!(
            "block {}: tx count {} != receipt count {}",
            block_number,
            txs.len(),
            receipts.len()
        );
    }
    if txs.is_empty() && !receipts.is_empty() {
        // Should not happen for valid archives, but worth surfacing rather
        // than silently dropping receipts.
        warn!(
            block = block_number,
            receipts = receipts.len(),
            "block has receipts but no transactions; ignoring receipts"
        );
    }

    // Per-tx receipt success flag, needed by the trace ingest plan for
    // the `has_transfer` predicate (a sub-call can succeed inside a
    // reverted parent tx; the tracer doesn't rewrite descendants).
    let tx_statuses: Vec<bool> = receipts.iter().map(|r| r.receipt.status()).collect();
    let logs_by_tx: Vec<Vec<alloy_primitives::Log>> = receipts
        .into_iter()
        .map(|r| r.receipt.logs().to_vec())
        .collect();

    let ingest_traces = build_ingest_traces(block_number, &traces, &tx_statuses, txs.len())?;

    Ok(FinalizedBlock {
        header,
        logs_by_tx,
        txs,
        traces: ingest_traces,
    })
}

fn map_call_kind(typ: &ArchiveCallKind) -> CallKind {
    match typ {
        ArchiveCallKind::Call => CallKind::Call,
        ArchiveCallKind::DelegateCall => CallKind::DelegateCall,
        ArchiveCallKind::CallCode => CallKind::CallCode,
        ArchiveCallKind::Create => CallKind::Create,
        ArchiveCallKind::Create2 => CallKind::Create2,
        ArchiveCallKind::SelfDestruct => CallKind::SelfDestruct,
        ArchiveCallKind::StaticCall => CallKind::StaticCall,
    }
}

/// Decodes each tx's `Vec<Vec<CallFrame>>` from the archive's RLP form,
/// flattens DFS, computes per-frame `trace_address`, threads in the
/// tx-level receipt status, and concatenates across txs in tx order.
///
/// Archives prior to the trace recorder being enabled may carry an
/// empty `BlockTraces` even on blocks with txs; in that case we emit an
/// empty trace vec rather than fail.
fn build_ingest_traces(
    block_number: u64,
    raw_traces: &BlockTraces,
    tx_statuses: &[bool],
    tx_count: usize,
) -> Result<Vec<IngestTrace>> {
    if raw_traces.is_empty() {
        return Ok(Vec::new());
    }
    if raw_traces.len() != tx_count {
        bail!(
            "block {}: trace tx count {} != tx count {}",
            block_number,
            raw_traces.len(),
            tx_count
        );
    }

    let mut out = Vec::new();
    for (tx_idx, raw_tx_trace) in raw_traces.iter().enumerate() {
        let nested: Vec<Vec<CallFrame>> = decode_trace(raw_tx_trace)
            .map_err(|e| eyre!("block {block_number} tx {tx_idx}: decode_trace failed: {e}"))?;
        let frames: Vec<CallFrame> = nested.into_iter().flatten().collect();
        if frames.is_empty() {
            continue;
        }
        let depths: Vec<u32> = frames.iter().map(|f| f.depth.to::<u32>()).collect();
        let trace_addresses = compute_trace_addresses(depths)
            .map_err(|e| eyre!("block {block_number} tx {tx_idx}: trace_address: {e:?}"))?;
        debug_assert_eq!(frames.len(), trace_addresses.len());

        let tx_status = *tx_statuses
            .get(tx_idx)
            .ok_or_else(|| eyre!("block {block_number} tx {tx_idx}: missing receipt"))?;
        let tx_index_u32 =
            u32::try_from(tx_idx).map_err(|_| eyre!("block {block_number}: tx index overflow"))?;

        for (frame, trace_address) in frames.into_iter().zip(trace_addresses.into_iter()) {
            out.push(IngestTrace {
                typ: map_call_kind(&frame.typ),
                from: frame.from,
                to: frame.to,
                value: frame.value,
                gas: frame.gas.to::<u64>(),
                gas_used: frame.gas_used.to::<u64>(),
                input: frame.input,
                output: frame.output,
                status: frame.status.to::<u8>(),
                depth: frame.depth.to::<u32>(),
                tx_index: tx_index_u32,
                trace_address,
                tx_status,
            });
        }
    }
    Ok(out)
}
