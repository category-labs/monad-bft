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
    CallKind, FinalizedBlock, IngestPlan, IngestTrace, IngestTx, IoRetryPolicy,
    MonadChainDataService, QueryLimits,
};
use opentelemetry::KeyValue;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, warn, Level};

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

    /// Last block to ingest (inclusive). Mutually exclusive with `--count`;
    /// exactly one of the two must be set.
    #[arg(long, conflicts_with = "count", required_unless_present = "count")]
    end: Option<u64>,

    /// Number of blocks to ingest from the resolved start (i.e.
    /// `published_head + 1`). End block is derived as `start + count - 1`.
    /// Mutually exclusive with `--end`.
    #[arg(long, conflicts_with = "end", required_unless_present = "end")]
    count: Option<u64>,

    /// Optional OTel collector endpoint for archive-side metrics. Off by
    /// default; archive readers still build without it.
    #[arg(long)]
    otel_endpoint: Option<String>,

    /// How often to log progress, in blocks.
    #[arg(long, default_value_t = 1000)]
    log_every: u64,

    /// Maximum number of block fetches in flight against the archive
    /// concurrently. Each in-flight slot issues block + receipts + traces
    /// in parallel via `try_join!`, so peak archive request concurrency
    /// is roughly `3 * concurrency`. Increase for high-latency archives
    /// (e.g. S3); leave low for local FS where queueing buys nothing.
    /// Memory ceiling is bounded by this many fetched-but-not-yet-ingested
    /// blocks held in the prefetch buffer.
    #[arg(long, default_value_t = 512)]
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

    /// Enable adaptive concurrency. When set, `--concurrency` is the
    /// starting value; an AIMD controller scales it within
    /// `[--min-concurrency, --max-concurrency]` based on retry rate per
    /// window (halve above 5% retries, additive increase below 1%).
    #[arg(long)]
    autotune: bool,

    /// Lower bound on adaptive concurrency. Ignored unless `--autotune`.
    #[arg(long, default_value_t = 1)]
    min_concurrency: usize,

    /// Upper bound on adaptive concurrency. Ignored unless `--autotune`.
    #[arg(long, default_value_t = 5000)]
    max_concurrency: usize,

    /// Number of ordered fetch futures to keep buffered ahead of ingest.
    /// This is separate from `--concurrency`: the buffer may hold many
    /// pending/completed block fetches while the concurrency semaphore caps
    /// active archive requests.
    #[arg(long)]
    fetch_buffer: Option<usize>,

    /// Skip fetching and ingesting execution traces. With this flag set,
    /// `Family::Trace` stays empty for every ingested block, and both
    /// `query_traces` and `query_transfers` will return zero results for
    /// those blocks (transfers is a derived view over traces). Drops the
    /// per-block archive fanout from 3 requests to 2.
    #[arg(long)]
    no_traces: bool,

    /// Number of consecutive blocks coalesced into one ingest batch. 1
    /// matches the pre-batching live-mode behavior; backfill should raise
    /// this (e.g. 32-256) to amortize fjall WAL+commit cost. Each batch
    /// buffers N fully-built `FinalizedBlock`s in memory.
    #[arg(long, default_value_t = 1)]
    batch_size: usize,

    /// Number of fully planned ingest batches allowed to queue in front of
    /// the IO worker. This is the backpressure boundary between planning
    /// and write/CAS publication.
    #[arg(long, default_value_t = 2)]
    plan_buffer: usize,

    /// Number of retries for non-CAS meta/blob write failures. CAS is never
    /// retried; a CAS conflict means this writer lost the publication lease.
    #[arg(long, default_value_t = 0)]
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

    /// Override the S3 endpoint URL for a compatible service (MinIO, R2, Ceph).
    /// Leave unset to target real AWS S3.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_endpoint_url: Option<String>,

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
    #[arg(long, default_value_t = 32)]
    s3_max_concurrency: usize,

    /// Static S3 access key id. Must be paired with --s3-secret-access-key.
    /// Leave both unset to use the ambient AWS credential chain.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_access_key_id: Option<String>,

    /// Static S3 secret access key. Must be paired with --s3-access-key-id.
    #[cfg(feature = "s3")]
    #[arg(long)]
    s3_secret_access_key: Option<String>,

    /// DynamoDB/Alternator table holding every metadata row (kv, scannable
    /// index/directory, and the publication-head CAS row share one table via a
    /// fixed binary `pk`/`sk` schema). Required when `--meta-backend dynamo`.
    #[cfg(feature = "dynamo")]
    #[arg(long)]
    dynamo_table: Option<String>,

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
    #[arg(long, default_value_t = 16)]
    dynamo_max_concurrency: usize,

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
    ingest_started: Instant,
}

struct PlannedBatch {
    last_block: u64,
    plan: IngestPlan,
    first_wait_ms: u64,
    ingest_started: Instant,
}

struct AppliedBatch {
    last_block: u64,
    outcomes: Vec<monad_chain_data::IngestOutcome>,
    timings: monad_chain_data::IngestBatchTimings,
    first_wait_ms: u64,
    total_ingest_ms: u64,
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
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(Level::INFO.into())
                .from_env_lossy(),
        )
        .init();

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

    let cli = Cli::parse();
    if cli.concurrency == 0 {
        bail!("--concurrency must be >= 1");
    }
    if matches!(cli.count, Some(0)) {
        bail!("--count must be >= 1");
    }
    if cli.batch_size == 0 {
        bail!("--batch-size must be >= 1");
    }
    if cli.plan_buffer == 0 {
        bail!("--plan-buffer must be >= 1");
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
            dispatch_blob(
                &cli,
                meta_store,
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
    meta_fjall: Option<FjallStore>,
    blob_fjall: Option<FjallStore>,
    same_dir: bool,
    meta_backend: &'static str,
    cache_config: CacheConfig,
) -> Result<()> {
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
    if start != expected_start {
        bail!(
            "start={} does not match expected next block {} (current head: {:?})",
            start,
            expected_start,
            head
        );
    }
    let end = match (cli.end, cli.count) {
        (Some(e), None) => e,
        (None, Some(c)) => {
            let Some(e) = start.checked_add(c - 1) else {
                bail!("start + count - 1 overflows u64 (start={start}, count={c})");
            };
            e
        }
        // clap's required_unless_present + conflicts_with constraints make
        // the other two cases unreachable.
        _ => unreachable!("clap enforces exactly one of --end or --count"),
    };
    if start > end {
        bail!("start ({}) must be <= end ({})", start, end);
    }

    let metrics = Metrics::new(
        cli.otel_endpoint.as_deref(),
        "chain-data-ingest",
        "0".to_string(),
        Duration::from_secs(60),
    )
    .context("building metrics")?;
    let reader = cli
        .block_data_source
        .build(&metrics)
        .await
        .context("building block data reader")?;

    // Seed the lease clock from the archive's latest available finalized
    // block. The lease decisions are made against this observed upstream head;
    // when the source has no current head the cell stays `u64::MAX` (unknown),
    // so the authority fails closed rather than acting on a stale clock.
    match reader
        .get_latest(LatestKind::Uploaded)
        .await
        .context("reading archive latest available block")?
    {
        Some(latest) => {
            observed_upstream.store(latest, Ordering::SeqCst);
            info!(latest, "seeded lease clock from archive latest available block");
        }
        None => warn!(
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

    // Concurrency controller. When `--autotune` is off, min == max == initial
    // and `tune()` is never called, so the controller degenerates into a
    // fixed semaphore equivalent to the old `buffered(N)`.
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

    let fetch_buffer = cli.fetch_buffer.unwrap_or(max_concurrency);
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
    info!(
        start,
        end,
        concurrency = cli.concurrency,
        fetch_buffer,
        batch_size = cli.batch_size,
        plan_buffer = cli.plan_buffer,
        io_retry_forever = cli.io_retry_forever,
        io_max_retries = cli.io_max_retries,
        meta_backend = ctx.meta_backend,
        blob_backend = ctx.blob_backend,
        "starting ingest"
    );

    // A window narrower than the prefetch buffer can fall entirely inside
    // a drain phase — the consumer empties already-completed slots while
    // no new fetches finish in that span — producing an all-zero stats
    // window that flips the classifier to nonsense. Floor `log_every` at
    // `fetch_buffer` so each window must span at least one buffer refill.
    let log_every = cli.log_every.max(fetch_buffer as u64);
    if log_every != cli.log_every {
        info!(
            requested = cli.log_every,
            effective = log_every,
            "log_every raised to span at least one prefetch refill"
        );
    }
    // Wall-clock cap on window length. When throughput collapses (e.g.
    // throttled at high concurrency) the modulus trigger can take an
    // hour to fire; the wall-clock fallback keeps feedback flowing.
    const WINDOW_MAX_WALL: Duration = Duration::from_secs(30);

    let fetch_stats = Arc::new(Mutex::new(FetchStats::default()));
    let fetch_progress = Arc::new(FetchProgress::default());
    if cli.no_traces {
        info!("--no-traces set: skipping trace fetch and ingest");
    }

    let (fetched_tx, mut fetched_rx) = mpsc::channel::<FetchedBatch>(cli.plan_buffer);
    let (planned_tx, mut planned_rx) = mpsc::channel::<PlannedBatch>(cli.plan_buffer);
    let (applied_tx, mut applied_rx) = mpsc::channel::<AppliedBatch>(cli.plan_buffer);
    let fetch_consumed_total = Arc::new(AtomicU64::new(0));

    let fetch_handle = {
        let reader = reader.clone();
        let fetch_stats = fetch_stats.clone();
        let fetch_progress = fetch_progress.clone();
        let permits = control.permits();
        let fetched_tx = fetched_tx.clone();
        let fetch_consumed_total = fetch_consumed_total.clone();
        let max_retries = cli.max_retries;
        let initial_backoff = Duration::from_millis(cli.retry_backoff_ms);
        let fetch_traces = !cli.no_traces;
        let batch_size = cli.batch_size;
        tokio::spawn(async move {
            // Bounded prefetch. `buffered(fetch_buffer)` preserves input
            // order; the semaphore inside the closure is the dynamic active
            // fetch limit.
            let fetch_stream = stream::iter(start..=end)
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
                            fetch_traces,
                        )
                        .await
                    }
                })
                .buffered(fetch_buffer);
            futures::pin_mut!(fetch_stream);

            let mut pending: Vec<(u64, FinalizedBlock)> = Vec::with_capacity(batch_size);
            loop {
                pending.clear();
                let wait_started = Instant::now();
                let mut first_wait_ms = 0;
                for slot in 0..batch_size {
                    let Some(item) = fetch_stream.next().await else {
                        break;
                    };
                    if slot == 0 {
                        first_wait_ms = wait_started.elapsed().as_millis() as u64;
                    }
                    let (n, block, receipts, traces) = item?;
                    fetch_consumed_total.fetch_add(1, Ordering::Relaxed);
                    let finalized = into_finalized_block(block, receipts, traces)
                        .with_context(|| format!("transforming block {n}"))?;
                    pending.push((n, finalized));
                }
                if pending.is_empty() {
                    break;
                }
                let last_block = pending.last().expect("pending non-empty").0;
                let blocks = pending.iter().map(|(_, b)| b.clone()).collect();
                fetched_tx
                    .send(FetchedBatch {
                        last_block,
                        blocks,
                        first_wait_ms,
                        ingest_started: Instant::now(),
                    })
                    .await
                    .map_err(|_| eyre!("planning worker stopped before fetch completed"))?;
            }
            Ok::<_, eyre::Report>(())
        })
    };

    drop(fetched_tx);

    let plan_handle = {
        let service = service.clone();
        tokio::spawn(async move {
            while let Some(batch) = fetched_rx.recv().await {
                let plan = service
                    .plan_ingest_blocks(batch.blocks)
                    .await
                    .with_context(|| {
                        format!("planning batch ending at block {}", batch.last_block)
                    })?
                    .expect("fetch worker never sends empty batches");
                planned_tx
                    .send(PlannedBatch {
                        last_block: batch.last_block,
                        plan,
                        first_wait_ms: batch.first_wait_ms,
                        ingest_started: batch.ingest_started,
                    })
                    .await
                    .map_err(|_| eyre!("IO worker stopped before planning completed"))?;
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
    let io_handle = {
        let service = service.clone();
        tokio::spawn(async move {
            while let Some(batch) = planned_rx.recv().await {
                let (outcomes, timings) = service
                    .apply_ingest_plan_with_retry(batch.plan, io_retry)
                    .await
                    .with_context(|| {
                        format!("applying batch ending at block {}", batch.last_block)
                    })?;
                let total_ingest_ms = batch.ingest_started.elapsed().as_millis() as u64;
                applied_tx
                    .send(AppliedBatch {
                        last_block: batch.last_block,
                        outcomes,
                        timings,
                        first_wait_ms: batch.first_wait_ms,
                        total_ingest_ms,
                    })
                    .await
                    .map_err(|_| eyre!("progress consumer stopped before IO completed"))?;
            }
            Ok::<_, eyre::Report>(())
        })
    };

    let mut total_logs: u64 = 0;
    let mut total_txs: u64 = 0;
    let mut total_traces: u64 = 0;
    let mut window_start = Instant::now();
    let mut window_blocks: u64 = 0;
    let mut window_txs: u64 = 0;
    let mut window_logs: u64 = 0;
    let mut window_traces: u64 = 0;
    let mut ingest_ms: Vec<u64> = Vec::with_capacity(cli.log_every as usize);
    let mut wait_ms: Vec<u64> = Vec::with_capacity(cli.log_every as usize);
    let mut phase = PhaseStats::default();
    // Background dictionary pre-training: once an epoch passes its sampling
    // window, kick off training for the *next* epoch's dictionary so it is
    // published before the writer reaches that epoch. `ensure_epoch_dict` is
    // single-flight, so this is safe even if it races the plan-path ensure.
    let dict_config = *service.tables().dicts().config();
    let mut last_pretrained: u32 = 0;
    while let Some(applied) = applied_rx.recv().await {
        let AppliedBatch {
            last_block: n,
            outcomes,
            timings,
            first_wait_ms,
            total_ingest_ms,
        } = applied;
        ingest_ms.push(total_ingest_ms);
        wait_ms.push(first_wait_ms);
        phase.record(&timings);
        emit_phase_metrics(&metrics, &timings, total_ingest_ms);

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

        let flush = n == end
            || (window_blocks > 0
                && (n % log_every == 0 || window_start.elapsed() >= WINDOW_MAX_WALL));
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
            let fetch_completed_per_sec = round_2(fetch_window.completed as f64 / elapsed_secs);
            let fetch_p50 = percentile(&fetch_window.durations_ms, 0.50);
            let fetch_p99 = percentile(&fetch_window.durations_ms, 0.99);
            let fetch_request_wall_total_ms = sum_u64(&fetch_window.durations_ms);
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
            let retry_rate = if window_blocks == 0 {
                0.0
            } else {
                fetch_window.retry_attempts as f64 / window_blocks as f64
            };
            let concurrency_observed = control.current();
            let (bottleneck, hint) = classify_bottleneck(
                fetch_p50,
                ingest_p50,
                wait_avg_ms,
                retry_rate,
                concurrency_observed,
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
                bottleneck,
                hint,
                concurrency = concurrency_observed,
                blocks_per_sec,
                fetch_completed_per_sec,
                fetch_started_total = fetch_progress_snapshot.started,
                fetch_completed_total = fetch_progress_snapshot.completed,
                fetch_consumed_total = fetch_consumed_snapshot,
                fetch_completed_backlog,
                fetch_in_flight,
                fetch_buffer_capacity = fetch_buffer,
                txs_per_sec,
                logs_per_sec,
                traces_per_sec,
                fetch_p50_ms = fetch_p50,
                fetch_p99_ms = fetch_p99,
                ingest_p50_ms = ingest_p50,
                ingest_p99_ms = ingest_p99,
                window_wall_ms = round_2(elapsed_secs * 1_000.0),
                fetch_request_wall_total_ms,
                ingest_batch_wall_total_ms,
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

            if cli.autotune {
                let adjusted = control.tune(retry_rate);
                if adjusted != concurrency_observed {
                    info!(
                        from = concurrency_observed,
                        to = adjusted,
                        retry_rate,
                        "autotune: adjusted concurrency"
                    );
                }
            }

            window_start = Instant::now();
            window_blocks = 0;
            window_txs = 0;
            window_logs = 0;
            window_traces = 0;
            ingest_ms.clear();
            wait_ms.clear();
        }
    }

    // Await all three workers, then surface the deepest-stage failure first.
    // When a planning/IO worker fails it drops its channel, which makes the
    // upstream worker fail with a cascade ("X stopped before Y completed").
    // Reporting the downstream (root-cause) error ahead of those cascades keeps
    // the real failure from being masked. A panic (JoinError) always wins.
    let (fetch_res, plan_res, io_res) = tokio::join!(fetch_handle, plan_handle, io_handle);
    let io_res = io_res.context("IO worker panicked")?;
    let plan_res = plan_res.context("planning worker panicked")?;
    let fetch_res = fetch_res.context("fetch worker panicked")?;
    io_res.context("IO worker failed")?;
    plan_res.context("planning worker failed")?;
    fetch_res.context("fetch worker failed")?;

    // The ingest pipeline has drained; stop refreshing the lease clock.
    clock_refresh_handle.abort();

    info!(end, total_txs, total_logs, total_traces, "ingest complete");
    Ok(())
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
    info!(
        s3_bucket = %bucket,
        s3_region = ?cli.s3_region,
        s3_endpoint_url = ?cli.s3_endpoint_url,
        s3_prefix = %cli.s3_prefix,
        s3_force_path_style = cli.s3_force_path_style,
        s3_max_concurrency = cli.s3_max_concurrency,
        s3_static_credentials = credentials.is_some(),
        "blob backend: s3"
    );
    let config = S3BlobStoreConfig {
        bucket,
        root_prefix: cli.s3_prefix.clone(),
        endpoint_url: cli.s3_endpoint_url.clone(),
        region: cli.s3_region.clone(),
        force_path_style: cli.s3_force_path_style,
        max_concurrency: cli.s3_max_concurrency,
        credentials,
    };
    S3BlobStore::new(config)
        .await
        .context("building S3 blob store")
}

/// Builds a [`DynamoMetaStore`] from the `--dynamo-*` flags. Mirrors
/// [`build_s3_blob_store`]: a partial static credential pair is rejected rather
/// than silently falling through to the ambient AWS credential chain. Targets
/// AWS DynamoDB by default, or any DynamoDB-API-compatible service (DynamoDB
/// Local, ScyllaDB Alternator) via `--dynamo-endpoint-url`.
#[cfg(feature = "dynamo")]
async fn build_dynamo_meta_store(cli: &Cli) -> Result<monad_chain_data::store::DynamoMetaStore> {
    use monad_chain_data::store::{DynamoCredentials, DynamoMetaStore, DynamoMetaStoreConfig};

    let Some(table_name) = cli.dynamo_table.clone() else {
        bail!("--meta-backend dynamo requires --dynamo-table");
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
    info!(
        dynamo_table = %table_name,
        dynamo_region = ?cli.dynamo_region,
        dynamo_endpoint_url = ?cli.dynamo_endpoint_url,
        dynamo_max_concurrency = cli.dynamo_max_concurrency,
        dynamo_create_table = cli.dynamo_create_table,
        dynamo_static_credentials = credentials.is_some(),
        "meta backend: dynamo"
    );
    let config = DynamoMetaStoreConfig {
        table_name,
        endpoint_url: cli.dynamo_endpoint_url.clone(),
        region: cli.dynamo_region.clone(),
        batch_max_concurrency: cli.dynamo_max_concurrency,
        credentials,
    };
    let store = DynamoMetaStore::new(config)
        .await
        .context("building DynamoDB meta store")?;
    if cli.dynamo_create_table {
        info!("--dynamo-create-table set: ensuring the metadata table exists");
        store
            .create_table()
            .await
            .context("creating DynamoDB meta table")?;
    }
    Ok(store)
}

async fn fetch_block(
    reader: &BlockDataReaderErased,
    n: u64,
    fetch_traces: bool,
) -> Result<(u64, Block, BlockReceipts, BlockTraces)> {
    let (block, receipts, traces) = tokio::try_join!(
        reader.get_block_by_number(n),
        reader.get_block_receipts(n),
        async {
            if fetch_traces {
                reader
                    .try_get_block_traces(n)
                    .await
                    .map(|opt| opt.unwrap_or_default())
            } else {
                Ok(BlockTraces::default())
            }
        },
    )
    .with_context(|| format!("fetching block {n}"))?;
    Ok((n, block, receipts, traces))
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
    fetch_traces: bool,
) -> Result<(u64, Block, BlockReceipts, BlockTraces)> {
    let mut backoff = initial_backoff;
    // Wall time across all attempts: a retried fetch's "latency" is what
    // the consumer actually waits for, not just the successful attempt.
    let started = Instant::now();
    progress.started.fetch_add(1, Ordering::Relaxed);
    for attempt in 0..=max_retries {
        match fetch_block(reader, n, fetch_traces).await {
            Ok(item) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                progress.completed.fetch_add(1, Ordering::Relaxed);
                let mut stats = stats.lock().expect("fetch stats poisoned");
                stats.completed = stats.completed.saturating_add(1);
                stats.durations_ms.push(elapsed_ms);
                return Ok(item);
            }
            Err(e) if attempt < max_retries => {
                stats.lock().expect("fetch stats poisoned").retry_attempts += 1;
                debug!(
                    block = n,
                    attempt = attempt + 1,
                    max_attempts = max_retries + 1,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "fetch failed, retrying after backoff"
                );
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2);
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("fetch_block({n}) failed after {} attempts", max_retries + 1)
                });
            }
        }
    }
    unreachable!("loop exits via Ok or final Err arm")
}

#[derive(Default)]
struct FetchStats {
    durations_ms: Vec<u64>,
    completed: u64,
    retry_attempts: u64,
}

#[derive(Default)]
struct FetchProgress {
    started: AtomicU64,
    completed: AtomicU64,
}

#[derive(Clone, Copy)]
struct FetchProgressSnapshot {
    started: u64,
    completed: u64,
}

impl FetchProgress {
    fn snapshot(&self) -> FetchProgressSnapshot {
        FetchProgressSnapshot {
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
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

/// Dynamic concurrency limiter driven by AIMD on per-window retry rate.
///
/// The semaphore is the actual gate: fetchers `acquire_owned()` a permit
/// before issuing the request and drop it on return. `tune()` mutates the
/// permit pool — `add_permits` to grow, spawned `acquire_many.forget` to
/// shrink (the shrink awaits permits to free, so concurrency decreases
/// asymptotically as in-flight requests finish).
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

    /// AIMD step on observed retry rate. Decrease is tiered so mild
    /// throttling produces a mild correction; only severe throttling
    /// halves:
    /// - retries above 20% → ×0.5 (severe — halve)
    /// - retries above 5%  → ×0.75 (moderate — back off 25%)
    /// - retries above 1%  → ×0.9 (mild — back off 10%)
    /// - retries below 1%  → grow by max(2, cur/8) (additive, ~12.5%)
    /// - else hold
    ///
    /// Returns the new concurrency (== current if no change). When
    /// `min == max` this is a fixed-permit no-op.
    fn tune(&self, retry_rate: f64) -> usize {
        let cur = self.current.load(Ordering::Relaxed);
        let new = if retry_rate > 0.20 {
            (cur / 2).max(self.min)
        } else if retry_rate > 0.05 {
            (cur * 3 / 4).max(self.min)
        } else if retry_rate > 0.01 {
            (cur * 9 / 10).max(self.min)
        } else if cur < self.max {
            cur.saturating_add((cur / 8).max(2)).min(self.max)
        } else {
            cur
        };
        if new == cur {
            return cur;
        }
        self.current.store(new, Ordering::Relaxed);
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
        new
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
