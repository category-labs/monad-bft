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
use futures::{stream, FutureExt, StreamExt};
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
    store::{
        BlobCompressionConfig, BlobCompressionSnapshot, BlobCompressionStats, BlobCompressionStore,
        CacheConfig, FjallStore, FjallTuning, MetaStore, MetaStoreCas, TableId,
    },
    BlockRecord, CallKind, FamilyWindowRecord, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, IngestPlan, IngestTrace, IngestTx, IoRetryPolicy, MonadChainDataService,
    PrimaryId, QueryLimits,
};
use opentelemetry::KeyValue;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "chain-data-ingest",
    about = "Stream blocks + receipts from a monad-archive source into a local chain-data store"
)]
pub struct Cli {
    /// fjall data directory for both meta and blob stores. Use
    /// `--meta-data-dir` and `--blob-data-dir` to place them in separate DBs.
    #[arg(long, conflicts_with_all = ["meta_data_dir", "blob_data_dir"])]
    pub data_dir: Option<PathBuf>,

    /// fjall data directory for metadata tables. Created on first run.
    #[arg(long)]
    pub meta_data_dir: Option<PathBuf>,

    /// fjall data directory for blob tables. Created on first run.
    #[arg(long)]
    pub blob_data_dir: Option<PathBuf>,

    /// Archive source. Examples: `"fs /var/lib/monad-archive"`,
    /// `"aws my-bucket"`, `"mongodb mongodb://host:27017 dbname"`.
    /// See `monad_archive::cli::BlockDataReaderArgs` for the full grammar.
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    pub block_data_source: BlockDataReaderArgs,

    /// First block to ingest (inclusive). Defaults to `published_head + 1`
    /// (i.e. `1` on a fresh data directory). When provided explicitly, must
    /// match that value — the flag exists for assertion in scripted runs.
    #[arg(long)]
    pub start: Option<u64>,

    /// Last block to ingest (inclusive). Mutually exclusive with `--count`.
    /// If neither `--end` nor `--count` is set, the ingester follows the
    /// archive source forever and polls for each next block as it appears.
    #[arg(long, conflicts_with = "count")]
    pub end: Option<u64>,

    /// Number of blocks to ingest from the resolved start (i.e.
    /// `published_head + 1`). End block is derived as `start + count - 1`.
    /// Mutually exclusive with `--end`. If neither `--end` nor `--count`
    /// is set, the ingester follows the archive source forever.
    #[arg(long, conflicts_with = "end")]
    pub count: Option<u64>,

    /// Optional OTel collector endpoint for archive-side metrics. Off by
    /// default; archive readers still build without it.
    #[arg(long)]
    pub otel_endpoint: Option<String>,

    /// How often to log progress, in blocks.
    #[arg(long, default_value_t = 1000)]
    pub log_every: u64,

    /// Maximum number of block fetches in flight against the archive
    /// concurrently. Each in-flight slot issues block + receipts + traces
    /// in parallel via `try_join!`, so peak archive request concurrency
    /// is roughly `3 * concurrency`. Increase for high-latency archives
    /// (e.g. S3); leave low for local FS where queueing buys nothing.
    /// Memory ceiling is bounded by this many fetched-but-not-yet-ingested
    /// blocks held in the prefetch buffer.
    #[arg(long, default_value_t = 512)]
    pub concurrency: usize,

    /// Number of retry attempts after a fetch failure, per block. The
    /// first attempt is not counted, so `--max-retries 5` means up to 6
    /// total attempts. Set to 0 to disable retry. Only fetches retry —
    /// transform and ingest errors are deterministic and bail
    /// immediately.
    #[arg(long, default_value_t = 5)]
    pub max_retries: u32,

    /// Initial backoff between retry attempts, in milliseconds. Doubles
    /// after each failure (exponential, no jitter), so the default 200ms
    /// with 5 retries waits ~6.2s in total worst case.
    #[arg(long, default_value_t = 200)]
    pub retry_backoff_ms: u64,

    /// Poll interval while following the archive source and waiting for the
    /// next block to be uploaded.
    #[arg(long, default_value_t = 1000)]
    pub live_poll_ms: u64,

    /// Enable adaptive concurrency. When set, `--concurrency` is the
    /// starting value; an AIMD controller scales it within
    /// `[--min-concurrency, --max-concurrency]` based on retry rate per
    /// window (halve above 5% retries, additive increase below 1%).
    #[arg(long)]
    pub autotune: bool,

    /// Lower bound on adaptive concurrency. Ignored unless `--autotune`.
    #[arg(long, default_value_t = 1)]
    pub min_concurrency: usize,

    /// Upper bound on adaptive concurrency. Ignored unless `--autotune`.
    #[arg(long, default_value_t = 5000)]
    pub max_concurrency: usize,

    /// Number of ordered fetch futures to keep buffered ahead of ingest.
    /// This is separate from `--concurrency`: the buffer may hold many
    /// pending/completed block fetches while the concurrency semaphore caps
    /// active archive requests.
    #[arg(long)]
    pub fetch_buffer: Option<usize>,

    /// Skip fetching and ingesting execution traces. With this flag set,
    /// `Family::Trace` stays empty for every ingested block, and both
    /// `query_traces` and `query_transfers` will return zero results for
    /// those blocks (transfers is a derived view over traces). Drops the
    /// per-block archive fanout from 3 requests to 2.
    #[arg(long)]
    pub no_traces: bool,

    /// Maximum number of consecutive ready blocks coalesced into one ingest
    /// batch. The ingester waits only when no fetched block is ready, then
    /// drains already-ready fetched blocks up to this ceiling.
    #[arg(
        long = "max-batch",
        visible_alias = "max-batch-size",
        default_value_t = 1
    )]
    pub max_batch_size: usize,

    /// Number of fully planned ingest batches allowed to queue in front of
    /// the IO worker. This is the backpressure boundary between planning
    /// and write/CAS publication.
    #[arg(long, default_value_t = 2)]
    pub plan_buffer: usize,

    /// Number of retries for non-CAS meta/blob write failures. CAS is never
    /// retried; a CAS conflict means this writer lost the publication lease.
    #[arg(long, default_value_t = 0)]
    pub io_max_retries: usize,

    /// Retry non-CAS IO failures forever. Overrides --io-max-retries.
    #[arg(long)]
    pub io_retry_forever: bool,

    /// Initial non-CAS IO retry backoff, in milliseconds.
    #[arg(long, default_value_t = 200)]
    pub io_retry_backoff_ms: u64,

    /// Maximum non-CAS IO retry backoff, in milliseconds.
    #[arg(long, default_value_t = 10_000)]
    pub io_retry_max_backoff_ms: u64,

    /// fjall total-journal cap in MiB. Default 512 matches fjall's default;
    /// raise (e.g. 4096 or 8192) for high-throughput backfill to reduce
    /// journal-rotation pressure. Must be >= 64.
    #[arg(long, default_value_t = 512)]
    pub fjall_journal_mib: u64,

    /// Per-keyspace memtable cap in MiB. Default 64 matches fjall's default;
    /// raise (e.g. 256) so each keyspace amortizes more writes per flush.
    /// Total memtable footprint at steady state is roughly N_keyspaces * this.
    #[arg(long, default_value_t = 64)]
    pub fjall_memtable_mib: u64,

    /// fjall flush/compaction worker thread count. None lets fjall pick
    /// (min(CPU, 4)). Raise for many-core boxes under sustained write load.
    #[arg(long)]
    pub fjall_workers: Option<usize>,

    /// Per-table read-cache budget in MiB. Defaults to the budget implied by
    /// `CacheConfig::default()`; raising linearly scales every table's entry
    /// count. Sizing uses a per-table estimate of metadata + payload (bitmap
    /// fragments and bitmap page blobs dominate at ~8 KiB each; other tables
    /// are dominated by small metadata), so the actual cache footprint should
    /// roughly match the requested budget. `--cache-mib 0` disables caches
    /// entirely (compile-time skip).
    #[arg(long)]
    pub cache_mib: Option<usize>,

    /// App-level blob compression for newly written block blobs. Reads remain
    /// compatible with old raw blobs because compressed values carry a magic
    /// header and raw values are left unwrapped.
    #[arg(long, value_enum, default_value_t = BlobCompressionArg::None)]
    pub blob_compression: BlobCompressionArg,

    /// zstd compression level used when `--blob-compression zstd`.
    #[arg(long, default_value_t = 1)]
    pub blob_compression_level: i32,

    /// Minimum blob size eligible for app-level compression.
    #[arg(long, default_value_t = 1024)]
    pub blob_compression_min_bytes: usize,

    /// Fetch and ingest into in-memory stores to profile blob compression
    /// ratio/cost without writing anything into fjall. Requires --start.
    #[arg(long)]
    pub profile_blob_compression_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum BlobCompressionArg {
    None,
    Zstd,
}

impl Cli {
    pub fn live(block_data_source: BlockDataReaderArgs) -> Self {
        Self {
            data_dir: None,
            meta_data_dir: None,
            blob_data_dir: None,
            block_data_source,
            start: None,
            end: None,
            count: None,
            otel_endpoint: None,
            log_every: 1000,
            concurrency: 512,
            max_retries: 5,
            retry_backoff_ms: 200,
            live_poll_ms: 1000,
            autotune: false,
            min_concurrency: 1,
            max_concurrency: 5000,
            fetch_buffer: None,
            no_traces: false,
            max_batch_size: 1,
            plan_buffer: 2,
            io_max_retries: 0,
            io_retry_forever: false,
            io_retry_backoff_ms: 200,
            io_retry_max_backoff_ms: 10_000,
            fjall_journal_mib: 512,
            fjall_memtable_mib: 64,
            fjall_workers: None,
            cache_mib: None,
            blob_compression: BlobCompressionArg::None,
            blob_compression_level: 1,
            blob_compression_min_bytes: 1024,
            profile_blob_compression_only: false,
        }
    }
}

struct StoreDirs {
    meta: PathBuf,
    blob: PathBuf,
    same_dir: bool,
}

pub type ChainDataService = MonadChainDataService<FjallStore, BlobCompressionStore<FjallStore>>;

pub type SharedChainDataService = Arc<ChainDataService>;

pub struct OpenChainDataIngest {
    pub service: SharedChainDataService,
    meta_store: FjallStore,
    blob_fjall_store: FjallStore,
    store_dirs: StoreDirs,
    blob_compression_stats: BlobCompressionStats,
}

#[derive(Debug, Clone)]
pub struct ChainDataStoreConfig {
    pub data_dir: Option<PathBuf>,
    pub meta_data_dir: Option<PathBuf>,
    pub blob_data_dir: Option<PathBuf>,
    pub query_limits: QueryLimits,
    pub cache_mib: Option<usize>,
    pub blob_compression: BlobCompressionArg,
    pub blob_compression_level: i32,
    pub blob_compression_min_bytes: usize,
    pub fjall_journal_mib: u64,
    pub fjall_memtable_mib: u64,
    pub fjall_workers: Option<usize>,
}

impl ChainDataStoreConfig {
    pub fn from_cli(cli: &Cli, query_limits: QueryLimits) -> Self {
        Self {
            data_dir: cli.data_dir.clone(),
            meta_data_dir: cli.meta_data_dir.clone(),
            blob_data_dir: cli.blob_data_dir.clone(),
            query_limits,
            cache_mib: cli.cache_mib,
            blob_compression: cli.blob_compression,
            blob_compression_level: cli.blob_compression_level,
            blob_compression_min_bytes: cli.blob_compression_min_bytes,
            fjall_journal_mib: cli.fjall_journal_mib,
            fjall_memtable_mib: cli.fjall_memtable_mib,
            fjall_workers: cli.fjall_workers,
        }
    }
}

impl Default for ChainDataStoreConfig {
    fn default() -> Self {
        Self {
            data_dir: None,
            meta_data_dir: None,
            blob_data_dir: None,
            query_limits: QueryLimits::UNLIMITED,
            cache_mib: None,
            blob_compression: BlobCompressionArg::None,
            blob_compression_level: 1,
            blob_compression_min_bytes: 1024,
            fjall_journal_mib: 512,
            fjall_memtable_mib: 64,
            fjall_workers: None,
        }
    }
}

fn resolve_store_dirs_from_paths(
    data_dir: Option<&Path>,
    meta_data_dir: Option<&Path>,
    blob_data_dir: Option<&Path>,
) -> Result<StoreDirs> {
    match (data_dir, meta_data_dir, blob_data_dir) {
        (Some(data_dir), None, None) => Ok(StoreDirs {
            meta: data_dir.to_path_buf(),
            blob: data_dir.to_path_buf(),
            same_dir: true,
        }),
        (None, Some(meta), Some(blob)) => Ok(StoreDirs {
            meta: meta.to_path_buf(),
            blob: blob.to_path_buf(),
            same_dir: same_store_dir(meta, blob),
        }),
        (Some(_), _, _) => {
            bail!("--data-dir cannot be combined with --meta-data-dir or --blob-data-dir")
        }
        (None, _, _) => {
            bail!("provide either --data-dir or both --meta-data-dir and --blob-data-dir")
        }
    }
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

pub async fn run(cli: Cli) -> Result<()> {
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
        bail!("--max-batch must be >= 1");
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
    if cli.profile_blob_compression_only {
        return profile_blob_compression_only(&cli).await;
    }
    let opened =
        open_fjall_chain_data(ChainDataStoreConfig::from_cli(&cli, QueryLimits::UNLIMITED))?;

    run_with_opened(cli, opened).await
}

pub fn open_fjall_chain_data(config: ChainDataStoreConfig) -> Result<OpenChainDataIngest> {
    if config.fjall_journal_mib < 64 {
        bail!("--fjall-journal-mib must be >= 64");
    }
    let store_dirs = resolve_store_dirs_from_paths(
        config.data_dir.as_deref(),
        config.meta_data_dir.as_deref(),
        config.blob_data_dir.as_deref(),
    )?;

    let tuning = FjallTuning {
        max_journaling_size_bytes: config.fjall_journal_mib * 1024 * 1024,
        max_memtable_size_bytes: config.fjall_memtable_mib * 1024 * 1024,
        worker_threads: config.fjall_workers,
    };
    // fjall persists max_memtable_size per-keyspace at creation, so the
    // value baked in at first open wins on every subsequent reopen — the
    // flag silently does nothing on existing data dirs. Journal cap and
    // worker_threads are not persisted and always take effect.
    let meta_dir_fresh = data_dir_fresh(&store_dirs.meta);
    let blob_dir_fresh = data_dir_fresh(&store_dirs.blob);
    info!(
        fjall_journal_mib = config.fjall_journal_mib,
        fjall_memtable_mib = config.fjall_memtable_mib,
        fjall_workers = ?config.fjall_workers,
        meta_data_dir = %store_dirs.meta.display(),
        blob_data_dir = %store_dirs.blob.display(),
        meta_dir_fresh,
        blob_dir_fresh,
        "fjall tuning"
    );
    if config.fjall_memtable_mib != 64 && (!meta_dir_fresh || !blob_dir_fresh) {
        warn!(
            requested_memtable_mib = config.fjall_memtable_mib,
            meta_dir_fresh,
            blob_dir_fresh,
            "--fjall-memtable-mib is persisted per-keyspace at first creation; existing data dirs will keep their baked-in values. Journal cap and worker_threads still apply."
        );
    }
    let meta_store = FjallStore::open(&store_dirs.meta, tuning)
        .with_context(|| format!("opening fjall meta store at {}", store_dirs.meta.display()))?;
    let blob_fjall_store = if store_dirs.same_dir {
        meta_store.clone()
    } else {
        FjallStore::open(&store_dirs.blob, tuning)
            .with_context(|| format!("opening fjall blob store at {}", store_dirs.blob.display()))?
    };
    let baseline_cache = CacheConfig::default();
    let baseline_total_mib = baseline_cache.approx_total_mib().max(1);
    let cache_config = match config.cache_mib {
        None => baseline_cache,
        Some(0) => baseline_cache.scale(0, 1),
        Some(target_mib) => baseline_cache.scale(target_mib, baseline_total_mib),
    };
    info!(
        cache_mib_requested = ?config.cache_mib,
        baseline_total_mib,
        scaled_total_entries = cache_config.total_entries(),
        "cache config"
    );
    let blob_compression = match config.blob_compression {
        BlobCompressionArg::None => BlobCompressionConfig::none(),
        BlobCompressionArg::Zstd => BlobCompressionConfig::zstd(
            config.blob_compression_level,
            config.blob_compression_min_bytes,
        ),
    };
    let blob_compression_stats = BlobCompressionStats::default();
    let blob_store = BlobCompressionStore::new(
        blob_fjall_store.clone(),
        blob_compression,
        blob_compression_stats.clone(),
    );
    info!(
        blob_compression = ?config.blob_compression,
        blob_compression_level = config.blob_compression_level,
        blob_compression_min_bytes = config.blob_compression_min_bytes,
        "blob compression config"
    );
    let service = Arc::new(MonadChainDataService::with_cache_config(
        meta_store.clone(),
        blob_store,
        config.query_limits,
        cache_config,
    ));

    Ok(OpenChainDataIngest {
        service,
        meta_store,
        blob_fjall_store,
        store_dirs,
        blob_compression_stats,
    })
}

pub async fn run_with_opened(cli: Cli, opened: OpenChainDataIngest) -> Result<()> {
    let OpenChainDataIngest {
        service,
        meta_store,
        blob_fjall_store,
        store_dirs,
        blob_compression_stats,
    } = opened;

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
        (Some(e), None) => Some(e),
        (None, Some(c)) => {
            let Some(e) = start.checked_add(c - 1) else {
                bail!("start + count - 1 overflows u64 (start={start}, count={c})");
            };
            Some(e)
        }
        (None, None) => None,
        _ => unreachable!("clap prevents combining --end and --count"),
    };
    if let Some(end) = end {
        if start > end {
            bail!("start ({}) must be <= end ({})", start, end);
        }
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
        end = ?end,
        concurrency = cli.concurrency,
        fetch_buffer,
        max_batch = cli.max_batch_size,
        plan_buffer = cli.plan_buffer,
        io_retry_forever = cli.io_retry_forever,
        io_max_retries = cli.io_max_retries,
        meta_data_dir = %store_dirs.meta.display(),
        blob_data_dir = %store_dirs.blob.display(),
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
        let max_batch_size = cli.max_batch_size;
        let live_poll = Duration::from_millis(cli.live_poll_ms);
        tokio::spawn(async move {
            let mut pending: Vec<(u64, FinalizedBlock)> = Vec::with_capacity(max_batch_size);
            if let Some(end) = end {
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

                loop {
                    pending.clear();
                    let wait_started = Instant::now();
                    let Some(item) = fetch_stream.next().await else {
                        break;
                    };
                    let first_wait_ms = wait_started.elapsed().as_millis() as u64;
                    push_fetched_block(&mut pending, item?, &fetch_consumed_total)?;
                    while pending.len() < max_batch_size {
                        match fetch_stream.next().now_or_never() {
                            Some(Some(item)) => {
                                push_fetched_block(&mut pending, item?, &fetch_consumed_total)?;
                            }
                            Some(None) | None => break,
                        }
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
            } else {
                let fetch_stream = stream::iter(start..)
                    .map(move |n| {
                        let reader = reader.clone();
                        let stats = fetch_stats.clone();
                        let progress = fetch_progress.clone();
                        let permits = permits.clone();
                        async move {
                            wait_for_archive_block(&reader, n, live_poll).await?;
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

                loop {
                    pending.clear();
                    let wait_started = Instant::now();
                    let Some(item) = fetch_stream.next().await else {
                        break;
                    };
                    let first_wait_ms = wait_started.elapsed().as_millis() as u64;
                    push_fetched_block(&mut pending, item?, &fetch_consumed_total)?;
                    while pending.len() < max_batch_size {
                        match fetch_stream.next().now_or_never() {
                            Some(Some(item)) => {
                                push_fetched_block(&mut pending, item?, &fetch_consumed_total)?;
                            }
                            Some(None) | None => break,
                        }
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
    let mut last_blob_compression = BlobCompressionSnapshot::default();
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

        let flush = end.is_some_and(|end| n == end)
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
            let fjall_stats =
                match collect_fjall_stats(&meta_store, &blob_fjall_store, store_dirs.same_dir) {
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
            let blob_compression_window = blob_compression_stats
                .snapshot()
                .saturating_sub(&last_blob_compression);
            last_blob_compression = blob_compression_stats.snapshot();

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
                stage_a_log_plan_work_total_ms = phase_summary.stage_a_log_plan_ms_total,
                stage_a_tx_plan_work_total_ms = phase_summary.stage_a_tx_plan_ms_total,
                stage_a_trace_plan_work_total_ms = phase_summary.stage_a_trace_plan_ms_total,
                stage_a_delta_work_total_ms = phase_summary.stage_a_delta_ms_total,
                stage_a_session_stage_wall_total_ms = phase_summary.stage_a_session_stage_ms_total,
                stage_a_bitmap_fragment_count = phase_summary.stage_a_bitmap_fragment_count,
                stage_a_hash_location_count = phase_summary.stage_a_hash_location_count,
                stage_a_dir_fragments_total = phase_summary.stage_a_dir_fragments_total,
                stage_a_dir_fragments_written = phase_summary.stage_a_dir_fragments_written,
                stage_a_dir_fragments_skipped = phase_summary.stage_a_dir_fragments_skipped,
                stage_a_bitmap_fragments_total = phase_summary.stage_a_bitmap_fragments_total,
                stage_a_bitmap_fragments_written = phase_summary.stage_a_bitmap_fragments_written,
                stage_a_bitmap_fragments_skipped = phase_summary.stage_a_bitmap_fragments_skipped,
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
                reads_dir_list_p50_ms = phase_summary.reads_dir_list_p50,
                reads_dir_list_p99_ms = phase_summary.reads_dir_list_p99,
                reads_dir_get_p50_ms = phase_summary.reads_dir_get_p50,
                reads_dir_get_p99_ms = phase_summary.reads_dir_get_p99,
                reads_bitmap_open_streams_p50_ms = phase_summary.reads_bitmap_open_streams_p50,
                reads_bitmap_open_streams_p99_ms = phase_summary.reads_bitmap_open_streams_p99,
                reads_bitmap_list_p50_ms = phase_summary.reads_bitmap_list_p50,
                reads_bitmap_list_p99_ms = phase_summary.reads_bitmap_list_p99,
                reads_bitmap_get_p50_ms = phase_summary.reads_bitmap_get_p50,
                reads_bitmap_get_p99_ms = phase_summary.reads_bitmap_get_p99,
                reads_bitmap_shape_p50_ms = phase_summary.reads_bitmap_shape_p50,
                reads_bitmap_shape_p99_ms = phase_summary.reads_bitmap_shape_p99,
                reads_bitmap_index_p50_ms = phase_summary.reads_bitmap_index_p50,
                reads_bitmap_index_p99_ms = phase_summary.reads_bitmap_index_p99,
                reads_bitmap_compact_p50_ms = phase_summary.reads_bitmap_compact_p50,
                reads_bitmap_compact_p99_ms = phase_summary.reads_bitmap_compact_p99,
                reads_dir_index_p50_ms = phase_summary.reads_dir_index_p50,
                reads_dir_index_p99_ms = phase_summary.reads_dir_index_p99,
                reads_dir_decode_p50_ms = phase_summary.reads_dir_decode_p50,
                reads_dir_decode_p99_ms = phase_summary.reads_dir_decode_p99,
                reads_unaccounted_p50_ms = phase_summary.reads_unaccounted_p50,
                reads_unaccounted_p99_ms = phase_summary.reads_unaccounted_p99,
                reads_bitmap_open_streams_total_ms =
                    phase_summary.reads_bitmap_open_streams_ms_total,
                reads_bitmap_shape_total_ms = phase_summary.reads_bitmap_shape_ms_total,
                reads_bitmap_union_total_ms = phase_summary.reads_bitmap_union_ms_total,
                reads_bitmap_frontier_total_ms = phase_summary.reads_bitmap_frontier_ms_total,
                reads_bitmap_open_write_total_ms = phase_summary.reads_bitmap_open_write_ms_total,
                reads_bitmap_index_total_ms = phase_summary.reads_bitmap_index_ms_total,
                reads_bitmap_compact_total_ms = phase_summary.reads_bitmap_compact_ms_total,
                reads_bitmap_compact_wall_total_ms =
                    phase_summary.reads_bitmap_compact_wall_ms_total,
                reads_dir_index_total_ms = phase_summary.reads_dir_index_ms_total,
                reads_dir_decode_total_ms = phase_summary.reads_dir_decode_ms_total,
                reads_unaccounted_total_ms = phase_summary.reads_unaccounted_ms_total,
                reads_bitmap_open_streams_work_total_ms =
                    phase_summary.reads_bitmap_open_streams_ms_total,
                reads_bitmap_shape_work_total_ms = phase_summary.reads_bitmap_shape_ms_total,
                reads_bitmap_union_work_total_ms = phase_summary.reads_bitmap_union_ms_total,
                reads_bitmap_frontier_work_total_ms = phase_summary.reads_bitmap_frontier_ms_total,
                reads_bitmap_open_write_work_total_ms =
                    phase_summary.reads_bitmap_open_write_ms_total,
                reads_bitmap_index_work_total_ms = phase_summary.reads_bitmap_index_ms_total,
                reads_bitmap_compact_work_total_ms = phase_summary.reads_bitmap_compact_ms_total,
                reads_dir_index_work_total_ms = phase_summary.reads_dir_index_ms_total,
                reads_dir_decode_work_total_ms = phase_summary.reads_dir_decode_ms_total,
                reads_unaccounted_work_total_ms = phase_summary.reads_unaccounted_ms_total,
                reads_dir_list_count = phase_summary.reads_dir_list_count,
                reads_dir_get_count = phase_summary.reads_dir_get_count,
                reads_dir_fragment_count = phase_summary.reads_dir_fragment_count,
                reads_bitmap_open_streams_count = phase_summary.reads_bitmap_open_streams_count,
                reads_bitmap_list_count = phase_summary.reads_bitmap_list_count,
                reads_bitmap_get_count = phase_summary.reads_bitmap_get_count,
                reads_bitmap_fragment_count = phase_summary.reads_bitmap_fragment_count,
                reads_bitmap_fragment_bytes = phase_summary.reads_bitmap_fragment_bytes,
                reads_bitmap_frontier_stream_count =
                    phase_summary.reads_bitmap_frontier_stream_count,
                reads_bitmap_union_page_count = phase_summary.reads_bitmap_union_page_count,
                reads_bitmap_union_stream_count = phase_summary.reads_bitmap_union_stream_count,
                reads_bitmap_touched_page_count = phase_summary.reads_bitmap_touched_page_count,
                reads_bitmap_touched_stream_count = phase_summary.reads_bitmap_touched_stream_count,
                reads_bitmap_final_open_stream_count =
                    phase_summary.reads_bitmap_final_open_stream_count,
                reads_bitmap_compact_count = phase_summary.reads_bitmap_compact_count,
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
                blob_compression_raw_bytes = blob_compression_window.raw_input_bytes,
                blob_compression_stored_bytes = blob_compression_window.stored_output_bytes,
                blob_compression_ratio = round_2(blob_compression_window.ratio()),
                blob_compression_compressed_count = blob_compression_window.compressed_count,
                blob_compression_raw_count = blob_compression_window.raw_count,
                blob_compression_encode_wall_total_ms =
                    round_2(blob_compression_window.encode_wall_us as f64 / 1_000.0),
                blob_compression_encode_work_total_ms =
                    round_2(blob_compression_window.encode_work_us as f64 / 1_000.0),
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

    fetch_handle
        .await
        .context("fetch worker panicked")?
        .context("fetch worker failed")?;
    plan_handle
        .await
        .context("planning worker panicked")?
        .context("planning worker failed")?;
    io_handle
        .await
        .context("IO worker panicked")?
        .context("IO worker failed")?;

    info!(end = ?end, total_txs, total_logs, total_traces, "ingest complete");
    Ok(())
}

async fn profile_blob_compression_only(cli: &Cli) -> Result<()> {
    if cli.blob_compression == BlobCompressionArg::None {
        bail!("--profile-blob-compression-only requires --blob-compression zstd");
    }
    let Some(start) = cli.start else {
        bail!("--profile-blob-compression-only requires explicit --start");
    };
    let end = match (cli.end, cli.count) {
        (Some(e), None) => e,
        (None, Some(c)) => {
            let Some(e) = start.checked_add(c - 1) else {
                bail!("start + count - 1 overflows u64 (start={start}, count={c})");
            };
            e
        }
        _ => unreachable!("clap enforces exactly one of --end or --count"),
    };
    if start > end {
        bail!("start ({}) must be <= end ({})", start, end);
    }

    let metrics = Metrics::new(
        cli.otel_endpoint.as_deref(),
        "chain-data-ingest-compression-profile",
        "0".to_string(),
        Duration::from_secs(60),
    )
    .context("building metrics")?;
    let reader = cli
        .block_data_source
        .build(&metrics)
        .await
        .context("building block data reader")?;

    let blob_compression =
        BlobCompressionConfig::zstd(cli.blob_compression_level, cli.blob_compression_min_bytes);
    let compression_stats = BlobCompressionStats::default();
    let meta_store = InMemoryMetaStore::default();
    if start > 1 {
        let (_, prev_block, prev_receipts, prev_traces) = fetch_block(&reader, start - 1, false)
            .await
            .with_context(|| {
                format!(
                    "fetching previous block {} for in-memory continuity seed",
                    start - 1
                )
            })?;
        let prev = into_finalized_block(prev_block, prev_receipts, prev_traces)
            .with_context(|| format!("transforming previous block {}", start - 1))?;
        seed_profile_publication(&meta_store, &prev).await?;
    }

    let blob_store = BlobCompressionStore::new(
        InMemoryBlobStore::default(),
        blob_compression,
        compression_stats.clone(),
    );
    let service = MonadChainDataService::with_cache_config(
        meta_store,
        blob_store,
        QueryLimits::UNLIMITED,
        CacheConfig::default().scale(0, 1),
    );

    let fetch_buffer = cli.fetch_buffer.unwrap_or(cli.concurrency);
    if fetch_buffer == 0 {
        bail!("--fetch-buffer must be >= 1");
    }
    let permits = Arc::new(Semaphore::new(cli.concurrency));
    let fetch_stats = Arc::new(Mutex::new(FetchStats::default()));
    let fetch_progress = Arc::new(FetchProgress::default());
    let max_retries = cli.max_retries;
    let initial_backoff = Duration::from_millis(cli.retry_backoff_ms);
    let fetch_traces = !cli.no_traces;

    info!(
        start,
        end,
        concurrency = cli.concurrency,
        fetch_buffer,
        max_batch = cli.max_batch_size,
        blob_compression = ?cli.blob_compression,
        blob_compression_level = cli.blob_compression_level,
        blob_compression_min_bytes = cli.blob_compression_min_bytes,
        "starting in-memory blob compression profile"
    );

    let fetch_stream = {
        let fetch_stats = fetch_stats.clone();
        let fetch_progress = fetch_progress.clone();
        stream::iter(start..=end)
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
            .buffered(fetch_buffer)
    };
    futures::pin_mut!(fetch_stream);

    let started = Instant::now();
    let mut total_blocks = 0_u64;
    let mut total_txs = 0_u64;
    let mut total_logs = 0_u64;
    let mut total_traces = 0_u64;
    let mut ingest_ms = Vec::new();
    let mut phase = PhaseStats::default();
    let mut pending: Vec<(u64, FinalizedBlock)> = Vec::with_capacity(cli.max_batch_size);
    let mut fetch_consumed_total = 0_u64;

    loop {
        pending.clear();
        let Some(item) = fetch_stream.next().await else {
            break;
        };
        let (n, block, receipts, traces) = item?;
        fetch_consumed_total = fetch_consumed_total.saturating_add(1);
        let finalized = into_finalized_block(block, receipts, traces)
            .with_context(|| format!("transforming block {n}"))?;
        pending.push((n, finalized));
        while pending.len() < cli.max_batch_size {
            let Some(Some(item)) = fetch_stream.next().now_or_never() else {
                break;
            };
            let (n, block, receipts, traces) = item?;
            fetch_consumed_total = fetch_consumed_total.saturating_add(1);
            let finalized = into_finalized_block(block, receipts, traces)
                .with_context(|| format!("transforming block {n}"))?;
            pending.push((n, finalized));
        }
        let last_n = pending.last().expect("pending non-empty").0;
        let blocks: Vec<FinalizedBlock> = pending.iter().map(|(_, b)| b.clone()).collect();
        let ingest_started = Instant::now();
        let (outcomes, timings) = service
            .ingest_blocks(blocks)
            .await
            .with_context(|| format!("profiling ingest batch ending at block {last_n}"))?;
        ingest_ms.push(ingest_started.elapsed().as_millis() as u64);
        phase.record(&timings);
        for outcome in outcomes {
            total_blocks += 1;
            total_txs += outcome.written_txs as u64;
            total_logs += outcome.written_logs as u64;
            total_traces += outcome.written_traces as u64;
        }
    }

    let fetch_window = fetch_stats.lock().expect("fetch stats poisoned");
    let fetch_progress_snapshot = fetch_progress.snapshot();
    let fetch_completed_backlog = fetch_progress_snapshot
        .completed
        .saturating_sub(fetch_consumed_total);
    let fetch_in_flight = fetch_progress_snapshot
        .started
        .saturating_sub(fetch_progress_snapshot.completed);
    let phase_summary = phase.summary();
    let compression = compression_stats.snapshot();
    let elapsed_secs = started.elapsed().as_secs_f64().max(1e-9);
    info!(
        total_blocks,
        total_txs,
        total_logs,
        total_traces,
        elapsed_secs = round_2(elapsed_secs),
        blocks_per_sec = round_2(total_blocks as f64 / elapsed_secs),
        fetch_completed_per_sec = round_2(fetch_window.completed as f64 / elapsed_secs),
        fetch_started_total = fetch_progress_snapshot.started,
        fetch_completed_total = fetch_progress_snapshot.completed,
        fetch_consumed_total,
        fetch_completed_backlog,
        fetch_in_flight,
        fetch_buffer_capacity = fetch_buffer,
        fetch_p50_ms = percentile(&fetch_window.durations_ms, 0.50),
        fetch_p99_ms = percentile(&fetch_window.durations_ms, 0.99),
        ingest_p50_ms = percentile(&ingest_ms, 0.50),
        ingest_p99_ms = percentile(&ingest_ms, 0.99),
        stage_a_wall_total_ms = phase_summary.stage_a_wall_ms_total,
        commit_a_blob_wall_total_ms = phase_summary.commit_a_blob_wall_ms_total,
        phase_a_write_ops_total = phase_summary.phase_a_write_ops_total,
        phase_a_write_bytes_total = phase_summary.phase_a_write_bytes_total,
        phase_a_write_ops_by_table = %phase_summary.phase_a_write_ops_by_table,
        phase_a_write_bytes_by_table = %phase_summary.phase_a_write_bytes_by_table,
        phase_b_write_ops_total = phase_summary.phase_b_write_ops_total,
        phase_b_write_bytes_total = phase_summary.phase_b_write_bytes_total,
        phase_b_write_ops_by_table = %phase_summary.phase_b_write_ops_by_table,
        phase_b_write_bytes_by_table = %phase_summary.phase_b_write_bytes_by_table,
        blob_compression_raw_bytes = compression.raw_input_bytes,
        blob_compression_stored_bytes = compression.stored_output_bytes,
        blob_compression_saved_bytes = compression
            .raw_input_bytes
            .saturating_sub(compression.stored_output_bytes),
        blob_compression_ratio = round_2(compression.ratio()),
        blob_compression_compressed_count = compression.compressed_count,
        blob_compression_raw_count = compression.raw_count,
        blob_compression_encode_wall_total_ms =
            round_2(compression.encode_wall_us as f64 / 1_000.0),
        blob_compression_encode_work_total_ms =
            round_2(compression.encode_work_us as f64 / 1_000.0),
        retries = fetch_window.retry_attempts,
        "blob compression profile complete"
    );
    Ok(())
}

async fn seed_profile_publication(
    meta_store: &InMemoryMetaStore,
    prev: &FinalizedBlock,
) -> Result<()> {
    let empty = FamilyWindowRecord {
        first_primary_id: PrimaryId::ZERO,
        count: 0,
    };
    let record = BlockRecord {
        block_number: prev.block_number(),
        block_hash: prev.block_hash(),
        parent_hash: prev.parent_hash(),
        logs: empty,
        txs: empty,
        traces: empty,
    };
    meta_store
        .put(
            TableId::new("block_record"),
            &prev.block_number().to_be_bytes(),
            bytes::Bytes::from(record.encode()),
        )
        .await
        .context("seeding previous block record")?;
    meta_store
        .cas_put(
            TableId::new("publication_state"),
            b"state",
            None,
            bytes::Bytes::from(
                monad_chain_data::primitives::state::PublicationState {
                    indexed_finalized_head: prev.block_number(),
                }
                .encode(),
            ),
        )
        .await
        .context("seeding publication state")?;
    Ok(())
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

fn push_fetched_block(
    pending: &mut Vec<(u64, FinalizedBlock)>,
    item: (u64, Block, BlockReceipts, BlockTraces),
    fetch_consumed_total: &AtomicU64,
) -> Result<()> {
    let (n, block, receipts, traces) = item;
    fetch_consumed_total.fetch_add(1, Ordering::Relaxed);
    let finalized = into_finalized_block(block, receipts, traces)
        .with_context(|| format!("transforming block {n}"))?;
    pending.push((n, finalized));
    Ok(())
}

async fn wait_for_archive_block(
    reader: &BlockDataReaderErased,
    block_number: u64,
    poll_interval: Duration,
) -> Result<()> {
    loop {
        match reader.get_latest(LatestKind::Uploaded).await {
            Ok(Some(latest)) if latest >= block_number => return Ok(()),
            Ok(latest) => {
                debug!(
                    block_number,
                    latest = ?latest,
                    poll_ms = poll_interval.as_millis() as u64,
                    "waiting for archive source to publish next block"
                );
            }
            Err(error) => {
                warn!(
                    block_number,
                    poll_ms = poll_interval.as_millis() as u64,
                    error = %error,
                    "failed to read archive latest block; retrying"
                );
            }
        }
        tokio::time::sleep(poll_interval).await;
    }
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
    stage_a_log_plan_us: u64,
    stage_a_tx_plan_us: u64,
    stage_a_trace_plan_us: u64,
    stage_a_delta_us: u64,
    stage_a_session_stage_us: u64,
    stage_a_bitmap_fragment_count: u64,
    stage_a_hash_location_count: u64,
    stage_a_dir_fragments_total: u64,
    stage_a_dir_fragments_written: u64,
    stage_a_dir_fragments_skipped: u64,
    stage_a_bitmap_fragments_total: u64,
    stage_a_bitmap_fragments_written: u64,
    stage_a_bitmap_fragments_skipped: u64,
    phase_a_write_counts: monad_chain_data::WriteOpCounts,
    phase_b_write_counts: monad_chain_data::WriteOpCounts,
    commit_a_meta: Vec<u64>,
    commit_a_blob: Vec<u64>,
    reads: Vec<u64>,
    reads_dir_list: Vec<u64>,
    reads_dir_get: Vec<u64>,
    reads_bitmap_open_streams: Vec<u64>,
    reads_bitmap_list: Vec<u64>,
    reads_bitmap_get: Vec<u64>,
    reads_bitmap_shape: Vec<u64>,
    reads_bitmap_index: Vec<u64>,
    reads_bitmap_compact: Vec<u64>,
    reads_dir_index: Vec<u64>,
    reads_dir_decode: Vec<u64>,
    reads_unaccounted: Vec<u64>,
    stage_b: Vec<u64>,
    commit_b: Vec<u64>,
    cas: Vec<u64>,
    reads_dir_list_count: u64,
    reads_dir_get_count: u64,
    reads_bitmap_open_streams_count: u64,
    reads_bitmap_list_count: u64,
    reads_bitmap_get_count: u64,
    reads_bitmap_fragment_count: u64,
    reads_bitmap_fragment_bytes: u64,
    reads_bitmap_open_streams_us: u64,
    reads_bitmap_shape_us: u64,
    reads_bitmap_union_us: u64,
    reads_bitmap_frontier_us: u64,
    reads_bitmap_open_write_us: u64,
    reads_bitmap_index_us: u64,
    reads_bitmap_compact_us: u64,
    reads_bitmap_compact_wall_us: u64,
    reads_bitmap_compact_count: u64,
    reads_bitmap_frontier_stream_count: u64,
    reads_bitmap_union_page_count: u64,
    reads_bitmap_union_stream_count: u64,
    reads_bitmap_touched_page_count: u64,
    reads_bitmap_touched_stream_count: u64,
    reads_bitmap_final_open_stream_count: u64,
    reads_dir_fragment_count: u64,
    reads_dir_index_us: u64,
    reads_dir_decode_us: u64,
    reads_unaccounted_us: u64,
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
    stage_a_log_plan_ms_total: f64,
    stage_a_tx_plan_ms_total: f64,
    stage_a_trace_plan_ms_total: f64,
    stage_a_delta_ms_total: f64,
    stage_a_session_stage_ms_total: f64,
    stage_a_bitmap_fragment_count: u64,
    stage_a_hash_location_count: u64,
    stage_a_dir_fragments_total: u64,
    stage_a_dir_fragments_written: u64,
    stage_a_dir_fragments_skipped: u64,
    stage_a_bitmap_fragments_total: u64,
    stage_a_bitmap_fragments_written: u64,
    stage_a_bitmap_fragments_skipped: u64,
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
    reads_dir_list_p50: u64,
    reads_dir_list_p99: u64,
    reads_dir_get_p50: u64,
    reads_dir_get_p99: u64,
    reads_bitmap_open_streams_p50: u64,
    reads_bitmap_open_streams_p99: u64,
    reads_bitmap_list_p50: u64,
    reads_bitmap_list_p99: u64,
    reads_bitmap_get_p50: u64,
    reads_bitmap_get_p99: u64,
    reads_bitmap_shape_p50: u64,
    reads_bitmap_shape_p99: u64,
    reads_bitmap_index_p50: u64,
    reads_bitmap_index_p99: u64,
    reads_bitmap_compact_p50: u64,
    reads_bitmap_compact_p99: u64,
    reads_dir_index_p50: u64,
    reads_dir_index_p99: u64,
    reads_dir_decode_p50: u64,
    reads_dir_decode_p99: u64,
    reads_unaccounted_p50: u64,
    reads_unaccounted_p99: u64,
    reads_dir_list_count: u64,
    reads_dir_get_count: u64,
    reads_bitmap_open_streams_count: u64,
    reads_bitmap_list_count: u64,
    reads_bitmap_get_count: u64,
    reads_bitmap_fragment_count: u64,
    reads_bitmap_fragment_bytes: u64,
    reads_bitmap_open_streams_ms_total: f64,
    reads_bitmap_shape_ms_total: f64,
    reads_bitmap_union_ms_total: f64,
    reads_bitmap_frontier_ms_total: f64,
    reads_bitmap_open_write_ms_total: f64,
    reads_bitmap_index_ms_total: f64,
    reads_bitmap_compact_ms_total: f64,
    reads_bitmap_compact_wall_ms_total: f64,
    reads_bitmap_compact_count: u64,
    reads_bitmap_frontier_stream_count: u64,
    reads_bitmap_union_page_count: u64,
    reads_bitmap_union_stream_count: u64,
    reads_bitmap_touched_page_count: u64,
    reads_bitmap_touched_stream_count: u64,
    reads_bitmap_final_open_stream_count: u64,
    reads_dir_fragment_count: u64,
    reads_dir_index_ms_total: f64,
    reads_dir_decode_ms_total: f64,
    reads_unaccounted_ms_total: f64,
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
        self.stage_a_log_plan_us = self
            .stage_a_log_plan_us
            .saturating_add(t.stage_a_log_plan_us);
        self.stage_a_tx_plan_us = self.stage_a_tx_plan_us.saturating_add(t.stage_a_tx_plan_us);
        self.stage_a_trace_plan_us = self
            .stage_a_trace_plan_us
            .saturating_add(t.stage_a_trace_plan_us);
        self.stage_a_delta_us = self.stage_a_delta_us.saturating_add(t.stage_a_delta_us);
        self.stage_a_session_stage_us = self
            .stage_a_session_stage_us
            .saturating_add(t.stage_a_session_stage_us);
        self.stage_a_bitmap_fragment_count = self
            .stage_a_bitmap_fragment_count
            .saturating_add(t.stage_a_bitmap_fragment_count);
        self.stage_a_hash_location_count = self
            .stage_a_hash_location_count
            .saturating_add(t.stage_a_hash_location_count);
        self.stage_a_dir_fragments_total = self
            .stage_a_dir_fragments_total
            .saturating_add(t.stage_a_dir_fragments_total);
        self.stage_a_dir_fragments_written = self
            .stage_a_dir_fragments_written
            .saturating_add(t.stage_a_dir_fragments_written);
        self.stage_a_dir_fragments_skipped = self
            .stage_a_dir_fragments_skipped
            .saturating_add(t.stage_a_dir_fragments_skipped);
        self.stage_a_bitmap_fragments_total = self
            .stage_a_bitmap_fragments_total
            .saturating_add(t.stage_a_bitmap_fragments_total);
        self.stage_a_bitmap_fragments_written = self
            .stage_a_bitmap_fragments_written
            .saturating_add(t.stage_a_bitmap_fragments_written);
        self.stage_a_bitmap_fragments_skipped = self
            .stage_a_bitmap_fragments_skipped
            .saturating_add(t.stage_a_bitmap_fragments_skipped);
        self.phase_a_write_counts.merge(&t.phase_a_write_counts);
        self.phase_b_write_counts.merge(&t.phase_b_write_counts);
        self.commit_a_meta.push(t.commit_a_meta_ms);
        self.commit_a_blob.push(t.commit_a_blob_ms);
        self.reads.push(t.reads_ms);
        self.reads_dir_list.push(t.reads_dir_list_ms);
        self.reads_dir_get.push(t.reads_dir_get_ms);
        self.reads_bitmap_open_streams
            .push(t.reads_bitmap_open_streams_ms);
        self.reads_bitmap_list.push(t.reads_bitmap_list_ms);
        self.reads_bitmap_get.push(t.reads_bitmap_get_ms);
        self.reads_bitmap_shape.push(t.reads_bitmap_shape_ms);
        self.reads_bitmap_index.push(t.reads_bitmap_index_ms);
        self.reads_bitmap_compact.push(t.reads_bitmap_compact_ms);
        self.reads_dir_index.push(t.reads_dir_index_ms);
        self.reads_dir_decode.push(t.reads_dir_decode_ms);
        self.reads_unaccounted.push(t.reads_unaccounted_ms);
        self.stage_b.push(t.stage_b_ms);
        self.commit_b.push(t.commit_b_ms);
        self.cas.push(t.cas_ms);
        self.reads_dir_list_count = self
            .reads_dir_list_count
            .saturating_add(t.reads_dir_list_count);
        self.reads_dir_get_count = self
            .reads_dir_get_count
            .saturating_add(t.reads_dir_get_count);
        self.reads_bitmap_open_streams_count = self
            .reads_bitmap_open_streams_count
            .saturating_add(t.reads_bitmap_open_streams_count);
        self.reads_bitmap_list_count = self
            .reads_bitmap_list_count
            .saturating_add(t.reads_bitmap_list_count);
        self.reads_bitmap_get_count = self
            .reads_bitmap_get_count
            .saturating_add(t.reads_bitmap_get_count);
        self.reads_bitmap_fragment_count = self
            .reads_bitmap_fragment_count
            .saturating_add(t.reads_bitmap_fragment_count);
        self.reads_bitmap_fragment_bytes = self
            .reads_bitmap_fragment_bytes
            .saturating_add(t.reads_bitmap_fragment_bytes);
        self.reads_bitmap_open_streams_us = self
            .reads_bitmap_open_streams_us
            .saturating_add(t.reads_bitmap_open_streams_us);
        self.reads_bitmap_shape_us = self
            .reads_bitmap_shape_us
            .saturating_add(t.reads_bitmap_shape_us);
        self.reads_bitmap_union_us = self
            .reads_bitmap_union_us
            .saturating_add(t.reads_bitmap_union_us);
        self.reads_bitmap_frontier_us = self
            .reads_bitmap_frontier_us
            .saturating_add(t.reads_bitmap_frontier_us);
        self.reads_bitmap_open_write_us = self
            .reads_bitmap_open_write_us
            .saturating_add(t.reads_bitmap_open_write_us);
        self.reads_bitmap_index_us = self
            .reads_bitmap_index_us
            .saturating_add(t.reads_bitmap_index_us);
        self.reads_bitmap_compact_us = self
            .reads_bitmap_compact_us
            .saturating_add(t.reads_bitmap_compact_us);
        self.reads_bitmap_compact_wall_us = self
            .reads_bitmap_compact_wall_us
            .saturating_add(t.reads_bitmap_compact_wall_us);
        self.reads_bitmap_compact_count = self
            .reads_bitmap_compact_count
            .saturating_add(t.reads_bitmap_compact_count);
        self.reads_bitmap_frontier_stream_count = self
            .reads_bitmap_frontier_stream_count
            .saturating_add(t.reads_bitmap_frontier_stream_count);
        self.reads_bitmap_union_page_count = self
            .reads_bitmap_union_page_count
            .saturating_add(t.reads_bitmap_union_page_count);
        self.reads_bitmap_union_stream_count = self
            .reads_bitmap_union_stream_count
            .saturating_add(t.reads_bitmap_union_stream_count);
        self.reads_bitmap_touched_page_count = self
            .reads_bitmap_touched_page_count
            .saturating_add(t.reads_bitmap_touched_page_count);
        self.reads_bitmap_touched_stream_count = self
            .reads_bitmap_touched_stream_count
            .saturating_add(t.reads_bitmap_touched_stream_count);
        self.reads_bitmap_final_open_stream_count = self
            .reads_bitmap_final_open_stream_count
            .saturating_add(t.reads_bitmap_final_open_stream_count);
        self.reads_dir_fragment_count = self
            .reads_dir_fragment_count
            .saturating_add(t.reads_dir_fragment_count);
        self.reads_dir_index_us = self.reads_dir_index_us.saturating_add(t.reads_dir_index_us);
        self.reads_dir_decode_us = self
            .reads_dir_decode_us
            .saturating_add(t.reads_dir_decode_us);
        self.reads_unaccounted_us = self
            .reads_unaccounted_us
            .saturating_add(t.reads_unaccounted_us);
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
            stage_a_log_plan_ms_total: round_2(self.stage_a_log_plan_us as f64 / 1_000.0),
            stage_a_tx_plan_ms_total: round_2(self.stage_a_tx_plan_us as f64 / 1_000.0),
            stage_a_trace_plan_ms_total: round_2(self.stage_a_trace_plan_us as f64 / 1_000.0),
            stage_a_delta_ms_total: round_2(self.stage_a_delta_us as f64 / 1_000.0),
            stage_a_session_stage_ms_total: round_2(self.stage_a_session_stage_us as f64 / 1_000.0),
            stage_a_bitmap_fragment_count: self.stage_a_bitmap_fragment_count,
            stage_a_hash_location_count: self.stage_a_hash_location_count,
            stage_a_dir_fragments_total: self.stage_a_dir_fragments_total,
            stage_a_dir_fragments_written: self.stage_a_dir_fragments_written,
            stage_a_dir_fragments_skipped: self.stage_a_dir_fragments_skipped,
            stage_a_bitmap_fragments_total: self.stage_a_bitmap_fragments_total,
            stage_a_bitmap_fragments_written: self.stage_a_bitmap_fragments_written,
            stage_a_bitmap_fragments_skipped: self.stage_a_bitmap_fragments_skipped,
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
            reads_dir_list_p50: percentile(&self.reads_dir_list, 0.50),
            reads_dir_list_p99: percentile(&self.reads_dir_list, 0.99),
            reads_dir_get_p50: percentile(&self.reads_dir_get, 0.50),
            reads_dir_get_p99: percentile(&self.reads_dir_get, 0.99),
            reads_bitmap_open_streams_p50: percentile(&self.reads_bitmap_open_streams, 0.50),
            reads_bitmap_open_streams_p99: percentile(&self.reads_bitmap_open_streams, 0.99),
            reads_bitmap_list_p50: percentile(&self.reads_bitmap_list, 0.50),
            reads_bitmap_list_p99: percentile(&self.reads_bitmap_list, 0.99),
            reads_bitmap_get_p50: percentile(&self.reads_bitmap_get, 0.50),
            reads_bitmap_get_p99: percentile(&self.reads_bitmap_get, 0.99),
            reads_bitmap_shape_p50: percentile(&self.reads_bitmap_shape, 0.50),
            reads_bitmap_shape_p99: percentile(&self.reads_bitmap_shape, 0.99),
            reads_bitmap_index_p50: percentile(&self.reads_bitmap_index, 0.50),
            reads_bitmap_index_p99: percentile(&self.reads_bitmap_index, 0.99),
            reads_bitmap_compact_p50: percentile(&self.reads_bitmap_compact, 0.50),
            reads_bitmap_compact_p99: percentile(&self.reads_bitmap_compact, 0.99),
            reads_dir_index_p50: percentile(&self.reads_dir_index, 0.50),
            reads_dir_index_p99: percentile(&self.reads_dir_index, 0.99),
            reads_dir_decode_p50: percentile(&self.reads_dir_decode, 0.50),
            reads_dir_decode_p99: percentile(&self.reads_dir_decode, 0.99),
            reads_unaccounted_p50: percentile(&self.reads_unaccounted, 0.50),
            reads_unaccounted_p99: percentile(&self.reads_unaccounted, 0.99),
            reads_dir_list_count: self.reads_dir_list_count,
            reads_dir_get_count: self.reads_dir_get_count,
            reads_bitmap_open_streams_count: self.reads_bitmap_open_streams_count,
            reads_bitmap_list_count: self.reads_bitmap_list_count,
            reads_bitmap_get_count: self.reads_bitmap_get_count,
            reads_bitmap_fragment_count: self.reads_bitmap_fragment_count,
            reads_bitmap_fragment_bytes: self.reads_bitmap_fragment_bytes,
            reads_bitmap_open_streams_ms_total: round_2(
                self.reads_bitmap_open_streams_us as f64 / 1_000.0,
            ),
            reads_bitmap_shape_ms_total: round_2(self.reads_bitmap_shape_us as f64 / 1_000.0),
            reads_bitmap_union_ms_total: round_2(self.reads_bitmap_union_us as f64 / 1_000.0),
            reads_bitmap_frontier_ms_total: round_2(self.reads_bitmap_frontier_us as f64 / 1_000.0),
            reads_bitmap_open_write_ms_total: round_2(
                self.reads_bitmap_open_write_us as f64 / 1_000.0,
            ),
            reads_bitmap_index_ms_total: round_2(self.reads_bitmap_index_us as f64 / 1_000.0),
            reads_bitmap_compact_ms_total: round_2(self.reads_bitmap_compact_us as f64 / 1_000.0),
            reads_bitmap_compact_wall_ms_total: round_2(
                self.reads_bitmap_compact_wall_us as f64 / 1_000.0,
            ),
            reads_bitmap_compact_count: self.reads_bitmap_compact_count,
            reads_bitmap_frontier_stream_count: self.reads_bitmap_frontier_stream_count,
            reads_bitmap_union_page_count: self.reads_bitmap_union_page_count,
            reads_bitmap_union_stream_count: self.reads_bitmap_union_stream_count,
            reads_bitmap_touched_page_count: self.reads_bitmap_touched_page_count,
            reads_bitmap_touched_stream_count: self.reads_bitmap_touched_stream_count,
            reads_bitmap_final_open_stream_count: self.reads_bitmap_final_open_stream_count,
            reads_dir_fragment_count: self.reads_dir_fragment_count,
            reads_dir_index_ms_total: round_2(self.reads_dir_index_us as f64 / 1_000.0),
            reads_dir_decode_ms_total: round_2(self.reads_dir_decode_us as f64 / 1_000.0),
            reads_unaccounted_ms_total: round_2(self.reads_unaccounted_us as f64 / 1_000.0),
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
    meta_store: &FjallStore,
    blob_store: &FjallStore,
    same_dir: bool,
) -> Result<Vec<monad_chain_data::store::FjallKeyspaceStats>> {
    let mut stats = meta_store.keyspace_stats()?;
    if !same_dir {
        stats.extend(blob_store.keyspace_stats()?);
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
