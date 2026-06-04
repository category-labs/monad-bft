// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

#![allow(async_fn_in_trait)]

use std::{
    collections::BTreeMap,
    future::Future,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use eyre::{bail, eyre, Context, ContextCompat, Result};
use futures::{stream, StreamExt};
use tokio::sync::{mpsc, Mutex as TokioMutex, Semaphore};
use tracing::{info, warn};

use crate::{
    store::{BlobStore, MetaStoreCas},
    FinalizedBlock, IngestBatchTimings, IngestOutcome, IngestPlan, IoRetryPolicy,
    MonadChainDataService, PublicationAdvance, QueryLimits, WriteOpCounts,
};

const FOLLOW_POLL_INTERVAL: Duration = Duration::from_millis(50);
const LEASE_CLOCK_REFRESH: Duration = Duration::from_secs(30);
const FETCH_CONTROL_TICK: Duration = Duration::from_secs(2);
const WINDOW_MAX_WALL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChainDataFetchStrategy {
    SemaphoreBuffer,
    BoundedChannel,
    SpawnedTasks,
    SpawnedSemaphoreBuffer,
}

impl Default for ChainDataFetchStrategy {
    fn default() -> Self {
        Self::SpawnedSemaphoreBuffer
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
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
    pub fetch_strategy: ChainDataFetchStrategy,
    pub open_index_snapshot: Option<PathBuf>,
    pub max_batch_size: usize,
    pub min_batch_size: usize,
    pub plan_buffer: usize,
    pub write_buffer: usize,
    pub write_workers: usize,
    pub progress_buffer: usize,
    pub io_max_retries: usize,
    pub io_retry_forever: bool,
    pub io_retry_backoff_ms: u64,
    pub io_retry_max_backoff_ms: u64,
    pub log_every: u64,
}

impl Default for ChainDataIngestConfig {
    fn default() -> Self {
        Self {
            start: None,
            end: None,
            count: None,
            benchmark_synthetic_start: false,
            concurrency: 5000,
            max_retries: 5,
            retry_backoff_ms: 200,
            autotune: true,
            min_concurrency: 250,
            max_concurrency: 15000,
            fetch_buffer: 10000,
            fetch_strategy: ChainDataFetchStrategy::default(),
            open_index_snapshot: None,
            max_batch_size: 1000,
            min_batch_size: 1,
            plan_buffer: 8,
            write_buffer: 8,
            write_workers: 1,
            progress_buffer: 16,
            io_max_retries: 5,
            io_retry_forever: false,
            io_retry_backoff_ms: 200,
            io_retry_max_backoff_ms: 10_000,
            log_every: 1000,
        }
    }
}

impl ChainDataIngestConfig {
    pub fn validate(&self) -> Result<()> {
        if self.concurrency == 0 {
            bail!("chain-data concurrency must be >= 1");
        }
        if matches!(self.count, Some(0)) {
            bail!("chain-data count must be >= 1");
        }
        if self.end.is_some() && self.count.is_some() {
            bail!("chain-data end and count are mutually exclusive");
        }
        if self.max_batch_size == 0 {
            bail!("chain-data max_batch_size must be >= 1");
        }
        if self.min_batch_size == 0 {
            bail!("chain-data min_batch_size must be >= 1");
        }
        if self.min_batch_size > self.max_batch_size {
            bail!("chain-data min_batch_size must be <= max_batch_size");
        }
        if self.fetch_buffer == 0 {
            bail!("chain-data fetch_buffer must be >= 1");
        }
        if self.plan_buffer == 0
            || self.write_buffer == 0
            || self.write_workers == 0
            || self.progress_buffer == 0
        {
            bail!("chain-data pipeline buffers and write_workers must be >= 1");
        }
        if self.autotune {
            if self.min_concurrency == 0 {
                bail!("chain-data min_concurrency must be >= 1");
            }
            if self.max_concurrency < self.min_concurrency {
                bail!("chain-data max_concurrency must be >= min_concurrency");
            }
            if self.concurrency < self.min_concurrency || self.concurrency > self.max_concurrency {
                bail!("chain-data concurrency must be within min/max concurrency");
            }
        }
        if self.io_retry_max_backoff_ms < self.io_retry_backoff_ms {
            bail!("chain-data io_retry_max_backoff_ms must be >= io_retry_backoff_ms");
        }
        Ok(())
    }

    pub fn io_retry_policy(&self) -> IoRetryPolicy {
        if self.io_retry_forever {
            IoRetryPolicy::infinite(
                Duration::from_millis(self.io_retry_backoff_ms),
                Duration::from_millis(self.io_retry_max_backoff_ms),
            )
        } else {
            IoRetryPolicy::bounded(
                self.io_max_retries,
                Duration::from_millis(self.io_retry_backoff_ms),
                Duration::from_millis(self.io_retry_max_backoff_ms),
            )
        }
    }
}

pub trait ChainDataIngestSource: Clone + Send + Sync + 'static {
    fn get_latest_uploaded(&self) -> impl Future<Output = Result<Option<u64>>> + Send;

    fn fetch_finalized_block(
        &self,
        block_number: u64,
    ) -> impl Future<Output = Result<FinalizedBlock>> + Send;
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

struct WrittenBatch {
    seq: u64,
    last_block: u64,
    outcomes: Vec<IngestOutcome>,
    timings: IngestBatchTimings,
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

struct AppliedBatch {
    last_block: u64,
    outcomes: Vec<IngestOutcome>,
    timings: IngestBatchTimings,
    first_wait_ms: u64,
    fetch_batch_collect_ms: u64,
    plan_queue_wait_ms: u64,
    plan_ms: u64,
    io_queue_wait_ms: u64,
    io_apply_ms: u64,
    publish_queue_wait_ms: u64,
    publish_group_batches: u64,
    total_ingest_ms: u64,
}

type TimedFetchResult = Result<(u64, FinalizedBlock, FetchTimings)>;

#[derive(Clone, Copy, Default)]
struct FetchTimings {
    total_ms: u64,
}

#[derive(Default)]
struct FetchStats {
    durations_ms: Vec<u64>,
    retry_attempts: u64,
    completed: u64,
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

#[derive(Default)]
struct PhaseStats {
    stage_a: Vec<u64>,
    phase_a_write_counts: WriteOpCounts,
    phase_b_write_counts: WriteOpCounts,
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
    phase_b_write_ops_total: u64,
    phase_b_write_bytes_total: u64,
    phase_b_skipped_ratio: f64,
}

impl PhaseStats {
    fn record(&mut self, t: &IngestBatchTimings) {
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
        let phase_b_skipped_ratio = if self.phase_b_total == 0 {
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
            phase_b_write_ops_total: self.phase_b_write_counts.total_ops(),
            phase_b_write_bytes_total: self.phase_b_write_counts.total_bytes(),
            phase_b_skipped_ratio,
        }
    }
}

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

    fn adjust_from_fetch_window(
        &self,
        retry_rate: f64,
        completed_delta: u64,
        started_delta: u64,
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

pub async fn run_chain_data_ingest<M, B, S>(
    service: Arc<MonadChainDataService<M, B>>,
    source: S,
    config: ChainDataIngestConfig,
    observed_upstream: Option<Arc<AtomicU64>>,
) -> Result<()>
where
    M: MetaStoreCas,
    B: BlobStore,
    S: ChainDataIngestSource,
{
    config.validate()?;

    let head = service
        .publication()
        .load_published_head()
        .await
        .context("loading current chain-data publication head")?;
    let expected_start = head.map_or(1, |head| head + 1);
    let start = config.start.unwrap_or(expected_start);
    if config.benchmark_synthetic_start {
        if head.is_some() {
            bail!(
                "benchmark_synthetic_start requires an empty publication head; current head: {:?}",
                head
            );
        }
        if config.start.is_none() || start <= 1 {
            bail!("benchmark_synthetic_start requires explicit start > 1");
        }
        warn!(
            start,
            "chain-data benchmark synthetic start enabled; output is a suffix-only fixture"
        );
    } else if start != expected_start {
        bail!(
            "chain-data start={} does not match expected next block {} (current head: {:?})",
            start,
            expected_start,
            head
        );
    }
    let end = match (config.end, config.count) {
        (Some(end), None) => Some(end),
        (None, Some(count)) => Some(
            start
                .checked_add(count - 1)
                .context("chain-data start + count - 1 overflows u64")?,
        ),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("validated above"),
    };
    if let Some(end) = end {
        if start > end {
            bail!("chain-data start ({start}) must be <= end ({end})");
        }
    }

    if let Some(observed) = &observed_upstream {
        seed_observed_upstream(&source, observed).await?;
    }
    let refresh_handle = observed_upstream.as_ref().map(|observed| {
        let source = source.clone();
        let observed = observed.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(LEASE_CLOCK_REFRESH);
            tick.tick().await;
            loop {
                tick.tick().await;
                match source.get_latest_uploaded().await {
                    Ok(Some(latest)) => {
                        observed.fetch_max(latest, Ordering::SeqCst);
                    }
                    Ok(None) => {}
                    Err(error) => warn!(
                        %error,
                        "chain-data lease-clock refresh failed; keeping last observation"
                    ),
                }
            }
        })
    });

    if let Some(snapshot_path) = &config.open_index_snapshot {
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
                    approx_bytes = status.stats.approx_bytes,
                    "chain-data loaded open-index snapshot"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                info!(
                    path = %snapshot_path.display(),
                    "chain-data open-index snapshot not found"
                );
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("reading open-index snapshot {}", snapshot_path.display())
                });
            }
        }
    }

    if config.benchmark_synthetic_start {
        warn!("chain-data deferring planning-state preparation until synthetic predecessor seed");
    } else {
        service
            .prepare_ingest_planning_state()
            .await
            .context("preparing chain-data ingest planning state")?;
    }

    info!(
        start,
        end = ?end,
        follow = end.is_none(),
        concurrency = config.concurrency,
        fetch_buffer = config.fetch_buffer,
        max_batch_size = config.max_batch_size,
        min_batch_size = config.min_batch_size,
        plan_buffer = config.plan_buffer,
        write_buffer = config.write_buffer,
        write_workers = config.write_workers,
        progress_buffer = config.progress_buffer,
        fetch_strategy = ?config.fetch_strategy,
        "chain-data ingest worker starting"
    );

    let initial_concurrency = config.concurrency;
    let (min_concurrency, max_concurrency) = if config.autotune {
        (config.min_concurrency, config.max_concurrency)
    } else {
        (initial_concurrency, initial_concurrency)
    };
    let control = Arc::new(ConcurrencyControl::new(
        initial_concurrency,
        min_concurrency,
        max_concurrency,
    ));
    let io_retry = config.io_retry_policy();
    let log_every_batches = config.log_every.max(1);
    let fetch_stats = Arc::new(std::sync::Mutex::new(FetchStats::default()));
    let fetch_progress = Arc::new(FetchProgress::default());
    let pipeline_progress = Arc::new(PipelineProgress::default());
    let fetch_consumed_total = Arc::new(AtomicU64::new(0));

    let (fetched_tx, mut fetched_rx) = mpsc::channel::<FetchedBatch>(config.plan_buffer);
    let (planned_tx, planned_rx) = mpsc::channel::<PlannedBatch>(config.plan_buffer);
    let (written_tx, mut written_rx) = mpsc::channel::<WrittenBatch>(config.write_buffer);
    let (applied_tx, mut applied_rx) = mpsc::channel::<AppliedBatch>(config.progress_buffer);

    let heartbeat_handle = {
        let fetch_progress = fetch_progress.clone();
        let pipeline_progress = pipeline_progress.clone();
        let fetch_consumed_total = fetch_consumed_total.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WINDOW_MAX_WALL);
            tick.tick().await;
            loop {
                tick.tick().await;
                let fetch = fetch_progress.snapshot();
                let pipeline = pipeline_progress.snapshot();
                let consumed = fetch_consumed_total.load(Ordering::Relaxed);
                info!(
                    fetch_started_total = fetch.started,
                    fetch_completed_total = fetch.completed,
                    fetch_consumed_total = consumed,
                    fetch_completed_backlog = fetch.completed.saturating_sub(consumed),
                    fetch_in_flight = fetch.started.saturating_sub(fetch.completed),
                    planned_batches_total = pipeline.planned,
                    write_started_batches_total = pipeline.write_started,
                    write_apply_completed_batches_total = pipeline.write_apply_completed,
                    write_sent_to_publisher_batches_total = pipeline.write_sent_to_publisher,
                    publish_started_groups_total = pipeline.publish_started,
                    publish_completed_groups_total = pipeline.publish_completed,
                    published_batches_total = pipeline.published_batches,
                    applied_sent_batches_total = pipeline.applied_sent,
                    "chain-data ingest heartbeat"
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
                let denominator = completed_delta.max(started_delta).max(1);
                let retry_rate = retry_delta as f64 / denominator as f64;
                let before = control.current();
                let after =
                    control.adjust_from_fetch_window(retry_rate, completed_delta, started_delta);
                if after != before {
                    info!(
                        from = before,
                        to = after,
                        retry_rate = round_2(retry_rate),
                        retry_attempts = retry_delta,
                        fetch_started = started_delta,
                        fetch_completed = completed_delta,
                        "chain-data fetch autotune adjusted concurrency"
                    );
                }
                previous = current;
            }
        })
    };

    let fetch_handle = {
        let source = source.clone();
        let fetch_stats = fetch_stats.clone();
        let fetch_progress = fetch_progress.clone();
        let fetch_consumed_total = fetch_consumed_total.clone();
        let permits = control.permits();
        let fetched_tx = fetched_tx.clone();
        let max_retries = config.max_retries;
        let initial_backoff = Duration::from_millis(config.retry_backoff_ms);
        let fetch_buffer = config.fetch_buffer;
        let max_batch_size = config.max_batch_size;
        let min_batch_size = config.min_batch_size;
        let fetch_strategy = config.fetch_strategy;
        tokio::spawn(async move {
            let (ordered_tx, mut ordered_rx) = mpsc::channel::<TimedFetchResult>(fetch_buffer);
            let produce_handle = {
                let source = source.clone();
                let fetch_stats = fetch_stats.clone();
                let fetch_progress = fetch_progress.clone();
                let permits = permits.clone();
                let ordered_tx = ordered_tx.clone();
                tokio::spawn(async move {
                    let fetch_stream = block_number_stream(source.clone(), start, end)
                        .map(move |n| {
                            let source = source.clone();
                            let stats = fetch_stats.clone();
                            let progress = fetch_progress.clone();
                            let permits = permits.clone();
                            async move {
                                match fetch_strategy {
                                    ChainDataFetchStrategy::SpawnedTasks
                                    | ChainDataFetchStrategy::SpawnedSemaphoreBuffer => {
                                        let permit = permits.acquire_owned().await.expect(
                                            "chain-data concurrency semaphore should never close",
                                        );
                                        tokio::spawn(async move {
                                            let _permit = permit;
                                            fetch_block_with_retry(
                                                &source,
                                                n,
                                                max_retries,
                                                initial_backoff,
                                                &stats,
                                                &progress,
                                            )
                                            .await
                                        })
                                        .await
                                        .map_err(|e| eyre!("spawned fetch task panicked: {e}"))?
                                    }
                                    ChainDataFetchStrategy::SemaphoreBuffer
                                    | ChainDataFetchStrategy::BoundedChannel => {
                                        let _permit = permits.acquire_owned().await.expect(
                                            "chain-data concurrency semaphore should never close",
                                        );
                                        fetch_block_with_retry(
                                            &source,
                                            n,
                                            max_retries,
                                            initial_backoff,
                                            &stats,
                                            &progress,
                                        )
                                        .await
                                    }
                                }
                            }
                        })
                        .buffered(fetch_buffer);
                    futures::pin_mut!(fetch_stream);
                    while let Some(item) = fetch_stream.next().await {
                        if ordered_tx.send(item).await.is_err() {
                            break;
                        }
                    }
                    Ok::<_, eyre::Report>(())
                })
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
                push_fetched_item(
                    first_item,
                    &fetch_stats,
                    &fetch_consumed_total,
                    &mut pending,
                )?;
                while pending.len() < min_batch_size {
                    let Some(item) = ordered_rx.recv().await else {
                        break;
                    };
                    push_fetched_item(item, &fetch_stats, &fetch_consumed_total, &mut pending)?;
                }
                while pending.len() < max_batch_size {
                    match ordered_rx.try_recv() {
                        Ok(item) => {
                            push_fetched_item(
                                item,
                                &fetch_stats,
                                &fetch_consumed_total,
                                &mut pending,
                            )?;
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                let last_block = pending.last().expect("pending non-empty").0;
                let blocks = pending.iter().map(|(_, block)| block.clone()).collect();
                let ready_at = Instant::now();
                fetched_tx
                    .send(FetchedBatch {
                        last_block,
                        blocks,
                        first_wait_ms,
                        fetch_batch_collect_ms: wait_started.elapsed().as_millis() as u64,
                        ingest_started: ready_at,
                        fetched_ready_at: ready_at,
                    })
                    .await
                    .map_err(|_| eyre!("planning worker stopped before fetch completed"))?;
            }
            produce_handle
                .await
                .map_err(|e| eyre!("fetch producer panicked: {e}"))??;
            Ok::<_, eyre::Report>(())
        })
    };
    drop(fetched_tx);

    let plan_handle = {
        let service = service.clone();
        let pipeline_progress = pipeline_progress.clone();
        let benchmark_synthetic_start = config.benchmark_synthetic_start;
        tokio::spawn(async move {
            let mut next_plan_seq = 0u64;
            while let Some(batch) = fetched_rx.recv().await {
                let plan_queue_wait_ms = batch.fetched_ready_at.elapsed().as_millis() as u64;
                if benchmark_synthetic_start && next_plan_seq == 0 {
                    let first = batch
                        .blocks
                        .first()
                        .expect("fetch worker sends non-empty batches");
                    service
                        .seed_synthetic_ingest_start(first.block_number(), first.parent_hash())
                        .await
                        .with_context(|| {
                            format!(
                                "seeding chain-data synthetic predecessor for block {}",
                                first.block_number()
                            )
                        })?;
                }
                let plan_started = Instant::now();
                info!(
                    seq = next_plan_seq,
                    last_block = batch.last_block,
                    blocks = batch.blocks.len(),
                    plan_queue_wait_ms,
                    first_wait_ms = batch.first_wait_ms,
                    fetch_batch_collect_ms = batch.fetch_batch_collect_ms,
                    "chain-data planning worker starting batch"
                );
                let plan = service
                    .plan_ingest_blocks(batch.blocks)
                    .await
                    .with_context(|| {
                        format!(
                            "planning chain-data batch ending at block {}",
                            batch.last_block
                        )
                    })?
                    .expect("fetch worker sends non-empty batches");
                let planned_ready_at = Instant::now();
                let plan_ms = plan_started.elapsed().as_millis() as u64;
                info!(
                    seq = next_plan_seq,
                    last_block = batch.last_block,
                    plan_ms,
                    stage_a_ms = plan.timings.stage_a_ms,
                    reads_ms = plan.timings.reads_ms,
                    stage_b_ms = plan.timings.stage_b_ms,
                    phase_b_skipped = plan.timings.phase_b_skipped,
                    phase_a_write_ops = plan.timings.phase_a_write_counts.total_ops(),
                    phase_b_write_ops = plan.timings.phase_b_write_counts.total_ops(),
                    "chain-data planning worker sending planned batch"
                );
                planned_tx
                    .send(PlannedBatch {
                        seq: next_plan_seq,
                        last_block: batch.last_block,
                        plan,
                        first_wait_ms: batch.first_wait_ms,
                        fetch_batch_collect_ms: batch.fetch_batch_collect_ms,
                        plan_queue_wait_ms,
                        plan_ms,
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

    let planned_rx = Arc::new(TokioMutex::new(planned_rx));
    let mut io_handles = Vec::with_capacity(config.write_workers);
    for worker_idx in 0..config.write_workers {
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
                pipeline_progress.write_started.fetch_add(1, Ordering::Relaxed);
                let io_queue_wait_ms = batch.planned_ready_at.elapsed().as_millis() as u64;
                info!(
                    worker_idx,
                    seq = batch.seq,
                    last_block = batch.last_block,
                    io_queue_wait_ms,
                    phase_a_write_ops = batch.plan.timings.phase_a_write_counts.total_ops(),
                    phase_a_write_bytes = batch.plan.timings.phase_a_write_counts.total_bytes(),
                    phase_b_write_ops = batch.plan.timings.phase_b_write_counts.total_ops(),
                    phase_b_write_bytes = batch.plan.timings.phase_b_write_counts.total_bytes(),
                    "chain-data write worker starting staged write apply"
                );
                let io_apply_started = Instant::now();
                let (outcomes, timings, publication) = service
                    .apply_ingest_writes_with_retry(batch.plan, io_retry)
                    .await
                    .with_context(|| {
                        format!(
                            "write worker {worker_idx} applying chain-data batch {} ending at block {}",
                            batch.seq, batch.last_block
                        )
                    })?;
                let io_apply_ms = io_apply_started.elapsed().as_millis() as u64;
                info!(
                    worker_idx,
                    seq = batch.seq,
                    last_block = batch.last_block,
                    io_apply_ms,
                    "chain-data write worker completed staged write apply"
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
                let mut pending_publish = Vec::new();
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
                let publish_last_seq = pending_publish
                    .last()
                    .expect("pending publish non-empty")
                    .seq;
                let publish_last_block = pending_publish
                    .last()
                    .expect("pending publish non-empty")
                    .last_block;
                info!(
                    publish_group_batches = pending_publish.len(),
                    publish_last_seq,
                    publish_last_block,
                    "chain-data publisher starting publication advance"
                );
                pipeline_progress
                    .publish_started
                    .fetch_add(1, Ordering::Relaxed);
                let cas_ms = service
                    .publish_ingest_advance(publication)
                    .await
                    .with_context(|| {
                        format!(
                            "publishing chain-data batch group ending at seq {publish_last_seq} block {publish_last_block}"
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

    let mut total_blocks = 0u64;
    let mut total_txs = 0u64;
    let mut total_logs = 0u64;
    let mut total_traces = 0u64;
    let mut applied_batches = 0u64;
    let mut window_start = Instant::now();
    let mut window_batches = 0u64;
    let mut window_blocks = 0u64;
    let mut window_txs = 0u64;
    let mut window_logs = 0u64;
    let mut window_traces = 0u64;
    let mut ingest_ms = Vec::with_capacity(log_every_batches as usize);
    let mut wait_ms = Vec::with_capacity(log_every_batches as usize);
    let mut fetch_batch_collect_ms = Vec::with_capacity(log_every_batches as usize);
    let mut plan_queue_wait_ms = Vec::with_capacity(log_every_batches as usize);
    let mut plan_ms = Vec::with_capacity(log_every_batches as usize);
    let mut io_queue_wait_ms = Vec::with_capacity(log_every_batches as usize);
    let mut io_apply_ms = Vec::with_capacity(log_every_batches as usize);
    let mut publish_queue_wait_ms = Vec::with_capacity(log_every_batches as usize);
    let mut publish_group_batches = Vec::with_capacity(log_every_batches as usize);
    let mut phase = PhaseStats::default();

    while let Some(applied) = applied_rx.recv().await {
        ingest_ms.push(applied.total_ingest_ms);
        wait_ms.push(applied.first_wait_ms);
        fetch_batch_collect_ms.push(applied.fetch_batch_collect_ms);
        plan_queue_wait_ms.push(applied.plan_queue_wait_ms);
        plan_ms.push(applied.plan_ms);
        io_queue_wait_ms.push(applied.io_queue_wait_ms);
        io_apply_ms.push(applied.io_apply_ms);
        publish_queue_wait_ms.push(applied.publish_queue_wait_ms);
        if applied.publish_group_batches > 0 {
            publish_group_batches.push(applied.publish_group_batches);
        }
        phase.record(&applied.timings);
        applied_batches += 1;
        window_batches += 1;
        for outcome in &applied.outcomes {
            total_txs += outcome.written_txs as u64;
            total_logs += outcome.written_logs as u64;
            total_traces += outcome.written_traces as u64;
            window_blocks += 1;
            window_txs += outcome.written_txs as u64;
            window_logs += outcome.written_logs as u64;
            window_traces += outcome.written_traces as u64;
        }
        total_blocks += applied.outcomes.len() as u64;

        let flush = end.is_some_and(|end| applied.last_block == end)
            || (window_blocks > 0
                && (window_batches >= log_every_batches
                    || window_start.elapsed() >= WINDOW_MAX_WALL));
        if flush {
            let elapsed = window_start.elapsed().as_secs_f64().max(1e-9);
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
            let fetch_completed_per_sec = round_2(fetch_window.completed as f64 / elapsed);
            let phase_summary = std::mem::take(&mut phase).summary();
            info!(
                block = applied.last_block,
                total_blocks,
                total_txs,
                total_logs,
                total_traces,
                applied_batches_total = applied_batches,
                window_batches,
                concurrency = control.current(),
                blocks_per_sec = round_2(window_blocks as f64 / elapsed),
                fetch_completed_per_sec,
                fetch_started_total = fetch_progress_snapshot.started,
                fetch_completed_total = fetch_progress_snapshot.completed,
                fetch_consumed_total = fetch_consumed_snapshot,
                fetch_completed_backlog,
                fetch_in_flight,
                txs_per_sec = round_2(window_txs as f64 / elapsed),
                logs_per_sec = round_2(window_logs as f64 / elapsed),
                traces_per_sec = round_2(window_traces as f64 / elapsed),
                fetch_p50_ms = percentile(&fetch_window.durations_ms, 0.50),
                fetch_p99_ms = percentile(&fetch_window.durations_ms, 0.99),
                ingest_p50_ms = percentile(&ingest_ms, 0.50),
                ingest_p99_ms = percentile(&ingest_ms, 0.99),
                fetch_batch_collect_p50_ms = percentile(&fetch_batch_collect_ms, 0.50),
                plan_queue_wait_p50_ms = percentile(&plan_queue_wait_ms, 0.50),
                plan_p50_ms = percentile(&plan_ms, 0.50),
                io_queue_wait_p50_ms = percentile(&io_queue_wait_ms, 0.50),
                io_apply_p50_ms = percentile(&io_apply_ms, 0.50),
                publish_queue_wait_p50_ms = percentile(&publish_queue_wait_ms, 0.50),
                publish_group_max = publish_group_batches.iter().copied().max().unwrap_or(0),
                retries = fetch_window.retry_attempts,
                stage_a_wall_total_ms = phase_summary.stage_a_wall_ms_total,
                commit_a_meta_wall_total_ms = phase_summary.commit_a_meta_wall_ms_total,
                commit_a_blob_wall_total_ms = phase_summary.commit_a_blob_wall_ms_total,
                reads_wall_total_ms = phase_summary.reads_wall_ms_total,
                stage_b_wall_total_ms = phase_summary.stage_b_wall_ms_total,
                commit_b_wall_total_ms = phase_summary.commit_b_wall_ms_total,
                cas_wall_total_ms = phase_summary.cas_wall_ms_total,
                phase_a_write_ops_total = phase_summary.phase_a_write_ops_total,
                phase_a_write_bytes_total = phase_summary.phase_a_write_bytes_total,
                phase_b_write_ops_total = phase_summary.phase_b_write_ops_total,
                phase_b_write_bytes_total = phase_summary.phase_b_write_bytes_total,
                phase_b_skipped_ratio = round_2(phase_summary.phase_b_skipped_ratio),
                "chain-data ingest progress"
            );
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
        }
    }

    let (fetch_res, plan_res, io_res, publisher_res) = tokio::join!(
        fetch_handle,
        plan_handle,
        futures::future::join_all(io_handles),
        publisher_handle
    );
    let publisher_res = publisher_res.context("chain-data publish worker panicked")?;
    let plan_res = plan_res.context("chain-data planning worker panicked")?;
    let fetch_res = fetch_res.context("chain-data fetch worker panicked")?;
    publisher_res.context("chain-data publish worker failed")?;
    for (idx, io_res) in io_res.into_iter().enumerate() {
        io_res
            .with_context(|| format!("chain-data IO worker {idx} panicked"))?
            .with_context(|| format!("chain-data IO worker {idx} failed"))?;
    }
    plan_res.context("chain-data planning worker failed")?;
    fetch_res.context("chain-data fetch worker failed")?;

    if let Some(handle) = refresh_handle {
        handle.abort();
    }
    heartbeat_handle.abort();
    fetch_control_handle.abort();

    if let Some(snapshot_path) = &config.open_index_snapshot {
        let bytes = service
            .open_index_snapshot_bytes()
            .context("encoding chain-data open-index snapshot")?;
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
    }

    info!(end = ?end, total_blocks, total_txs, total_logs, total_traces, "chain-data ingest complete");
    Ok(())
}

async fn seed_observed_upstream<S: ChainDataIngestSource>(
    source: &S,
    observed: &AtomicU64,
) -> Result<()> {
    match source
        .get_latest_uploaded()
        .await
        .context("reading source latest for chain-data lease clock")?
    {
        Some(latest) => {
            observed.store(latest, Ordering::SeqCst);
            info!(latest, "seeded chain-data lease clock from source latest");
        }
        None => warn!("source reports no latest block; chain-data lease will fail closed"),
    }
    Ok(())
}

fn block_number_stream<S: ChainDataIngestSource>(
    source: S,
    start: u64,
    end: Option<u64>,
) -> impl futures::Stream<Item = u64> {
    stream::unfold((source, start, end), |(source, next, end)| async move {
        if end.is_some_and(|end| next > end) {
            return None;
        }
        loop {
            if end.is_some() {
                return Some((next, (source, next.saturating_add(1), end)));
            }
            match source.get_latest_uploaded().await {
                Ok(Some(latest)) if latest >= next => {
                    return Some((next, (source, next.saturating_add(1), end)));
                }
                Ok(Some(_)) | Ok(None) => {
                    tokio::time::sleep(FOLLOW_POLL_INTERVAL).await;
                }
                Err(error) => {
                    warn!(%error, next, "chain-data failed to poll source latest; retrying");
                    tokio::time::sleep(FOLLOW_POLL_INTERVAL).await;
                }
            }
        }
    })
}

fn push_fetched_item(
    item: TimedFetchResult,
    fetch_stats: &std::sync::Mutex<FetchStats>,
    fetch_consumed_total: &AtomicU64,
    pending: &mut Vec<(u64, FinalizedBlock)>,
) -> Result<()> {
    let (n, block, timings) = item?;
    fetch_consumed_total.fetch_add(1, Ordering::Relaxed);
    fetch_stats
        .lock()
        .expect("chain-data fetch stats poisoned")
        .durations_ms
        .push(timings.total_ms);
    pending.push((n, block));
    Ok(())
}

async fn fetch_block_with_retry<S: ChainDataIngestSource>(
    source: &S,
    block_number: u64,
    max_retries: u32,
    initial_backoff: Duration,
    fetch_stats: &std::sync::Mutex<FetchStats>,
    fetch_progress: &FetchProgress,
) -> TimedFetchResult {
    let mut attempt = 0;
    let mut backoff = initial_backoff;
    let started = Instant::now();
    fetch_progress.started.fetch_add(1, Ordering::Relaxed);
    loop {
        match source.fetch_finalized_block(block_number).await {
            Ok(block) => {
                let total_ms = started.elapsed().as_millis() as u64;
                {
                    let mut stats = fetch_stats.lock().expect("chain-data fetch stats poisoned");
                    stats.completed += 1;
                }
                fetch_progress.completed.fetch_add(1, Ordering::Relaxed);
                return Ok((block_number, block, FetchTimings { total_ms }));
            }
            Err(error) if attempt < max_retries => {
                attempt += 1;
                {
                    let mut stats = fetch_stats.lock().expect("chain-data fetch stats poisoned");
                    stats.retry_attempts += 1;
                }
                fetch_progress
                    .retry_attempts
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    %error,
                    block = block_number,
                    attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    "chain-data archive fetch failed; retrying"
                );
                tokio::time::sleep(jittered_backoff(backoff)).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
            Err(error) => {
                fetch_progress.completed.fetch_add(1, Ordering::Relaxed);
                return Err(error)
                    .with_context(|| format!("fetching chain-data block {block_number}"));
            }
        }
    }
}

fn jittered_backoff(backoff: Duration) -> Duration {
    let millis = backoff.as_millis() as u64;
    if millis == 0 {
        return backoff;
    }
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
        % millis.max(1);
    Duration::from_millis(millis.saturating_add(jitter / 2))
}

pub fn build_reader_writer_service<M: MetaStoreCas, B: BlobStore>(
    meta_store: M,
    blob_store: B,
    cache_config: crate::store::CacheConfig,
    owner_id: u64,
    lease_blocks: u64,
    renew_threshold_blocks: u64,
    observed_upstream: Arc<AtomicU64>,
) -> Arc<MonadChainDataService<M, B>> {
    let observe = observed_upstream;
    let observe_upstream: crate::ObserveUpstream =
        Arc::new(move || match observe.load(Ordering::SeqCst) {
            u64::MAX => None,
            block => Some(block),
        });
    Arc::new(MonadChainDataService::new_reader_writer_with_cache_config(
        meta_store,
        blob_store,
        QueryLimits::UNLIMITED,
        cache_config,
        owner_id,
        lease_blocks,
        renew_threshold_blocks,
        observe_upstream,
    ))
}

fn round_2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
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
