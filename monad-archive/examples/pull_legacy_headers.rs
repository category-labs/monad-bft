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

//! Phase 2 of the BFT migration: bulk-pull every legacy `bft_block/*.header`
//! key from the source archive into a local sink (typically a redb database
//! on `/mnt/staging`).
//!
//! No inventory file is required. The tool drives a streaming LIST against
//! the source bucket and pipes each page directly into a parallel GET+PUT
//! pipeline. To get useful parallelism on the LIST itself, the work is
//! sharded by hex sub-prefix (`bft_block/0`, `bft_block/1`, ...). Each shard
//! maintains its own resume cursor in the sink (`bft/migration/pull_cursor/<shard>`),
//! so a re-run picks up exactly where the last one left off without
//! re-LISTing already-processed prefixes.
//!
//! Bodies are intentionally skipped — they migrate via server-side
//! `CopyObject` after indexing.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use clap::Parser;
use eyre::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use monad_archive::{
    cli::ArchiveArgs,
    kvstore::{KVReader, KVStore, KVStoreErased, WritePolicy},
    metrics::Metrics,
    model::bft_paths,
};
use tokio::sync::Mutex;

const LIST_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Parser)]
#[clap(about = "Stream legacy bft_block/*.header keys from source archive into a local sink")]
struct Args {
    /// Source archive containing the legacy `bft_block/<id>.header` keys
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    source: ArchiveArgs,

    /// Sink archive to write headers into (typically a local redb)
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    sink: ArchiveArgs,

    /// Number of leading hex chars to use for shard fan-out. Default 1
    /// produces 16 shards (`bft_block/0`..`bft_block/f`); 2 produces 256.
    /// Each shard runs an independent LIST loop in parallel; raise this if
    /// LIST throughput is the bottleneck, lower it on small datasets.
    #[clap(long, default_value_t = 1)]
    shard_hex_chars: u32,

    /// Maximum concurrent in-flight GET+PUT pairs per shard
    #[clap(long, default_value_t = 256)]
    fetch_concurrency: usize,

    /// Skip GET on the sink before pulling. Saves a redb lookup per key on
    /// cold sinks; the resume cursor already prevents re-fetching across runs.
    #[clap(long)]
    no_skip_existing: bool,

    /// Abort on the first error instead of accumulating failures. In-flight
    /// pulls finish; no new pulls are dispatched after the first failure.
    #[clap(long)]
    fail_fast: bool,

    /// Cap on the number of failure samples retained for the exit report.
    /// (The total failure count is always reported.)
    #[clap(long, default_value_t = 100)]
    max_failures_sample: usize,
}

fn enumerate_shards(hex_chars: u32) -> Vec<String> {
    if hex_chars == 0 {
        return vec![String::new()];
    }
    let count = 16u64.pow(hex_chars);
    (0..count)
        .map(|i| format!("{i:0width$x}", width = hex_chars as usize))
        .collect()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
    if args.shard_hex_chars > 4 {
        bail!("--shard-hex-chars > 4 is excessive (would create > 65k shards)");
    }
    let metrics = Metrics::none();

    let source = args
        .source
        .build_block_data_archive(&metrics)
        .await
        .wrap_err("failed to build source archive")?;
    let sink = args
        .sink
        .build_block_data_archive(&metrics)
        .await
        .wrap_err("failed to build sink archive")?;

    let shards = enumerate_shards(args.shard_hex_chars);
    tracing::info!(
        shards = shards.len(),
        fetch_concurrency = args.fetch_concurrency,
        "Starting bulk pull"
    );

    let pulled = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let abort = Arc::new(AtomicBool::new(false));
    let failure_sample: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let started = Instant::now();

    let skip_existing = !args.no_skip_existing;
    let fail_fast = args.fail_fast;
    let max_failures_sample = args.max_failures_sample;
    let fetch_concurrency = args.fetch_concurrency;

    let shard_results = stream::iter(shards.clone().into_iter().map(|shard| {
        let source = source.store.clone();
        let sink = sink.store.clone();
        let pulled = pulled.clone();
        let skipped = skipped.clone();
        let failures = failures.clone();
        let abort = abort.clone();
        let failure_sample = failure_sample.clone();
        async move {
            run_shard(
                shard,
                source,
                sink,
                fetch_concurrency,
                skip_existing,
                fail_fast,
                max_failures_sample,
                pulled,
                skipped,
                failures,
                abort,
                failure_sample,
                started,
            )
            .await
        }
    }))
    // Cap concurrent shards so total in-flight fetches
    // (= shard_concurrency * fetch_concurrency) stays bounded even when
    // shard_hex_chars=2 produces 256 shards.
    .buffer_unordered(shards.len().min(64).max(1))
    .collect::<Vec<_>>()
    .await;

    let pulled = pulled.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    let failures = failures.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "Pull complete: pulled={pulled}, skipped_existing={skipped}, failures={failures}, elapsed={elapsed:.1}s"
    );

    let shard_errors: Vec<String> = shard_results
        .into_iter()
        .filter_map(|r| r.err().map(|e| format!("{e:#}")))
        .collect();
    if !shard_errors.is_empty() {
        for e in shard_errors.iter().take(10) {
            eprintln!("shard error: {e}");
        }
        bail!(
            "{} shard(s) errored; sink may be incomplete",
            shard_errors.len()
        );
    }

    if failures > 0 {
        let sample = failure_sample.lock().await;
        eprintln!("Showing up to {} failure samples:", sample.len());
        for (key, err) in sample.iter() {
            eprintln!("  {key}: {err}");
        }
        bail!(
            "pull had {failures} per-key failures; sink is incomplete and the run cannot be marked as a successful Phase 2"
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_shard(
    shard: String,
    source: KVStoreErased,
    sink: KVStoreErased,
    fetch_concurrency: usize,
    skip_existing: bool,
    fail_fast: bool,
    max_failures_sample: usize,
    pulled: Arc<AtomicU64>,
    skipped: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
    abort: Arc<AtomicBool>,
    failure_sample: Arc<Mutex<Vec<(String, String)>>>,
    started: Instant,
) -> Result<()> {
    let shard_prefix = format!("{}{shard}", bft_paths::legacy_prefix());
    let cursor_key = bft_paths::pull_cursor_path(&shard);

    // Resume from the cursor in the sink, if any. The cursor stores the last
    // key successfully processed (header-pulled or body-skipped) on this shard.
    let mut cursor: String = match sink
        .get(&cursor_key)
        .await
        .wrap_err_with(|| format!("read pull cursor for shard {shard}"))?
    {
        Some(b) => String::from_utf8(b.to_vec())
            .wrap_err_with(|| format!("pull cursor for shard {shard} not utf8"))?,
        None => String::new(),
    };
    if !cursor.is_empty() {
        tracing::info!(shard, %cursor, "resuming shard from cursor");
    }

    loop {
        if abort.load(Ordering::Relaxed) {
            return Ok(());
        }
        let page = source
            .scan_prefix_after_with_max_keys(&shard_prefix, &cursor, LIST_PAGE_SIZE)
            .await
            .wrap_err_with(|| format!("LIST failed for shard {shard} after={cursor}"))?;
        if page.is_empty() {
            return Ok(());
        }

        let last_key = page.last().expect("non-empty page").clone();

        // Per-page GET+PUT pipeline. Bodies are listed alongside headers in
        // lex order; we filter to .header here and let bodies advance the
        // cursor without doing any I/O for them.
        let processed = stream::iter(page.into_iter())
            .map(|key| {
                let source = source.clone();
                let sink = sink.clone();
                let abort = abort.clone();
                let pulled = pulled.clone();
                let skipped = skipped.clone();
                async move {
                    if abort.load(Ordering::Relaxed) {
                        return Ok::<(), (String, eyre::Report)>(());
                    }
                    if !key.ends_with(bft_paths::legacy_header_suffix()) {
                        return Ok(());
                    }
                    if skip_existing {
                        match sink.exists(&key).await {
                            Ok(true) => {
                                skipped.fetch_add(1, Ordering::Relaxed);
                                return Ok(());
                            }
                            Ok(false) => {}
                            Err(e) => {
                                return Err((
                                    key.clone(),
                                    e.wrap_err("sink exists check failed"),
                                ))
                            }
                        }
                    }
                    let bytes = source
                        .get(&key)
                        .await
                        .wrap_err_with(|| format!("source GET failed for {key}"))
                        .map_err(|e| (key.clone(), e))?;
                    let bytes = match bytes {
                        Some(b) => b,
                        None => {
                            return Err((
                                key.clone(),
                                eyre::eyre!("source missing legacy header key {key}"),
                            ))
                        }
                    };
                    sink.put(&key, bytes.to_vec(), WritePolicy::NoClobber)
                        .await
                        .wrap_err_with(|| format!("sink PUT failed for {key}"))
                        .map_err(|e| (key.clone(), e))?;
                    let n = pulled.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_power_of_two() || n % 100_000 == 0 {
                        let elapsed = started.elapsed().as_secs_f64().max(0.001);
                        tracing::info!(
                            pulled = n,
                            skipped = skipped.load(Ordering::Relaxed),
                            rate_per_s = (n as f64 / elapsed) as u64,
                            "Pull progress"
                        );
                    }
                    Ok(())
                }
            })
            .buffer_unordered(fetch_concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut had_failure = false;
        for res in processed {
            if let Err((key, err)) = res {
                failures.fetch_add(1, Ordering::Relaxed);
                let err_str = format!("{err:#}");
                tracing::error!(key, err = %err_str, "pull error");
                {
                    let mut sample = failure_sample.lock().await;
                    if sample.len() < max_failures_sample {
                        sample.push((key, err_str));
                    }
                }
                had_failure = true;
            }
        }

        if had_failure && fail_fast {
            abort.store(true, Ordering::Relaxed);
            return Ok(());
        }

        // Always advance the in-process cursor so the loop makes forward
        // progress through the bucket. Persist the cursor to the sink only
        // when the page was fully clean — that way a process restart
        // re-attempts the failed keys. NoClobber on PUTs makes the retries
        // safe; skip_existing makes them cheap on already-persisted ones.
        if !had_failure {
            sink.put(
                &cursor_key,
                last_key.as_bytes().to_vec(),
                WritePolicy::AllowOverwrite,
            )
            .await
            .wrap_err_with(|| format!("write cursor for shard {shard}"))?;
        }
        cursor = last_key;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enumerate_shards_zero_returns_empty_string() {
        assert_eq!(enumerate_shards(0), vec![""]);
    }

    #[test]
    fn enumerate_shards_one_returns_16() {
        let s = enumerate_shards(1);
        assert_eq!(s.len(), 16);
        assert_eq!(s.first().unwrap(), "0");
        assert_eq!(s.last().unwrap(), "f");
    }

    #[test]
    fn enumerate_shards_two_returns_256_zero_padded() {
        let s = enumerate_shards(2);
        assert_eq!(s.len(), 256);
        assert_eq!(s.first().unwrap(), "00");
        assert_eq!(s.last().unwrap(), "ff");
    }
}
