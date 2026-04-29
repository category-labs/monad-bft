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

//! Phase 5 of the BFT migration: convert the verified local per-key index
//! into the paged S3 layout. Reads `[min..=max]` from a `KvIndexBackend`
//! (typically a local redb on `/mnt/staging`) and writes one paged blob per
//! 1K-block range into the S3 sink via `PagedIndexBackend::put_ids_batch`.
//!
//! Pages run under `buffer_unordered`; each page is independently
//! idempotent (deterministic content for a fixed input range). Boundary
//! pages — where `--min-seq-num` or `--max-seq-num` falls mid-page — are
//! safe because `put_ids_batch` is read-modify-write.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use clap::Parser;
use eyre::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use monad_archive::{
    cli::ArchiveArgs,
    metrics::Metrics,
    model::{
        bft_paths::{self, INDEX_PAGE_SIZE},
        index_backend::{IndexBackend, KvIndexBackend, PagedIndexBackend},
    },
};

#[derive(Debug, Parser)]
#[clap(about = "Upload local per-key BFT index to the S3 paged layout")]
struct Args {
    /// Source archive holding the per-key index (`bft/index_legacy_perkey/<n>`).
    /// Typically a local redb populated by the migration indexer.
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    source: ArchiveArgs,

    /// Sink archive that will hold the paged index (`bft/index/<page>`).
    /// Typically the production S3 bucket.
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    sink: ArchiveArgs,

    /// First seq_num to upload (inclusive)
    #[clap(long)]
    min_seq_num: u64,

    /// Last seq_num to upload (inclusive)
    #[clap(long)]
    max_seq_num: u64,

    /// Pages in flight in parallel. Each task does a sink-side RMW; the
    /// sink's internal write_lock means PUTs against the sink serialize
    /// regardless. Concurrency primarily helps overlap source reads with
    /// sink PUTs.
    #[clap(long, default_value_t = 32)]
    concurrency: usize,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
    if args.min_seq_num > args.max_seq_num {
        bail!(
            "--min-seq-num ({}) must be <= --max-seq-num ({})",
            args.min_seq_num,
            args.max_seq_num
        );
    }

    let metrics = Metrics::none();
    let source_archive = args
        .source
        .build_block_data_archive(&metrics)
        .await
        .wrap_err("failed to build source archive")?;
    let sink_archive = args
        .sink
        .build_block_data_archive(&metrics)
        .await
        .wrap_err("failed to build sink archive")?;

    let source = KvIndexBackend::new(source_archive.store);
    let sink = PagedIndexBackend::new(sink_archive.store);

    let first_page = bft_paths::index_page_for(args.min_seq_num);
    let last_page = bft_paths::index_page_for(args.max_seq_num);
    let total_pages = last_page - first_page + 1;
    tracing::info!(
        "uploading paged index for seq [{}..={}] ({} pages, concurrency={})",
        args.min_seq_num,
        args.max_seq_num,
        total_pages,
        args.concurrency
    );

    let started = Instant::now();
    let pages_done = Arc::new(AtomicU64::new(0));
    let entries_total = Arc::new(AtomicU64::new(0));
    let pages_skipped = Arc::new(AtomicU64::new(0));

    let min_seq = args.min_seq_num;
    let max_seq = args.max_seq_num;

    let mut stream = stream::iter(first_page..=last_page)
        .map(|page| {
            let source = source.clone();
            let sink = sink.clone();
            let pages_done = pages_done.clone();
            let entries_total = entries_total.clone();
            let pages_skipped = pages_skipped.clone();
            async move {
                let page_min = (page * INDEX_PAGE_SIZE).max(min_seq);
                let page_max = ((page + 1) * INDEX_PAGE_SIZE - 1).min(max_seq);

                let mut iter = source.iter_range(page_min, page_max);
                let mut entries: Vec<(u64, monad_types::BlockId)> = Vec::new();
                while let Some(r) = iter.next().await {
                    entries.push(r.wrap_err_with(|| {
                        format!("source iter_range failed in page {page}")
                    })?);
                }
                drop(iter);

                if entries.is_empty() {
                    pages_skipped.fetch_add(1, Ordering::Relaxed);
                    return Ok::<_, eyre::Report>((page, 0));
                }

                let n = entries.len();
                sink.put_ids_batch(&entries)
                    .await
                    .wrap_err_with(|| format!("sink put_ids_batch failed for page {page}"))?;
                entries_total.fetch_add(n as u64, Ordering::Relaxed);
                let done = pages_done.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 1_000 == 0 {
                    tracing::info!("progress: {done}/{total_pages} pages uploaded");
                }
                Ok((page, n))
            }
        })
        .buffer_unordered(args.concurrency);

    let mut errors: Vec<(u64, eyre::Report)> = Vec::new();
    while let Some(res) = stream.next().await {
        match res {
            Ok(_) => {}
            Err(e) => {
                errors.push((u64::MAX, e));
            }
        }
    }

    let elapsed = started.elapsed();
    let pages_done = pages_done.load(Ordering::Relaxed);
    let pages_skipped = pages_skipped.load(Ordering::Relaxed);
    let entries_total = entries_total.load(Ordering::Relaxed);
    tracing::info!(
        "done: {pages_done} pages uploaded, {pages_skipped} empty pages skipped, \
         {entries_total} entries written, {} errors, elapsed {:.1?}",
        errors.len(),
        elapsed,
    );

    if !errors.is_empty() {
        for (_, e) in errors.iter().take(10) {
            tracing::error!("upload failure: {e:?}");
        }
        bail!(
            "upload_index_pages failed: {} pages errored (showing up to 10 above)",
            errors.len()
        );
    }
    Ok(())
}
