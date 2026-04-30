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

//! Phase 5 of the BFT migration: server-side copy each legacy
//! `bft_block/<hash>.body` object into the new sharded path
//! `bft/ledger/bodies/<shard>/<hash>` using `CopyObject`. Same-region copies
//! incur no egress; we only pay request charges.
//!
//! No inventory file is required. The tool iterates the local redb header
//! cache populated by `pull_legacy_headers` — every header there carries a
//! `block_body_id` that names the legacy body key. A resume cursor stored
//! inside that same cache lets a crashed run pick up where it left off
//! without re-HEAD'ing every previously copied body.
//!
//! HEAD-based idempotency is still applied per-key: if the destination is
//! already present and matches (ETag for simple objects, content-length for
//! multipart), the copy is skipped. `--delete-source` is opt-in and intended
//! to run only after correctness has been confirmed end-to-end.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use alloy_rlp::Decodable;
use clap::Parser;
use eyre::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use monad_archive::{
    cli::ArchiveArgs,
    kvstore::{s3::Bucket, KVReader, KVStore, KVStoreErased, WritePolicy},
    metrics::Metrics,
    model::{bft_ledger::BftBlockHeader, bft_paths},
};

const HEADER_PAGE_SIZE: usize = 1_000;

#[derive(Debug, Parser)]
#[clap(about = "Server-side copy legacy BFT bodies into the new sharded path, driven by the local header cache")]
struct Args {
    /// Source archive — the bucket containing `bft_block/<hash>.body`
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    source: ArchiveArgs,

    /// Destination archive — receives `bft/ledger/bodies/<shard>/<hash>`.
    /// May be the same physical bucket as `--source`.
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    sink: ArchiveArgs,

    /// Local header cache (typically a redb populated by `pull_legacy_headers`).
    /// Each header's `block_body_id` names the legacy body key to migrate;
    /// the resume cursor is stored here as well.
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    header_cache: ArchiveArgs,

    /// Maximum concurrent in-flight CopyObject calls
    #[clap(long, default_value_t = 256)]
    concurrency: usize,

    /// Delete the legacy source object after a successful copy.
    /// OFF by default — only enable after end-to-end verification.
    #[clap(long)]
    delete_source: bool,

    /// Abort on the first per-key failure. In-flight copies finish; no new
    /// copies are dispatched after the first failure.
    #[clap(long)]
    fail_fast: bool,
}

fn require_bucket(store: KVStoreErased, label: &str) -> Result<Bucket> {
    match store {
        KVStoreErased::Bucket(b) => Ok(b),
        _ => bail!("{label} must be an AWS S3 bucket archive (server-side CopyObject is S3-only)"),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
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
    let header_cache = args
        .header_cache
        .build_block_data_archive(&metrics)
        .await
        .wrap_err("failed to build header cache archive")?;

    let source_bucket = require_bucket(source.store, "--source")?;
    let sink_bucket = require_bucket(sink.store, "--sink")?;
    let source_bucket_name = source_bucket.bucket_name().to_owned();
    let cache = header_cache.store;

    let cursor_key = bft_paths::body_copy_cursor_path().to_owned();
    let mut cursor: String = match cache
        .get(&cursor_key)
        .await
        .wrap_err("read body-copy cursor")?
    {
        Some(b) => String::from_utf8(b.to_vec()).wrap_err("body-copy cursor not utf8")?,
        None => String::new(),
    };
    if !cursor.is_empty() {
        tracing::info!(%cursor, "resuming body migration from cursor");
    }

    let copied = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let deleted = Arc::new(AtomicU64::new(0));
    let failures = Arc::new(AtomicU64::new(0));
    let abort = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let header_prefix = bft_paths::legacy_prefix().to_owned();

    'outer: loop {
        if abort.load(Ordering::Relaxed) {
            break;
        }
        let page = cache
            .scan_prefix_after_with_max_keys(&header_prefix, &cursor, HEADER_PAGE_SIZE)
            .await
            .wrap_err("scan header cache")?;
        if page.is_empty() {
            break;
        }
        let last_key = page.last().expect("non-empty page").clone();

        // Build (legacy_body_key, dst_body_key) pairs by decoding each header
        // out of the cache. We do the decode in the cache loop (sequential,
        // microsecond redb reads) and feed the resulting keys into the
        // CopyObject pipeline below.
        let mut copy_jobs: Vec<(String, String)> = Vec::with_capacity(page.len());
        for key in &page {
            if !key.ends_with(bft_paths::legacy_header_suffix()) {
                continue;
            }
            let bytes = match cache.get(key).await? {
                Some(b) => b,
                None => bail!("header cache scan listed {key} but get returned None"),
            };
            let header = BftBlockHeader::decode(&mut &bytes[..])
                .wrap_err_with(|| format!("decode cached header at {key}"))?;
            let body_id = header.block_body_id.0;
            let legacy = bft_paths::legacy_body_path(&body_id);
            let dst = bft_paths::body_path(&body_id);
            copy_jobs.push((legacy, dst));
        }

        let processed: Vec<Result<()>> = stream::iter(copy_jobs.into_iter())
            .map(|(legacy_key, dst_key)| {
                let source = source_bucket.clone();
                let sink = sink_bucket.clone();
                let source_bucket_name = source_bucket_name.clone();
                let copied = copied.clone();
                let skipped = skipped.clone();
                let deleted = deleted.clone();
                let abort = abort.clone();
                let delete_source = args.delete_source;
                let started = started;
                async move {
                    if abort.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    copy_one(
                        &source,
                        &sink,
                        &source_bucket_name,
                        &legacy_key,
                        &dst_key,
                        delete_source,
                        &copied,
                        &skipped,
                        &deleted,
                        started,
                    )
                    .await
                }
            })
            .buffer_unordered(args.concurrency)
            .collect()
            .await;

        let mut had_failure = false;
        for res in processed {
            if let Err(e) = res {
                failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(?e, "body copy error");
                had_failure = true;
            }
        }
        if had_failure && args.fail_fast {
            abort.store(true, Ordering::Relaxed);
            break 'outer;
        }

        // Persist the cursor only when the page was clean. In-process loop
        // advances the local cursor unconditionally so we make forward
        // progress; on restart we replay from the last clean checkpoint.
        if !had_failure {
            cache
                .put(
                    &cursor_key,
                    last_key.as_bytes().to_vec(),
                    WritePolicy::AllowOverwrite,
                )
                .await
                .wrap_err("write body-copy cursor")?;
        }
        cursor = last_key;
    }

    let copied = copied.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    let deleted = deleted.load(Ordering::Relaxed);
    let failures = failures.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "Body migration: copied={copied}, skipped_existing={skipped}, deleted_source={deleted}, failures={failures}, elapsed={elapsed:.1}s"
    );
    if failures > 0 {
        bail!("body migration had {failures} per-key failures; sink may be incomplete");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn copy_one(
    source: &Bucket,
    sink: &Bucket,
    source_bucket_name: &str,
    legacy_key: &str,
    dst_key: &str,
    delete_source: bool,
    copied: &AtomicU64,
    skipped: &AtomicU64,
    deleted: &AtomicU64,
    started: Instant,
) -> Result<()> {
    let src_meta = source
        .head_meta(legacy_key)
        .await
        .wrap_err_with(|| format!("HEAD source failed for {legacy_key}"))?;
    let Some(src_meta) = src_meta else {
        bail!("source missing legacy body key {legacy_key}");
    };
    let dst_meta = sink
        .head_meta(dst_key)
        .await
        .wrap_err_with(|| format!("HEAD sink failed for {dst_key}"))?;

    // Skip when destination exists AND looks identical:
    //   - simple objects: ETag is plain MD5, exact match means same bytes.
    //   - multipart objects: ETag is `<md5>-<parts>` and can differ between
    //     source and a CopyObject'd destination even when the bytes are
    //     identical, so fall back to content-length match. Same-region S3
    //     CopyObject preserves bytes (and thus length), so a length match on
    //     a present destination is a sound idempotency signal that avoids
    //     pointless re-copies of large multipart bodies.
    let is_multipart = src_meta
        .etag
        .as_deref()
        .or(dst_meta.as_ref().and_then(|m| m.etag.as_deref()))
        .map(|t| t.trim_matches('"').contains('-'))
        .unwrap_or(false);
    let needs_copy = match &dst_meta {
        None => true,
        Some(d) => {
            if is_multipart {
                d.size != src_meta.size || d.size.is_none()
            } else {
                d.etag != src_meta.etag || d.etag.is_none()
            }
        }
    };

    if needs_copy {
        sink.copy_from(source_bucket_name, legacy_key, dst_key)
            .await
            .wrap_err_with(|| format!("CopyObject failed: {legacy_key} -> {dst_key}"))?;
        let n = copied.fetch_add(1, Ordering::Relaxed) + 1;
        if n.is_power_of_two() || n % 100_000 == 0 {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            tracing::info!(
                copied = n,
                skipped = skipped.load(Ordering::Relaxed),
                rate_per_s = (n as f64 / elapsed) as u64,
                "Body copy progress"
            );
        }
    } else {
        skipped.fetch_add(1, Ordering::Relaxed);
    }

    if delete_source {
        source
            .delete(legacy_key)
            .await
            .wrap_err_with(|| format!("DeleteObject failed for {legacy_key}"))?;
        deleted.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}
