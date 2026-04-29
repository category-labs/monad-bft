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
//! The inventory file lists the legacy body keys to migrate, one per line
//! (blank lines and `#` comments allowed). HEAD-based idempotency lets the
//! tool resume safely after partial runs. `--delete-source` is opt-in and
//! intended to run only after correctness has been confirmed end-to-end.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use clap::Parser;
use eyre::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use monad_archive::{
    cli::ArchiveArgs,
    kvstore::{s3::Bucket, KVStore, KVStoreErased},
    metrics::Metrics,
    model::bft_paths,
};
use monad_types::Hash;

#[derive(Debug, Parser)]
#[clap(about = "Server-side copy legacy BFT bodies into the new sharded path")]
struct Args {
    /// Source archive — the bucket containing `bft_block/<hash>.body`
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    source: ArchiveArgs,

    /// Destination archive — receives `bft/ledger/bodies/<shard>/<hash>`.
    /// May be the same physical bucket as `--source`.
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    sink: ArchiveArgs,

    /// Path to a newline-separated list of legacy body keys
    /// (`bft_block/<hex>.body`). `#` comments and blanks OK.
    #[clap(long)]
    inventory: PathBuf,

    /// Maximum concurrent in-flight CopyObject calls
    #[clap(long, default_value_t = 256)]
    concurrency: usize,

    /// Delete the legacy source object after a successful copy.
    /// OFF by default — only enable after end-to-end verification.
    #[clap(long)]
    delete_source: bool,
}

fn parse_inventory(contents: &str) -> Result<Vec<(String, Hash)>> {
    let mut keys = Vec::new();
    for (idx, raw) in contents.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let body_id = bft_paths::parse_legacy_body_path(line).ok_or_else(|| {
            eyre::eyre!(
                "Inventory line {}: key {line} is not a legacy body key",
                idx + 1
            )
        })?;
        keys.push((line.to_string(), body_id));
    }
    Ok(keys)
}

fn require_bucket(store: KVStoreErased, label: &str) -> Result<Bucket> {
    match store {
        KVStoreErased::Bucket(b) => Ok(b),
        _ => bail!(
            "{label} must be an AWS S3 bucket archive (server-side CopyObject is S3-only)"
        ),
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

    let source_bucket = require_bucket(source.store, "--source")?;
    let sink_bucket = require_bucket(sink.store, "--sink")?;
    let source_bucket_name = source_bucket.bucket_name().to_owned();

    let inventory_text = std::fs::read_to_string(&args.inventory)
        .wrap_err_with(|| format!("failed to read inventory {}", args.inventory.display()))?;
    let entries = parse_inventory(&inventory_text)?;
    let total = entries.len();
    if total == 0 {
        bail!("Inventory at {} contained no body keys", args.inventory.display());
    }
    tracing::info!(total, "Loaded inventory");

    let copied = AtomicU64::new(0);
    let skipped = AtomicU64::new(0);
    let deleted = AtomicU64::new(0);
    let started = Instant::now();

    stream::iter(entries.into_iter())
        .map(|(legacy_key, body_id)| {
            let source = source_bucket.clone();
            let sink = sink_bucket.clone();
            let source_bucket_name = source_bucket_name.clone();
            let copied = &copied;
            let skipped = &skipped;
            let deleted = &deleted;
            let delete_source = args.delete_source;
            async move {
                let dst_key = bft_paths::body_path(&body_id);

                let src_meta = source
                    .head_meta(&legacy_key)
                    .await
                    .wrap_err_with(|| format!("HEAD source failed for {legacy_key}"))?;
                let Some(src_meta) = src_meta else {
                    bail!("source missing legacy body key {legacy_key}");
                };
                let dst_meta = sink
                    .head_meta(&dst_key)
                    .await
                    .wrap_err_with(|| format!("HEAD sink failed for {dst_key}"))?;

                // Skip when destination exists AND looks identical:
                //   - simple objects: ETag is plain MD5, exact match means same bytes.
                //   - multipart objects: ETag is `<md5>-<parts>` and can differ
                //     between source and a CopyObject'd destination even when the
                //     bytes are identical, so fall back to content-length match.
                //     Same-region S3 CopyObject preserves bytes (and thus length),
                //     so a length match on a present destination is a sound
                //     idempotency signal that avoids pointless re-copies of large
                //     multipart bodies.
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
                    sink.copy_from(&source_bucket_name, &legacy_key, &dst_key)
                        .await
                        .wrap_err_with(|| {
                            format!("CopyObject failed: {legacy_key} -> {dst_key}")
                        })?;
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
                        .delete(&legacy_key)
                        .await
                        .wrap_err_with(|| format!("DeleteObject failed for {legacy_key}"))?;
                    deleted.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
        })
        .buffer_unordered(args.concurrency)
        .for_each(|res: Result<()>| async {
            if let Err(e) = res {
                tracing::error!(?e, "body copy error");
            }
        })
        .await;

    let copied = copied.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    let deleted = deleted.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "Body migration: copied={copied}, skipped_existing={skipped}, deleted_source={deleted}, total={total}, elapsed={elapsed:.1}s",
    );

    if copied + skipped < total as u64 {
        bail!(
            "incomplete migration: {} keys errored",
            total as u64 - copied - skipped
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_inventory_extracts_body_id() {
        let id = "aa".repeat(32);
        let inv = format!("# comment\n\nbft_block/{id}.body\n");
        let entries = parse_inventory(&inv).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, format!("bft_block/{id}.body"));
        assert_eq!(entries[0].1 .0, [0xAAu8; 32]);
    }

    #[test]
    fn parse_inventory_rejects_non_body_key() {
        let id = "aa".repeat(32);
        let inv = format!("bft_block/{id}.header\n");
        let err = parse_inventory(&inv).unwrap_err();
        assert!(err.to_string().contains("not a legacy body key"));
    }
}
