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
//! on `/mnt/staging`). Bodies are intentionally skipped — they migrate via
//! server-side `CopyObject` after indexing. The inventory file lists the
//! authoritative set of keys to pull, one per line; lines starting with `#`
//! and blank lines are ignored.

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
    kvstore::{KVReader, KVStore, WritePolicy},
    metrics::Metrics,
    model::bft_paths,
};

#[derive(Debug, Parser)]
#[clap(about = "Pull legacy bft_block/*.header keys from source archive into a local sink")]
struct Args {
    /// Source archive containing the legacy `bft_block/<id>.header` keys
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    source: ArchiveArgs,

    /// Sink archive to write headers into (typically a local redb)
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    sink: ArchiveArgs,

    /// Path to a newline-separated list of legacy header keys
    /// (e.g. `bft_block/<hex>.header`). Comments (`#`) and blank lines OK.
    #[clap(long)]
    inventory: PathBuf,

    /// Maximum concurrent in-flight GET+PUT pairs
    #[clap(long, default_value_t = 1_000)]
    concurrency: usize,

    /// Skip existence check on the sink before pulling. Faster on cold sinks
    /// but redundant work on resumed runs.
    #[clap(long)]
    no_skip_existing: bool,
}

fn parse_inventory(contents: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    for (idx, raw) in contents.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !line.starts_with(bft_paths::legacy_prefix()) || !line.ends_with(".header") {
            bail!(
                "Inventory line {}: key {line} is not a legacy header key (expected `{prefix}<id>.header`)",
                idx + 1,
                prefix = bft_paths::legacy_prefix(),
            );
        }
        keys.push(line.to_string());
    }
    Ok(keys)
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

    let inventory_text = std::fs::read_to_string(&args.inventory)
        .wrap_err_with(|| format!("failed to read inventory {}", args.inventory.display()))?;
    let keys = parse_inventory(&inventory_text)?;
    let total = keys.len();
    if total == 0 {
        bail!("Inventory at {} contained no header keys", args.inventory.display());
    }
    tracing::info!(total, "Loaded inventory");

    let source_store = source.store;
    let sink_store = sink.store;
    let pulled = AtomicU64::new(0);
    let skipped = AtomicU64::new(0);
    let started = Instant::now();

    let skip_existing = !args.no_skip_existing;

    stream::iter(keys.into_iter())
        .map(|key| {
            let source = source_store.clone();
            let sink = sink_store.clone();
            let pulled = &pulled;
            let skipped = &skipped;
            async move {
                if skip_existing && sink.exists(&key).await? {
                    skipped.fetch_add(1, Ordering::Relaxed);
                    return Ok::<_, eyre::Report>(());
                }
                let bytes = source
                    .get(&key)
                    .await
                    .wrap_err_with(|| format!("source GET failed for {key}"))?;
                let bytes = match bytes {
                    Some(b) => b,
                    None => bail!("source missing legacy header key {key}"),
                };
                sink.put(&key, bytes.to_vec(), WritePolicy::NoClobber)
                    .await
                    .wrap_err_with(|| format!("sink PUT failed for {key}"))?;
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
        .buffer_unordered(args.concurrency)
        .for_each(|res| async {
            if let Err(e) = res {
                tracing::error!(?e, "pull error");
            }
        })
        .await;

    let pulled = pulled.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "Pull complete: pulled={pulled}, skipped_existing={skipped}, total={total}, elapsed={elapsed:.1}s",
    );

    if pulled + skipped < total as u64 {
        bail!(
            "incomplete pull: {} keys errored",
            total as u64 - pulled - skipped
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_inventory_strips_blanks_and_comments() {
        let inv = "# comment\n\nbft_block/aa.header\n\nbft_block/bb.header\n";
        let keys = parse_inventory(inv).unwrap();
        assert_eq!(keys, vec!["bft_block/aa.header", "bft_block/bb.header"]);
    }

    #[test]
    fn parse_inventory_rejects_non_header_keys() {
        let err = parse_inventory("bft_block/aa.body\n").unwrap_err();
        assert!(err.to_string().contains("not a legacy header key"));
    }

    #[test]
    fn parse_inventory_rejects_unrelated_prefix() {
        let err = parse_inventory("other/aa.header\n").unwrap_err();
        assert!(err.to_string().contains("not a legacy header key"));
    }
}
