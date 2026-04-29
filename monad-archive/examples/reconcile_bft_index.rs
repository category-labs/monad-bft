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

//! Phase 4a of the BFT migration: completeness pass.
//!
//! Walks the sink index across `[min..=max]`. For every indexed seq_num it
//! verifies:
//!   1. the entry's header exists in the source and decodes,
//!   2. the header's `seq_num` matches the index slot,
//!   3. the parent_id forms a contiguous hash chain with seq_num - 1,
//!   4. the header's body is reachable in the source.
//! Final postcondition: the sink has no `bft/index_markers/*` left over.
//!
//! Distinct from `verify_bft_copy_correctness.rs`, which samples bytes for
//! drift detection but does not catch gaps.

use std::path::PathBuf;

use alloy_rlp::Decodable;
use clap::Parser;
use eyre::{bail, Context, Result};
use futures::stream::{self, StreamExt};
use monad_archive::{
    cli::ArchiveArgs,
    kvstore::{KVReader, KVStoreErased},
    metrics::Metrics,
    model::{
        bft_ledger::BftBlockHeader,
        bft_paths,
        index_backend::{IndexBackend, IndexBackendErased, KvIndexBackend, PagedIndexBackend},
    },
};
use monad_types::BlockId;
use serde::Serialize;

#[derive(Debug, Parser)]
#[clap(about = "Phase 4a completeness pass: validate the BFT index against the source archive")]
struct Args {
    /// Source archive containing the legacy `bft_block/*.{header,body}` data
    /// (typically a local redb during the verify phase, or S3 after upload)
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    source: ArchiveArgs,

    /// Sink archive holding the new index + markers
    #[clap(long, value_parser = clap::value_parser!(ArchiveArgs))]
    sink: ArchiveArgs,

    /// First seq_num to check (inclusive)
    #[clap(long)]
    min_seq_num: u64,

    /// Last seq_num to check (inclusive)
    #[clap(long)]
    max_seq_num: u64,

    /// Treat sink index as paged (`bft/index/<page>`). Default is the
    /// per-key local layout (`bft/index_legacy_perkey/<n>`).
    #[clap(long)]
    paged_sink: bool,

    /// Skip the body-presence check (faster when bodies are still on the
    /// legacy path during a partial migration).
    #[clap(long)]
    skip_body_check: bool,

    /// Concurrency for source header / body lookups.
    #[clap(long, default_value_t = 256)]
    concurrency: usize,

    /// Optional path to write the structured JSON report to.
    #[clap(long)]
    json_out: Option<PathBuf>,

    /// Cap on the number of failures of each kind to keep in the report.
    #[clap(long, default_value_t = 100)]
    max_failures_per_kind: usize,

    /// Process the range in fixed windows of this many seq_nums to bound RAM
    /// regardless of [min..=max] size. Only the last (BlockId) of each window
    /// carries across the boundary for the chain-contiguity check.
    #[clap(long, default_value_t = 100_000)]
    window_size: u64,
}

#[derive(Debug, Default, Serialize)]
struct ReconcileReport {
    min_seq_num: u64,
    max_seq_num: u64,
    total_checked: u64,
    indexed: u64,
    missing_index: u64,
    header_missing: u64,
    header_seq_mismatch: u64,
    chain_breaks: u64,
    body_missing: u64,
    leftover_markers: usize,
    sample_missing_index: Vec<u64>,
    sample_header_missing: Vec<u64>,
    sample_header_seq_mismatch: Vec<u64>,
    sample_chain_breaks: Vec<u64>,
    sample_body_missing: Vec<u64>,
}

impl ReconcileReport {
    fn record_missing_index(&mut self, seq: u64, cap: usize) {
        self.missing_index = self.missing_index.saturating_add(1);
        if self.sample_missing_index.len() < cap {
            self.sample_missing_index.push(seq);
        }
    }
    fn record_header_missing(&mut self, seq: u64, cap: usize) {
        self.header_missing = self.header_missing.saturating_add(1);
        if self.sample_header_missing.len() < cap {
            self.sample_header_missing.push(seq);
        }
    }
    fn record_header_seq_mismatch(&mut self, seq: u64, cap: usize) {
        self.header_seq_mismatch = self.header_seq_mismatch.saturating_add(1);
        if self.sample_header_seq_mismatch.len() < cap {
            self.sample_header_seq_mismatch.push(seq);
        }
    }
    fn record_chain_break(&mut self, seq: u64, cap: usize) {
        self.chain_breaks = self.chain_breaks.saturating_add(1);
        if self.sample_chain_breaks.len() < cap {
            self.sample_chain_breaks.push(seq);
        }
    }
    fn record_body_missing(&mut self, seq: u64, cap: usize) {
        self.body_missing = self.body_missing.saturating_add(1);
        if self.sample_body_missing.len() < cap {
            self.sample_body_missing.push(seq);
        }
    }

    fn any_failures(&self) -> bool {
        self.missing_index
            + self.header_missing
            + self.header_seq_mismatch
            + self.chain_breaks
            + self.body_missing
            > 0
            || self.leftover_markers > 0
    }
}

/// Load the index entry for one seq_num plus the header that entry points to.
async fn load_indexed_header(
    seq: u64,
    sink: &IndexBackendErased,
    source: &KVStoreErased,
) -> Result<Option<(BlockId, Option<BftBlockHeader>)>> {
    let Some(id) = sink.get_id(seq).await? else {
        return Ok(None);
    };
    let key = bft_paths::legacy_header_path(&id);
    let bytes = source
        .get(&key)
        .await
        .wrap_err_with(|| format!("source GET failed for {key}"))?;
    match bytes {
        None => Ok(Some((id, None))),
        Some(b) => {
            let header = BftBlockHeader::decode(&mut &b[..])
                .wrap_err_with(|| format!("failed to decode source header at {key}"))?;
            Ok(Some((id, Some(header))))
        }
    }
}

/// Process one window of seq_nums end-to-end (passes 1-3) and return the
/// BlockId at the last slot of the window (or `None` if it was unindexed).
/// `prev_id` is the carried BlockId from the previous window's last slot.
#[allow(clippy::too_many_arguments)]
async fn process_window(
    window_start: u64,
    window_end: u64,
    sink_index: &IndexBackendErased,
    source: &KVStoreErased,
    concurrency: usize,
    cap: usize,
    skip_body_check: bool,
    is_first_window: bool,
    prev_id: Option<BlockId>,
    report: &mut ReconcileReport,
) -> Result<Option<BlockId>> {
    let span = (window_end - window_start + 1) as usize;
    let mut indexed: Vec<Option<(BlockId, Option<BftBlockHeader>)>> = vec![None; span];

    // Pass 1: parallel index + header lookup, bounded by concurrency.
    let lookups = stream::iter(window_start..=window_end)
        .map(|seq| {
            let sink_index = sink_index.clone();
            let source = source.clone();
            async move {
                let res = load_indexed_header(seq, &sink_index, &source).await;
                (seq, res)
            }
        })
        .buffer_unordered(concurrency);

    futures::pin_mut!(lookups);
    while let Some((seq, res)) = lookups.next().await {
        let res = res.wrap_err_with(|| format!("lookup failed at seq_num {seq}"))?;
        let slot = (seq - window_start) as usize;
        match &res {
            None => report.record_missing_index(seq, cap),
            Some((_, None)) => {
                report.indexed = report.indexed.saturating_add(1);
                report.record_header_missing(seq, cap);
            }
            Some((_, Some(header))) => {
                report.indexed = report.indexed.saturating_add(1);
                if header.seq_num.0 != seq {
                    report.record_header_seq_mismatch(seq, cap);
                }
            }
        }
        indexed[slot] = res;
    }

    // Pass 2: chain contiguity within the window, plus boundary check against
    // the previous window's tail via `prev_id`.
    for slot in 0..span {
        let seq = window_start + slot as u64;
        let Some((_, Some(this_header))) = indexed[slot].as_ref() else {
            continue;
        };
        let predecessor: Option<&BlockId> = if slot == 0 {
            if is_first_window {
                continue;
            }
            prev_id.as_ref()
        } else {
            indexed[slot - 1].as_ref().map(|(id, _)| id)
        };
        let Some(prev) = predecessor else {
            continue;
        };
        if this_header.get_parent_id() != *prev {
            report.record_chain_break(seq, cap);
        }
    }

    // Pass 3: body presence within window. Only checks slots with a decoded
    // header.
    if !skip_body_check {
        let body_lookups = stream::iter(window_start..=window_end)
            .filter_map(|seq| {
                let slot = (seq - window_start) as usize;
                let body_id = indexed[slot]
                    .as_ref()
                    .and_then(|(_, h)| h.as_ref().map(|h| h.block_body_id.0));
                async move { body_id.map(|b| (seq, b)) }
            })
            .map(|(seq, body_id)| {
                let source = source.clone();
                async move {
                    let key = bft_paths::legacy_body_path(&body_id);
                    let exists = source.exists(&key).await?;
                    Ok::<_, eyre::Report>((seq, exists))
                }
            })
            .buffer_unordered(concurrency);

        futures::pin_mut!(body_lookups);
        while let Some(res) = body_lookups.next().await {
            let (seq, exists) = res?;
            if !exists {
                report.record_body_missing(seq, cap);
            }
        }
    }

    // Carry: BlockId at the last slot of this window. None if the slot was
    // unindexed — in that case the chain-contiguity check at the next
    // window's first slot is naturally skipped.
    Ok(indexed[span - 1].as_ref().map(|(id, _)| *id))
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

    let sink_index: IndexBackendErased = if args.paged_sink {
        PagedIndexBackend::new(sink.store.clone()).into()
    } else {
        KvIndexBackend::new(sink.store.clone()).into()
    };

    if args.window_size == 0 {
        bail!("--window-size must be > 0");
    }

    let cap = args.max_failures_per_kind;
    let mut report = ReconcileReport {
        min_seq_num: args.min_seq_num,
        max_seq_num: args.max_seq_num,
        total_checked: args.max_seq_num - args.min_seq_num + 1,
        ..Default::default()
    };

    // Window the work. Holding all headers in RAM is fine for sub-chain-sized
    // verifies but blows up at the full 120M-row scale; window_size bounds
    // peak memory regardless of the [min..=max] span. Across windows we carry
    // only the last slot's BlockId — enough for the chain-contiguity check at
    // the boundary.
    let mut prev_id: Option<BlockId> = None;
    let mut window_start = args.min_seq_num;
    while window_start <= args.max_seq_num {
        let window_end = window_start
            .saturating_add(args.window_size - 1)
            .min(args.max_seq_num);
        let is_first_window = window_start == args.min_seq_num;
        prev_id = process_window(
            window_start,
            window_end,
            &sink_index,
            &source.store,
            args.concurrency,
            cap,
            args.skip_body_check,
            is_first_window,
            prev_id,
            &mut report,
        )
        .await?;
        window_start = window_end + 1;
    }

    // Pass 4: markers postcondition.
    let leftover = sink
        .store
        .scan_prefix(bft_paths::markers_prefix())
        .await
        .wrap_err("scan_prefix on markers failed")?;
    report.leftover_markers = leftover.len();

    if let Some(path) = &args.json_out {
        let bytes = serde_json::to_vec_pretty(&report).wrap_err("serialize report")?;
        std::fs::write(path, bytes)
            .wrap_err_with(|| format!("failed to write {}", path.display()))?;
    }

    println!(
        "Reconcile [{}..={}]: indexed={}/{} missing_index={} header_missing={} \
         header_seq_mismatch={} chain_breaks={} body_missing={} leftover_markers={}",
        report.min_seq_num,
        report.max_seq_num,
        report.indexed,
        report.total_checked,
        report.missing_index,
        report.header_missing,
        report.header_seq_mismatch,
        report.chain_breaks,
        report.body_missing,
        report.leftover_markers,
    );

    if report.any_failures() {
        eprintln!("Reconciliation FAILED");
        std::process::exit(1);
    }
    println!("Reconciliation OK");
    Ok(())
}
