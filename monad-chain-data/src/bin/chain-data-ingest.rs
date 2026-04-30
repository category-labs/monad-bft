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

use std::{path::PathBuf, time::Duration};

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::Bytes;
use clap::Parser;
use eyre::{bail, Context, Result};
use monad_archive::{
    cli::BlockDataReaderArgs,
    metrics::Metrics,
    model::{
        block_data_archive::{Block, BlockReceipts},
        BlockDataReader,
    },
};
use monad_chain_data::{
    store::FjallStore, FinalizedBlock, IngestTx, MonadChainDataService, QueryLimits,
};
use tracing::{info, warn, Level};

#[derive(Parser, Debug)]
#[command(
    name = "chain-data-ingest",
    about = "Stream blocks + receipts from a monad-archive source into a local chain-data store"
)]
struct Cli {
    /// fjall data directory. Created on first run.
    #[arg(long)]
    data_dir: PathBuf,

    /// Archive source. Examples: `"fs /var/lib/monad-archive"`,
    /// `"aws my-bucket"`, `"mongodb mongodb://host:27017 dbname"`.
    /// See `monad_archive::cli::BlockDataReaderArgs` for the full grammar.
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    block_data_source: BlockDataReaderArgs,

    /// First block to ingest (inclusive). Must be `1` for a fresh data
    /// directory; on resume it must be `published_head + 1`.
    #[arg(long)]
    start: u64,

    /// Last block to ingest (inclusive).
    #[arg(long)]
    end: u64,

    /// Optional OTel collector endpoint for archive-side metrics. Off by
    /// default; archive readers still build without it.
    #[arg(long)]
    otel_endpoint: Option<String>,

    /// How often to log progress, in blocks.
    #[arg(long, default_value_t = 1000)]
    log_every: u64,
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

    let cli = Cli::parse();
    if cli.start > cli.end {
        bail!("start ({}) must be <= end ({})", cli.start, cli.end);
    }

    let store = FjallStore::open(&cli.data_dir)
        .with_context(|| format!("opening fjall store at {}", cli.data_dir.display()))?;
    let service = MonadChainDataService::new(store.clone(), store, QueryLimits::UNLIMITED);

    // Sanity check the resume point against the publication head before any
    // archive I/O, so a misconfigured `--start` fails fast rather than
    // burning a fetch.
    let head = service
        .publication()
        .load_published_head()
        .await
        .context("loading current publication head")?;
    let expected_start = head.map_or(1, |h| h + 1);
    if cli.start != expected_start {
        bail!(
            "start={} does not match expected next block {} (current head: {:?})",
            cli.start,
            expected_start,
            head
        );
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

    info!(
        start = cli.start,
        end = cli.end,
        data_dir = %cli.data_dir.display(),
        "starting ingest"
    );

    let mut total_logs: u64 = 0;
    let mut total_txs: u64 = 0;
    for n in cli.start..=cli.end {
        let block = reader
            .get_block_by_number(n)
            .await
            .with_context(|| format!("fetching block {n}"))?;
        let receipts = reader
            .get_block_receipts(n)
            .await
            .with_context(|| format!("fetching receipts for block {n}"))?;

        let finalized = into_finalized_block(block, receipts)
            .with_context(|| format!("transforming block {n}"))?;
        let outcome = service
            .ingest_block(finalized)
            .await
            .with_context(|| format!("ingesting block {n}"))?;

        total_logs += outcome.written_logs as u64;
        total_txs += outcome.written_txs as u64;

        if n % cli.log_every == 0 || n == cli.end {
            info!(block = n, total_txs, total_logs, "ingest progress");
        }
    }

    info!(end = cli.end, total_txs, total_logs, "ingest complete");
    Ok(())
}

fn into_finalized_block(block: Block, receipts: BlockReceipts) -> Result<FinalizedBlock> {
    let header = block.header;
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
            header.number,
            txs.len(),
            receipts.len()
        );
    }
    if txs.is_empty() && !receipts.is_empty() {
        // Should not happen for valid archives, but worth surfacing rather
        // than silently dropping receipts.
        warn!(
            block = header.number,
            receipts = receipts.len(),
            "block has receipts but no transactions; ignoring receipts"
        );
    }

    let logs_by_tx: Vec<Vec<alloy_primitives::Log>> = receipts
        .into_iter()
        .map(|r| r.receipt.logs().to_vec())
        .collect();

    Ok(FinalizedBlock {
        header,
        logs_by_tx,
        txs,
    })
}
