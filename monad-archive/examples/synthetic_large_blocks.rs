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

use std::{collections::HashSet, sync::Arc};

use alloy_consensus::{
    proofs::{calculate_receipt_root, calculate_transaction_root},
    ReceiptEnvelope, SignableTransaction, Transaction, TxEip1559,
};
use alloy_primitives::{Bloom, B256, U256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use clap::Parser;
use eyre::{ensure, Context, Result};
use futures::{stream, StreamExt, TryStreamExt};
use monad_archive::{
    archive_reader::LatestKind,
    cli::{ArchiveArgs, BlockDataReaderArgs},
    kvstore::WritePolicy,
    metrics::Metrics,
    model::{
        block_data_archive::{decode_trace, Block, BlockReceipts, BlockTraces},
        BlockDataReader, BlockDataReaderErased,
    },
};
use monad_eth_types::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use tracing::{info, Level};

#[derive(Debug, Parser)]
#[command(
    name = "synthetic-large-blocks",
    about = "Build a filesystem archive of large synthetic blocks from sampled real archive data"
)]
struct Cli {
    /// Archive source. Example:
    /// "aws mainnet-deu-009-0 --profile default --region us-east-2".
    #[arg(long, value_parser = clap::value_parser!(BlockDataReaderArgs))]
    source: BlockDataReaderArgs,

    /// Sink used to cache the real sampled blocks. Example: "fs /tmp/archive-real-cache".
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    cache_sink: ArchiveArgs,

    /// Sink where generated synthetic archive blocks are written. Example:
    /// "fs /tmp/archive-synthetic".
    #[arg(long, value_parser = clap::value_parser!(ArchiveArgs))]
    synthetic_sink: ArchiveArgs,

    /// First real block to sample.
    #[arg(long, default_value_t = 60_000_000)]
    sample_start: u64,

    /// Number of real blocks to sample and cache.
    #[arg(long, default_value_t = 10_000)]
    sample_count: u64,

    /// First synthetic block number.
    #[arg(long, default_value_t = 1)]
    synthetic_start: u64,

    /// Number of synthetic blocks to create.
    #[arg(long, default_value_t = 100)]
    synthetic_count: u64,

    /// Minimum synthetic transactions per block.
    #[arg(long, default_value_t = 5_000)]
    min_txs: usize,

    /// Maximum synthetic transactions per block.
    #[arg(long, default_value_t = 15_000)]
    max_txs: usize,

    /// Maximum concurrent real block fetch/cache tasks.
    #[arg(long, default_value_t = 128)]
    fetch_concurrency: usize,

    /// Deterministic RNG seed for tx sampling and synthetic tx signing key.
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Overwrite existing cache/synthetic archive keys.
    #[arg(long)]
    overwrite: bool,
}

#[derive(Clone)]
struct TxSample {
    tx: TxEnvelopeWithSender,
    receipt: ReceiptWithLogIndex,
    trace: Vec<u8>,
    gas_used: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let cli = Cli::parse();
    ensure!(cli.min_txs <= cli.max_txs, "--min-txs must be <= --max-txs");
    ensure!(cli.sample_count > 0, "--sample-count must be non-zero");
    ensure!(
        cli.synthetic_count > 0,
        "--synthetic-count must be non-zero"
    );

    let metrics = Metrics::none();
    let source = cli.source.build(&metrics).await?;
    let cache = cli.cache_sink.build_block_data_archive(&metrics).await?;
    let synthetic = cli
        .synthetic_sink
        .build_block_data_archive(&metrics)
        .await?;
    let write_policy = if cli.overwrite {
        WritePolicy::AllowOverwrite
    } else {
        WritePolicy::NoClobber
    };

    let sample_end = cli.sample_start + cli.sample_count - 1;
    info!(
        sample_start = cli.sample_start,
        sample_end, "caching real sample blocks"
    );
    cache_real_blocks(
        source,
        cache.clone(),
        cli.sample_start,
        sample_end,
        cli.fetch_concurrency,
        write_policy,
    )
    .await?;

    info!("loading cached transaction samples");
    let samples = load_samples(cache.clone(), cli.sample_start, sample_end).await?;
    ensure!(
        !samples.is_empty(),
        "sampled real blocks contained no transactions"
    );
    info!(samples = samples.len(), "loaded transaction samples");

    let signer = synthetic_signer(cli.seed)?;
    write_synthetic_blocks(
        synthetic,
        &samples,
        signer,
        cli.synthetic_start,
        cli.synthetic_count,
        cli.min_txs,
        cli.max_txs,
        cli.seed,
        write_policy,
    )
    .await?;

    Ok(())
}

async fn cache_real_blocks(
    source: BlockDataReaderErased,
    cache: monad_archive::model::block_data_archive::BlockDataArchive,
    start: u64,
    end: u64,
    concurrency: usize,
    policy: WritePolicy,
) -> Result<()> {
    let source = Arc::new(source);
    let cache = Arc::new(cache);

    stream::iter(start..=end)
        .map(|block_number| {
            let source = Arc::clone(&source);
            let cache = Arc::clone(&cache);
            async move {
                if policy == WritePolicy::NoClobber
                    && cache
                        .get_block_data_with_offsets(block_number)
                        .await
                        .is_ok()
                {
                    return Ok::<_, eyre::Report>(block_number);
                }

                let (block, receipts, traces) = futures::try_join!(
                    source.get_block_by_number(block_number),
                    source.get_block_receipts(block_number),
                    source.get_block_traces(block_number),
                )
                .wrap_err_with(|| format!("fetching real block {block_number}"))?;

                validate_block_parts(block_number, &block, &receipts, &traces)
                    .wrap_err_with(|| format!("validating real block {block_number}"))?;
                cache
                    .archive_block(block, policy)
                    .await
                    .wrap_err_with(|| format!("caching real block {block_number}"))?;
                cache
                    .archive_receipts(receipts, block_number, policy)
                    .await
                    .wrap_err_with(|| format!("caching real receipts {block_number}"))?;
                cache
                    .archive_traces(traces, block_number, policy)
                    .await
                    .wrap_err_with(|| format!("caching real traces {block_number}"))?;
                Ok::<_, eyre::Report>(block_number)
            }
        })
        .buffered(concurrency)
        .try_for_each(|block_number| {
            let cache = Arc::clone(&cache);
            async move {
                if block_number != 0 {
                    cache
                        .update_latest(block_number, LatestKind::Uploaded)
                        .await
                        .wrap_err_with(|| format!("updating cache latest to {block_number}"))?;
                }
                if block_number % 100 == 0 {
                    info!(block_number, "cached real block");
                }
                Ok(())
            }
        })
        .await
}

async fn load_samples(
    cache: monad_archive::model::block_data_archive::BlockDataArchive,
    start: u64,
    end: u64,
) -> Result<Vec<TxSample>> {
    let mut samples = Vec::new();

    for block_number in start..=end {
        let block_data = cache
            .get_block_data_with_offsets(block_number)
            .await
            .wrap_err_with(|| format!("reading cached block {block_number}"))?;
        validate_block_parts(
            block_number,
            &block_data.block,
            &block_data.receipts,
            &block_data.traces,
        )
        .wrap_err_with(|| format!("validating cached block {block_number}"))?;

        let mut previous_gas = 0_u64;
        samples.extend(
            block_data
                .block
                .body
                .transactions
                .into_iter()
                .zip(block_data.receipts)
                .zip(block_data.traces)
                .map(|((tx, receipt), trace)| {
                    let gas_used = receipt
                        .receipt
                        .cumulative_gas_used()
                        .saturating_sub(previous_gas)
                        .max(1);
                    previous_gas = receipt.receipt.cumulative_gas_used();
                    TxSample {
                        tx,
                        receipt,
                        trace,
                        gas_used,
                    }
                }),
        );

        if block_number % 100 == 0 {
            info!(
                block_number,
                samples = samples.len(),
                "loaded cached block samples"
            );
        }
    }

    Ok(samples)
}

async fn write_synthetic_blocks(
    synthetic: monad_archive::model::block_data_archive::BlockDataArchive,
    samples: &[TxSample],
    signer: PrivateKeySigner,
    start: u64,
    count: u64,
    min_txs: usize,
    max_txs: usize,
    seed: u64,
    policy: WritePolicy,
) -> Result<()> {
    let mut rng = ChaCha20Rng::seed_from_u64(seed);
    let mut parent_hash = B256::ZERO;
    let mut next_nonce = 0_u64;
    let mut seen_hashes = HashSet::new();

    for block_number in start..start + count {
        let tx_count = rng.gen_range(min_txs..=max_txs);
        let mut selected = Vec::with_capacity(tx_count);
        for _ in 0..tx_count {
            let sample_idx = rng.gen_range(0..samples.len());
            selected.push(samples[sample_idx].clone());
        }

        let (block, receipts, traces) = build_synthetic_block(
            block_number,
            parent_hash,
            selected,
            &signer,
            &mut next_nonce,
        )?;

        for tx in &block.body.transactions {
            ensure!(
                seen_hashes.insert(*tx.tx.tx_hash()),
                "duplicate synthetic tx hash generated: {}",
                tx.tx.tx_hash()
            );
        }

        validate_block_parts(block_number, &block, &receipts, &traces)
            .wrap_err_with(|| format!("validating synthetic block {block_number}"))?;
        synthetic
            .archive_block(block.clone(), policy)
            .await
            .wrap_err_with(|| format!("writing synthetic block {block_number}"))?;
        synthetic
            .archive_receipts(receipts, block_number, policy)
            .await
            .wrap_err_with(|| format!("writing synthetic receipts {block_number}"))?;
        synthetic
            .archive_traces(traces, block_number, policy)
            .await
            .wrap_err_with(|| format!("writing synthetic traces {block_number}"))?;
        synthetic
            .update_latest(block_number, LatestKind::Uploaded)
            .await
            .wrap_err_with(|| format!("updating synthetic latest to {block_number}"))?;

        parent_hash = block.header.hash_slow();
        info!(
            block_number,
            tx_count,
            hash = %parent_hash,
            "wrote synthetic block"
        );

        let readback = synthetic
            .get_block_data_with_offsets(block_number)
            .await
            .wrap_err_with(|| format!("readback synthetic block {block_number}"))?;
        validate_block_parts(
            block_number,
            &readback.block,
            &readback.receipts,
            &readback.traces,
        )
        .wrap_err_with(|| format!("validating synthetic readback {block_number}"))?;
    }

    Ok(())
}

fn build_synthetic_block(
    block_number: u64,
    parent_hash: B256,
    selected: Vec<TxSample>,
    signer: &PrivateKeySigner,
    next_nonce: &mut u64,
) -> Result<(Block, BlockReceipts, BlockTraces)> {
    let mut transactions = Vec::with_capacity(selected.len());
    let mut receipts = Vec::with_capacity(selected.len());
    let mut traces = Vec::with_capacity(selected.len());
    let mut cumulative_gas = 0_u64;
    let mut starting_log_index = 0_u64;
    let mut logs_bloom = Bloom::default();

    for sample in selected {
        let tx = synthetic_tx_from_sample(&sample.tx, signer, *next_nonce)
            .wrap_err("building synthetic tx")?;
        *next_nonce = next_nonce.saturating_add(1);

        let mut receipt = sample.receipt.clone();
        cumulative_gas = cumulative_gas.saturating_add(sample.gas_used);
        rewrite_receipt(&mut receipt.receipt, cumulative_gas);
        receipt.starting_log_index = starting_log_index;
        starting_log_index = starting_log_index.saturating_add(receipt.receipt.logs().len() as u64);
        for log in receipt.receipt.logs() {
            logs_bloom.accrue_log(log);
        }

        transactions.push(tx);
        receipts.push(receipt);
        traces.push(sample.trace);
    }

    let mut block = Block {
        header: monad_archive::prelude::Header {
            number: block_number,
            parent_hash,
            timestamp: 1_700_000_000 + block_number,
            gas_used: cumulative_gas,
            logs_bloom,
            ..Default::default()
        },
        body: monad_archive::prelude::BlockBody {
            transactions,
            ommers: vec![],
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
        },
    };

    let root_txs = block
        .body
        .transactions
        .iter()
        .map(|tx| tx.tx.clone())
        .collect::<Vec<_>>();
    let root_receipts = receipts
        .iter()
        .map(|receipt| receipt.receipt.clone())
        .collect::<Vec<_>>();
    block.header.transactions_root = calculate_transaction_root(&root_txs);
    block.header.receipts_root = calculate_receipt_root(&root_receipts);

    Ok((block, receipts, traces))
}

fn synthetic_tx_from_sample(
    sample: &TxEnvelopeWithSender,
    signer: &PrivateKeySigner,
    nonce: u64,
) -> Result<TxEnvelopeWithSender> {
    let source = &sample.tx;
    let tx = TxEip1559 {
        chain_id: source.chain_id().unwrap_or(10_143),
        nonce,
        gas_limit: source.gas_limit(),
        max_fee_per_gas: source.max_fee_per_gas(),
        max_priority_fee_per_gas: source
            .max_priority_fee_per_gas()
            .unwrap_or_else(|| source.priority_fee_or_price()),
        to: source.kind(),
        value: source.value(),
        access_list: source.access_list().cloned().unwrap_or_default(),
        input: source.input().clone(),
    };
    let signature = signer.sign_hash_sync(&tx.signature_hash())?;
    let tx = tx.into_signed(signature).into();

    Ok(TxEnvelopeWithSender {
        tx,
        sender: signer.address(),
    })
}

fn rewrite_receipt(receipt: &mut ReceiptEnvelope, cumulative_gas_used: u64) {
    match receipt {
        ReceiptEnvelope::Legacy(inner)
        | ReceiptEnvelope::Eip2930(inner)
        | ReceiptEnvelope::Eip1559(inner)
        | ReceiptEnvelope::Eip4844(inner)
        | ReceiptEnvelope::Eip7702(inner) => {
            inner.receipt.cumulative_gas_used = cumulative_gas_used;
            inner.logs_bloom = inner.receipt.bloom_slow();
        }
    }
}

fn validate_block_parts(
    block_number: u64,
    block: &Block,
    receipts: &BlockReceipts,
    traces: &BlockTraces,
) -> Result<()> {
    ensure!(
        block.header.number == block_number,
        "block number mismatch: expected {block_number}, got {}",
        block.header.number
    );
    ensure!(
        block.body.transactions.len() == receipts.len(),
        "tx count {} != receipt count {}",
        block.body.transactions.len(),
        receipts.len()
    );
    ensure!(
        block.body.transactions.len() == traces.len(),
        "tx count {} != trace count {}",
        block.body.transactions.len(),
        traces.len()
    );

    let txs = block
        .body
        .transactions
        .iter()
        .map(|tx| tx.tx.clone())
        .collect::<Vec<_>>();
    let receipt_envelopes = receipts
        .iter()
        .map(|receipt| receipt.receipt.clone())
        .collect::<Vec<_>>();
    ensure!(
        block.header.transactions_root == calculate_transaction_root(&txs),
        "invalid transactions root"
    );
    ensure!(
        block.header.receipts_root == calculate_receipt_root(&receipt_envelopes),
        "invalid receipts root"
    );

    let mut previous_gas = 0_u64;
    let mut next_log_index = 0_u64;
    let mut logs_bloom = Bloom::default();
    for receipt in receipts {
        ensure!(
            receipt.receipt.cumulative_gas_used() >= previous_gas,
            "receipt cumulative gas decreased"
        );
        ensure!(
            receipt.starting_log_index == next_log_index,
            "receipt starting_log_index mismatch"
        );
        previous_gas = receipt.receipt.cumulative_gas_used();
        next_log_index += receipt.receipt.logs().len() as u64;
        for log in receipt.receipt.logs() {
            logs_bloom.accrue_log(log);
        }
    }
    ensure!(
        block.header.gas_used == previous_gas,
        "header gas_used {} != last receipt cumulative gas {}",
        block.header.gas_used,
        previous_gas
    );
    ensure!(block.header.logs_bloom == logs_bloom, "invalid logs bloom");

    for (idx, trace) in traces.iter().enumerate() {
        decode_trace(trace).wrap_err_with(|| format!("invalid trace at tx index {idx}"))?;
    }

    Ok(())
}

fn synthetic_signer(seed: u64) -> Result<PrivateKeySigner> {
    let key = U256::from(seed.max(1));
    PrivateKeySigner::from_bytes(&B256::from(key)).map_err(Into::into)
}
