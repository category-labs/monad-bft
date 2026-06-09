// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

#![allow(async_fn_in_trait)]

use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::Bytes;
use eyre::{bail, eyre, Result};
use monad_archive::{
    archive_reader::LatestKind,
    model::{
        block_data_archive::{
            decode_trace, Block, BlockReceipts, BlockTraces, CallFrame, CallKind as ArchiveCallKind,
        },
        BlockDataReader, BlockDataReaderErased,
    },
};
use monad_chain_data::{
    compute_trace_addresses, run_configured_chain_data_engine_ingest, CallKind,
    ChainDataIngestSource, FinalizedBlock, IngestTrace, IngestTx,
};
use tracing::warn;

use super::cli::ArchiverChainDataIngestConfig;

/// Fetch retry policy: the producer now keeps many block fetches in flight at
/// once, so transient upstream blips (S3 5xx / throttling / connection resets)
/// are far likelier. A failed fetch would otherwise abort the whole ingest
/// pipeline and bounce the write lease. Retrying here — inside the source impl —
/// keeps the engine transport-agnostic (see `TODO(fetch-retry)` in ingest_core).
const FETCH_RETRY_ATTEMPTS: u32 = 8;
const FETCH_RETRY_BASE_MS: u64 = 50;
const FETCH_RETRY_MAX_DELAY_MS: u64 = 5_000;

#[derive(Clone)]
struct ArchiverChainDataSource {
    reader: BlockDataReaderErased,
}

impl ArchiverChainDataSource {
    /// One attempt: the three per-block objects (block, receipts, traces) fetched
    /// concurrently. Any one failing fails the attempt.
    async fn fetch_block_once(&self, block_number: u64) -> Result<FinalizedBlock> {
        let (block, receipts, traces) = tokio::try_join!(
            self.reader.get_block_by_number(block_number),
            self.reader.get_block_receipts(block_number),
            async {
                self.reader
                    .try_get_block_traces(block_number)
                    .await
                    .map(|traces| traces.unwrap_or_default())
            },
        )?;
        into_finalized_block(block, receipts, traces)
    }
}

impl ChainDataIngestSource for ArchiverChainDataSource {
    async fn get_latest_uploaded(&self) -> Result<Option<u64>> {
        self.reader.get_latest(LatestKind::Uploaded).await
    }

    async fn fetch_finalized_block(&self, block_number: u64) -> Result<FinalizedBlock> {
        let mut delay_ms = FETCH_RETRY_BASE_MS;
        for attempt in 1..=FETCH_RETRY_ATTEMPTS {
            match self.fetch_block_once(block_number).await {
                Ok(block) => return Ok(block),
                Err(e) if attempt < FETCH_RETRY_ATTEMPTS => {
                    // Full jitter on [0, delay): decorrelates retries across the
                    // many in-flight fetches so they don't thundering-herd S3.
                    let jitter = rand::random::<u64>() % delay_ms.max(1);
                    let sleep_ms = delay_ms.saturating_add(jitter);
                    warn!(
                        block = block_number,
                        attempt,
                        sleep_ms,
                        error = %e,
                        "chain-data block fetch failed; retrying with backoff"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                    delay_ms = (delay_ms * 2).min(FETCH_RETRY_MAX_DELAY_MS);
                }
                Err(e) => {
                    return Err(e.wrap_err(format!(
                        "chain-data block {block_number} fetch failed after \
                         {FETCH_RETRY_ATTEMPTS} attempts"
                    )));
                }
            }
        }
        unreachable!("loop returns on the final attempt")
    }
}

pub async fn chain_data_ingest_worker(
    block_data_source: BlockDataReaderErased,
    config: ArchiverChainDataIngestConfig,
) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    config.store.validate()?;
    config.engine.validate()?;
    run_configured_chain_data_engine_ingest(
        config.store,
        config.engine,
        ArchiverChainDataSource {
            reader: block_data_source,
        },
    )
    .await
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
        warn!(
            block = block_number,
            receipts = receipts.len(),
            "block has receipts but no transactions; ignoring receipts"
        );
    }

    let tx_statuses: Vec<bool> = receipts.iter().map(|r| r.receipt.status()).collect();
    let logs_by_tx = receipts
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
        let tx_status = *tx_statuses
            .get(tx_idx)
            .ok_or_else(|| eyre!("block {block_number} tx {tx_idx}: missing receipt"))?;
        let tx_index =
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
                tx_index,
                trace_address,
                tx_status,
            });
        }
    }
    Ok(out)
}
