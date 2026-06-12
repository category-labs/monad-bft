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

//! The chain-data ingest source over this archive (the "BDR"): the canonical
//! conversion from archive block/receipt/trace types into chain-data's
//! [`FinalizedBlock`], in two payload modes:
//!
//! - **native**: chain-data re-encodes rows into its own pack blobs; the
//!   source only decodes.
//! - **external**: chain-data indexes the EXISTING archive objects; the
//!   source additionally scans each raw object's per-tx RLP item ranges
//!   (sentinel/version prefix included) and attaches an
//!   [`ExternalPayloadSpec`] so per-row locators point straight into them.

use std::{sync::Arc, time::Duration};

use alloy_eips::eip2718::Encodable2718;
use alloy_rlp::Decodable;
use monad_chain_data::{
    ChainDataIngestSource, ExternalFamilyRegion, ExternalPayloadSpec, FinalizedBlock, IngestTrace,
    IngestTx,
};

use crate::{
    failover_circuit_breaker::{CircuitBreaker, FallbackExecutor},
    model::{
        block_data_archive::{
            decode_trace, Block, BlockDataArchive, BlockReceipts, BlockStorageRepr, BlockTraces,
            CallFrame, ReceiptStorageRepr,
        },
        RangeRlp,
    },
    prelude::*,
    rlp_offset_scanner::get_all_tx_offsets,
};

/// Fetch retry policy: the engine's producer keeps many block fetches in
/// flight at once, so transient upstream blips (S3 5xx / throttling /
/// connection resets) are likely. A failed fetch would otherwise abort the
/// whole ingest pipeline; retrying inside the source keeps the engine
/// transport-agnostic. The retry wraps the failover executor (retry
/// outermost), so each attempt independently tries primary-then-fallback.
const FETCH_RETRY_ATTEMPTS: u32 = 8;
const FETCH_RETRY_BASE_MS: u64 = 50;
const FETCH_RETRY_MAX_DELAY_MS: u64 = 5_000;

/// Failover circuit-breaker policy, matching `ArchiveReader::with_fallback`'s
/// defaults: open after 100 consecutive primary failures, retry the primary
/// after 5 minutes.
const FAILOVER_FAILURE_THRESHOLD: u32 = 100;
const FAILOVER_RECOVERY_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Clone)]
enum SourceMode {
    Native(Arc<FallbackExecutor<BlockDataReaderErased>>),
    /// External payloads require archive-backed readers: the spec is
    /// built by scanning the RAW stored objects, which only
    /// [`BlockDataArchive`] exposes.
    External(Arc<FallbackExecutor<BlockDataArchive>>),
}

/// [`ChainDataIngestSource`] over the archive.
#[derive(Clone)]
pub struct ArchiverChainDataSource {
    mode: SourceMode,
}

fn failover<P>(primary: P, fallback: Option<P>) -> Arc<FallbackExecutor<P>> {
    Arc::new(FallbackExecutor::new(
        primary,
        fallback,
        CircuitBreaker::new(FAILOVER_FAILURE_THRESHOLD, FAILOVER_RECOVERY_TIMEOUT),
    ))
}

fn require_archive(reader: BlockDataReaderErased, what: &str) -> Result<BlockDataArchive> {
    match reader {
        BlockDataReaderErased::BlockDataArchive(archive) => Ok(archive),
        _ => bail!("external payload indexing requires an archive {what}"),
    }
}

impl ArchiverChainDataSource {
    /// Native-payload source over any block-data reader (archive or triedb),
    /// with an optional fallback reader served behind a circuit breaker on
    /// primary failures.
    pub fn native(reader: BlockDataReaderErased, fallback: Option<BlockDataReaderErased>) -> Self {
        Self {
            mode: SourceMode::Native(failover(reader, fallback)),
        }
    }

    /// External-payload source; errors unless `reader` — and `fallback`, when
    /// given — is archive-backed.
    ///
    /// Byte-identity assumption: external-mode locators index into the archive
    /// bucket the chain-data store's `[store.archive]` points at
    /// (operationally the primary). A fallback-served block therefore assumes
    /// the archive replicas hold byte-identical objects for that block —
    /// guaranteed by the deterministic V1 object encoding and enforced by the
    /// archive checker's replica-agreement checks. Object EXISTENCE cannot
    /// skew past replication faults either: the tip never advances beyond the
    /// PRIMARY's `latest` marker (see `get_latest_uploaded`), below which the
    /// primary wrote all three objects before updating the marker, so
    /// failover only ever covers transient errors and repaired faults.
    pub fn external(
        reader: BlockDataReaderErased,
        fallback: Option<BlockDataReaderErased>,
    ) -> Result<Self> {
        let primary = require_archive(reader, "block data source")?;
        let fallback = fallback
            .map(|reader| require_archive(reader, "fallback block data source"))
            .transpose()?;
        Ok(Self {
            mode: SourceMode::External(failover(primary, fallback)),
        })
    }

    /// One attempt: the whole per-block fetch (and, externally, the offset
    /// scan + spec build) runs against ONE replica via the failover executor;
    /// any error — including a missing object, which a not-yet-archived block
    /// near the tip can legitimately produce on either replica — fails the
    /// attempt over to the fallback. That is deliberate and simple: the
    /// producer only requests blocks at/below `get_latest_uploaded` (which
    /// fails over identically), and the outer retry re-runs the whole
    /// primary-then-fallback sequence, so a transiently missing object is
    /// retried rather than masked.
    async fn fetch_block_once(&self, block_number: u64) -> Result<FinalizedBlock> {
        match &self.mode {
            SourceMode::Native(executor) => {
                // Strict traces first, so a fallback can cover a primary
                // trace gap; missing traces EVERYWHERE then degrade to empty
                // traces (the archiver's pre-fallback tolerance) via the
                // lenient pass.
                match executor
                    .execute(|reader| fetch_native_block(reader, block_number, true))
                    .await
                {
                    Ok(block) => Ok(block),
                    Err(_) => {
                        executor
                            .execute(|reader| fetch_native_block(reader, block_number, false))
                            .await
                    }
                }
            }
            SourceMode::External(executor) => {
                executor
                    .execute(|archive| fetch_external_block(archive, block_number))
                    .await
            }
        }
    }
}

impl ChainDataIngestSource for ArchiverChainDataSource {
    async fn get_latest_uploaded(&self) -> Result<Option<u64>> {
        match &self.mode {
            SourceMode::Native(executor) => {
                executor
                    .execute(|reader| reader.get_latest(LatestKind::Uploaded))
                    .await
            }
            // The primary's `latest` marker is AUTHORITATIVE in external mode:
            // readers resolve published locators against `[store.archive]`
            // (operationally the primary bucket), so following a fallback's
            // marker would publish rows the readers' bucket does not hold.
            // Fetch failover below the primary tip stays; a dead primary
            // stalls tip ADVANCE, which is the safe direction.
            SourceMode::External(executor) => {
                executor.primary.get_latest(LatestKind::Uploaded).await
            }
        }
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

/// Native-mode fetch: the three per-block objects fetched concurrently from
/// one reader. Any one failing fails the attempt. `strict_traces` makes an
/// ABSENT traces object an error too, so the failover executor consults the
/// fallback before the block is indexed with zero traces forever (chain-data
/// has no re-ingest); the lenient pass restores the historic tolerance.
async fn fetch_native_block(
    reader: &BlockDataReaderErased,
    block_number: u64,
    strict_traces: bool,
) -> Result<FinalizedBlock> {
    let (block, receipts, traces) = try_join!(
        reader.get_block_by_number(block_number),
        reader.get_block_receipts(block_number),
        async {
            match reader.try_get_block_traces(block_number).await? {
                Some(traces) => Ok(traces),
                None if strict_traces => Err(eyre::eyre!(
                    "traces object missing for block {block_number}"
                )),
                None => Ok(Default::default()),
            }
        },
    )?;
    into_finalized_block(block, receipts, traces)
}

/// External-mode fetch: raw objects (offset scan needs the exact stored
/// bytes), then the same decode path as native plus the attached spec.
/// Per-block scan failures are hard errors — a wrong locator would serve
/// another tx's bytes.
async fn fetch_external_block(
    archive: &BlockDataArchive,
    block_number: u64,
) -> Result<FinalizedBlock> {
    let block_key = archive.block_key(block_number);
    let receipts_key = archive.receipts_key(block_number);
    let traces_key = archive.traces_key(block_number);
    let (block_raw, receipts_raw, traces_raw) = try_join!(
        archive.store.get(&block_key),
        archive.store.get(&receipts_key),
        archive.store.get(&traces_key),
    )
    .wrap_err("Error getting block data")?;
    let block_raw = block_raw.wrap_err("No block found")?;
    let receipts_raw = receipts_raw.wrap_err("No receipt found")?;
    let traces_raw = traces_raw.wrap_err("No trace found")?;

    let offsets = scan_archive_object_offsets(&block_raw, &receipts_raw, &traces_raw)
        .wrap_err_with(|| format!("block {block_number}: archive offset scan failed"))?;

    let block = BlockStorageRepr::decode_and_convert(&block_raw)
        .await
        .wrap_err("Failed to decode block")?;
    let receipts = ReceiptStorageRepr::decode_and_convert(&receipts_raw)
        .wrap_err("Failed to decode receipts")?;
    let traces: BlockTraces =
        Vec::decode(&mut traces_raw.as_ref()).wrap_err("Failed to decode traces")?;

    let tx_statuses: Vec<bool> = receipts.iter().map(|r| r.receipt.status()).collect();
    let mut finalized = into_finalized_block(block, receipts, traces)?;
    finalized.external = Some(build_external_spec(
        block_number,
        (&block_key, &receipts_key, &traces_key),
        &offsets,
        &finalized,
        &tx_statuses,
    )?);
    Ok(finalized)
}

/// Per-tx RLP item ranges of the three RAW archive objects, absolute within
/// each stored object (the scanner works on the sentinel-stripped list
/// payloads; the prefixes are added back here). Pre-sender (V0) framings are
/// rejected: their tx/receipt items decode to different types than the
/// external mirrors expect.
pub fn scan_archive_object_offsets(
    block_raw: &[u8],
    receipts_raw: &[u8],
    traces_raw: &[u8],
) -> Result<Vec<TxByteOffsets>> {
    let block_prefix = block_items_offset(block_raw)?;
    let receipts_prefix = receipts_items_offset(receipts_raw)?;
    let offsets = get_all_tx_offsets(
        &block_raw[block_prefix..],
        &receipts_raw[receipts_prefix..],
        traces_raw,
    )?;
    Ok(offsets
        .into_iter()
        .map(|TxByteOffsets { tx, receipt, trace }| TxByteOffsets {
            tx: shift(tx, block_prefix),
            receipt: shift(receipt, receipts_prefix),
            trace,
        })
        .collect())
}

fn shift(range: RangeRlp, by: usize) -> RangeRlp {
    RangeRlp {
        start: range.start + by,
        end: range.end + by,
    }
}

/// Byte offset of the block object's RLP payload (`[55][version]` framing for
/// V1/V2). V0 objects (with or without the sentinel) carry `TxEnvelope` items
/// without senders — unsupported for external indexing.
fn block_items_offset(block_raw: &[u8]) -> Result<usize> {
    match block_raw {
        [55, 1, ..] | [55, 2, ..] => Ok(2),
        [55, 0, ..] => bail!("V0 archive block (no sender) is unsupported for external indexing"),
        _ => bail!("unversioned legacy archive block is unsupported for external indexing"),
    }
}

/// Byte offset of the receipts object's RLP payload (`[88][version]`). A bare
/// empty list (legacy empty-receipts framing) is accepted as zero items.
fn receipts_items_offset(receipts_raw: &[u8]) -> Result<usize> {
    match receipts_raw {
        [88, 1, ..] => Ok(2),
        [88, 0, ..] => {
            bail!("V0 archive receipts are unsupported for external indexing")
        }
        [alloy_rlp::EMPTY_LIST_CODE] => Ok(0),
        _ => bail!("unversioned legacy archive receipts are unsupported for external indexing"),
    }
}

/// Builds the per-block [`ExternalPayloadSpec`] from the scanned item ranges
/// and the decoded block: container partitions per family, cumulative row
/// manifests (logs per receipt, DFS frames per tx), and the per-tx status
/// bitvec. chain-data re-validates all of it against the block at ingest.
fn build_external_spec(
    block_number: u64,
    (block_key, receipts_key, traces_key): (&str, &str, &str),
    offsets: &[TxByteOffsets],
    finalized: &FinalizedBlock,
    tx_statuses: &[bool],
) -> Result<ExternalPayloadSpec> {
    let txs = family_region(
        block_key,
        offsets.iter().map(|o| &o.tx),
        Vec::new(),
        Vec::new(),
    )?;

    let log_rows = cumulative(finalized.logs_by_tx.iter().map(Vec::len))?;
    let logs = family_region(
        receipts_key,
        offsets.iter().map(|o| &o.receipt),
        log_rows,
        Vec::new(),
    )?;

    let mut frames_per_tx = vec![0usize; offsets.len()];
    for trace in &finalized.traces {
        let slot = frames_per_tx
            .get_mut(trace.tx_index as usize)
            .ok_or_eyre("trace tx_index out of range")?;
        *slot += 1;
    }
    let trace_rows = cumulative(frames_per_tx.into_iter())?;
    let traces = family_region(
        traces_key,
        offsets.iter().map(|o| &o.trace),
        trace_rows,
        status_bits(tx_statuses),
    )?;

    Ok(ExternalPayloadSpec {
        block_number,
        logs,
        txs,
        traces,
    })
}

/// One family's region: asserts the scanned item ranges are CONTIGUOUS
/// (each item's end is the next item's start — they partition the list
/// payload) and collapses them into the base-relative offset partition,
/// guarding the u32 offset space.
fn family_region<'a>(
    key: &str,
    ranges: impl Iterator<Item = &'a RangeRlp>,
    container_rows: Vec<u32>,
    container_status: Vec<u8>,
) -> Result<ExternalFamilyRegion> {
    let mut base: Option<usize> = None;
    let mut container_offsets: Vec<u32> = vec![0];
    let mut expected_start = 0usize;
    for range in ranges {
        let base = *base.get_or_insert(range.start);
        if range.start != expected_start.max(base) || range.end <= range.start {
            bail!("archive object items are not contiguous");
        }
        expected_start = range.end;
        container_offsets.push(
            u32::try_from(range.end - base).wrap_err("archive object exceeds u32 offset space")?,
        );
    }
    let base_offset =
        u32::try_from(base.unwrap_or(0)).wrap_err("archive object exceeds u32 offset space")?;
    Ok(ExternalFamilyRegion {
        key: key.as_bytes().to_vec(),
        base_offset,
        container_offsets,
        container_rows,
        container_status,
    })
}

/// Cumulative row counts (`container_rows` manifest shape).
fn cumulative(counts: impl Iterator<Item = usize>) -> Result<Vec<u32>> {
    let mut total = 0u64;
    counts
        .map(|count| {
            total += count as u64;
            u32::try_from(total).wrap_err("row count exceeds u32")
        })
        .collect()
}

/// LSB-first per-tx status bitvec (chain-data's `container_status` layout).
fn status_bits(statuses: &[bool]) -> Vec<u8> {
    let mut bits = vec![0u8; statuses.len().div_ceil(8)];
    for (i, &status) in statuses.iter().enumerate() {
        if status {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    bits
}

/// THE canonical conversion from archive types to chain-data's ingest block
/// (`external` is attached separately by the external fetch path).
pub fn into_finalized_block(
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
            signed_tx_bytes: alloy_primitives::Bytes::from(tx_w.tx.encoded_2718()),
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
        external: None,
    })
}

fn map_call_kind(typ: &CallKind) -> monad_chain_data::CallKind {
    match typ {
        CallKind::Call => monad_chain_data::CallKind::Call,
        CallKind::DelegateCall => monad_chain_data::CallKind::DelegateCall,
        CallKind::CallCode => monad_chain_data::CallKind::CallCode,
        CallKind::Create => monad_chain_data::CallKind::Create,
        CallKind::Create2 => monad_chain_data::CallKind::Create2,
        CallKind::SelfDestruct => monad_chain_data::CallKind::SelfDestruct,
        CallKind::StaticCall => monad_chain_data::CallKind::StaticCall,
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
        let trace_addresses = monad_chain_data::compute_trace_addresses(depths)
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

#[cfg(test)]
mod tests {
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
    use alloy_primitives::{Address, Signature, TxKind};
    use alloy_rlp::Encodable;
    use monad_eth_testutil::make_receipt;
    use monad_eth_types::{ReceiptWithLogIndex, TxEnvelopeWithSender};

    use super::*;

    fn signed_tx(nonce: u64) -> TxEnvelopeWithSender {
        let tx = TxEip1559 {
            chain_id: 1,
            nonce,
            gas_limit: 30_000,
            max_fee_per_gas: 1_000,
            max_priority_fee_per_gas: 10,
            to: TxKind::Call(Address::repeat_byte(0x42)),
            value: U256::from(nonce),
            ..Default::default()
        };
        let signed = tx.into_signed(Signature::test_signature());
        let tx = TxEnvelope::from(signed);
        TxEnvelopeWithSender {
            sender: Address::repeat_byte(0x11),
            tx,
        }
    }

    fn encoded_objects(tx_count: usize) -> (Vec<u8>, Vec<u8>, Vec<u8>, Block, BlockReceipts) {
        let txs: Vec<TxEnvelopeWithSender> = (0..tx_count as u64).map(signed_tx).collect();
        let block = Block {
            header: Header {
                number: 7,
                ..Default::default()
            },
            body: BlockBody {
                transactions: txs,
                ommers: vec![],
                withdrawals: None,
            },
        };
        let receipts: BlockReceipts = (0..tx_count)
            .map(|i| ReceiptWithLogIndex {
                receipt: ReceiptEnvelope::Eip1559(make_receipt((i + 1) * 100)),
                starting_log_index: 0,
            })
            .collect();
        let traces: BlockTraces = (0..tx_count)
            .map(|i| {
                // Inner content is opaque to the scanner; realistic frames are
                // exercised by the round-trip test.
                let mut blob = Vec::new();
                vec![vec![i as u8; 3]].encode(&mut blob);
                blob
            })
            .collect();

        let block_raw = BlockStorageRepr::V1(block.clone()).encode().unwrap();
        let receipts_raw = ReceiptStorageRepr::V1(receipts.clone()).encode().unwrap();
        let mut traces_raw = Vec::new();
        traces.encode(&mut traces_raw);
        (block_raw, receipts_raw, traces_raw, block, receipts)
    }

    /// The contiguity + decodability contract `family_region` relies on:
    /// scanned item ranges (made absolute within the RAW objects, sentinel
    /// prefix included) partition each list payload with no gaps, and slicing
    /// the raw object at each range yields exactly one decodable item,
    /// byte-identical to encoding that item alone.
    #[test]
    fn scanned_offsets_are_contiguous_and_slice_to_decodable_items() {
        let (block_raw, receipts_raw, traces_raw, block, receipts) = encoded_objects(3);
        let offsets = scan_archive_object_offsets(&block_raw, &receipts_raw, &traces_raw).unwrap();
        assert_eq!(offsets.len(), 3);

        for window in offsets.windows(2) {
            assert_eq!(window[0].tx.end, window[1].tx.start, "tx items contiguous");
            assert_eq!(
                window[0].receipt.end, window[1].receipt.start,
                "receipt items contiguous"
            );
            assert_eq!(
                window[0].trace.end, window[1].trace.start,
                "trace items contiguous"
            );
        }
        // The receipts/traces objects are a single list: the last item runs
        // to the end of the object. The block object's tx list is trailed by
        // the ommers/withdrawals fields, so only an upper bound holds there.
        assert_eq!(offsets.last().unwrap().receipt.end, receipts_raw.len());
        assert_eq!(offsets.last().unwrap().trace.end, traces_raw.len());
        assert!(offsets.last().unwrap().tx.end < block_raw.len());

        for (i, offset) in offsets.iter().enumerate() {
            let tx_slice = &block_raw[offset.tx.start..offset.tx.end];
            let decoded = TxEnvelopeWithSender::decode(&mut &*tx_slice).unwrap();
            assert_eq!(decoded, block.body.transactions[i]);
            let mut reencoded = Vec::new();
            block.body.transactions[i].encode(&mut reencoded);
            assert_eq!(tx_slice, reencoded.as_slice(), "exact item bytes");

            let receipt_slice = &receipts_raw[offset.receipt.start..offset.receipt.end];
            let decoded = ReceiptWithLogIndex::decode(&mut &*receipt_slice).unwrap();
            assert_eq!(decoded, receipts[i]);

            let trace_slice = &traces_raw[offset.trace.start..offset.trace.end];
            let decoded = Vec::<u8>::decode(&mut &*trace_slice).unwrap();
            let mut expected_blob = Vec::new();
            vec![vec![i as u8; 3]].encode(&mut expected_blob);
            assert_eq!(decoded, expected_blob);
        }
    }

    #[test]
    fn empty_block_scans_to_zero_items() {
        let (block_raw, receipts_raw, traces_raw, ..) = encoded_objects(0);
        let offsets = scan_archive_object_offsets(&block_raw, &receipts_raw, &traces_raw).unwrap();
        assert!(offsets.is_empty());
    }

    use crate::kvstore::{memory::MemoryStorage, WritePolicy};

    /// An archive holding one `tx_count`-tx block `number` (per-tx trace
    /// containers that decode to zero frames) with the uploaded marker at
    /// `number`.
    async fn populated_archive(number: u64, tx_count: usize) -> BlockDataArchive {
        let traces: BlockTraces = (0..tx_count)
            .map(|_| {
                let mut blob = Vec::new();
                Vec::<Vec<CallFrame>>::new().encode(&mut blob);
                blob
            })
            .collect();
        build_archive(number, tx_count, Some(traces)).await
    }

    /// Like [`populated_archive`] but each tx carries one real trace frame,
    /// so a fallback-served trace fetch is observable in the result.
    async fn traced_archive(number: u64, tx_count: usize) -> BlockDataArchive {
        let frame = CallFrame {
            typ: CallKind::Call,
            flags: U64::ZERO,
            from: Address::ZERO,
            to: Some(Address::ZERO),
            value: U256::ZERO,
            gas: U64::from(21_000u64),
            gas_used: U64::from(100u64),
            input: alloy_primitives::Bytes::new(),
            output: alloy_primitives::Bytes::new(),
            status: alloy_primitives::U8::from(1u8),
            depth: U64::ZERO,
            logs: None,
        };
        let traces: BlockTraces = (0..tx_count)
            .map(|_| {
                let mut blob = Vec::new();
                vec![vec![frame.clone()]].encode(&mut blob);
                blob
            })
            .collect();
        build_archive(number, tx_count, Some(traces)).await
    }

    /// Block + receipts archived but NO traces object (a replica trace gap).
    async fn traceless_archive(number: u64, tx_count: usize) -> BlockDataArchive {
        build_archive(number, tx_count, None).await
    }

    async fn build_archive(
        number: u64,
        tx_count: usize,
        traces: Option<BlockTraces>,
    ) -> BlockDataArchive {
        let archive = BlockDataArchive::new(MemoryStorage::new("healthy-fallback"));
        let txs: Vec<TxEnvelopeWithSender> = (0..tx_count as u64).map(signed_tx).collect();
        let block = Block {
            header: Header {
                number,
                ..Default::default()
            },
            body: BlockBody {
                transactions: txs,
                ommers: vec![],
                withdrawals: None,
            },
        };
        let receipts: BlockReceipts = (0..tx_count)
            .map(|i| ReceiptWithLogIndex {
                receipt: ReceiptEnvelope::Eip1559(make_receipt((i + 1) * 100)),
                starting_log_index: 0,
            })
            .collect();
        archive
            .archive_block(block, WritePolicy::NoClobber)
            .await
            .unwrap();
        archive
            .archive_receipts(receipts, number, WritePolicy::NoClobber)
            .await
            .unwrap();
        if let Some(traces) = traces {
            archive
                .archive_traces(traces, number, WritePolicy::NoClobber)
                .await
                .unwrap();
        }
        archive
            .update_latest(number, LatestKind::Uploaded)
            .await
            .unwrap();
        archive
    }

    /// An archive whose every storage op errors (`MemoryStorage` failure
    /// injection) — a hard-down primary.
    fn broken_archive() -> BlockDataArchive {
        let storage = MemoryStorage::new("broken-primary");
        storage
            .should_fail
            .store(true, std::sync::atomic::Ordering::SeqCst);
        BlockDataArchive::new(storage)
    }

    /// A hard-down primary fails the attempt over to the fallback for native
    /// (decoded) fetches.
    #[tokio::test]
    async fn native_fetch_fails_over_to_fallback() {
        let fallback = populated_archive(7, 2).await;
        let source =
            ArchiverChainDataSource::native(broken_archive().into(), Some(fallback.into()));
        let block = source.fetch_finalized_block(7).await.unwrap();
        assert_eq!(block.header.number, 7);
        assert_eq!(block.txs.len(), 2);
        assert!(block.external.is_none(), "native mode attaches no spec");
    }

    /// A hard-down primary fails the whole external fetch unit (raw GETs +
    /// offset scan + spec build) over to the fallback, which serves a complete
    /// spec under the shared archive key layout.
    #[tokio::test]
    async fn external_fetch_fails_over_with_spec() {
        let fallback = populated_archive(7, 2).await;
        let expected_key = fallback.block_key(7).into_bytes();
        let source =
            ArchiverChainDataSource::external(broken_archive().into(), Some(fallback.into()))
                .unwrap();
        let block = source.fetch_finalized_block(7).await.unwrap();
        assert_eq!(block.header.number, 7);
        assert_eq!(block.txs.len(), 2);
        let spec = block.external.expect("fallback-served external spec");
        assert_eq!(spec.block_number, 7);
        assert_eq!(spec.txs.key, expected_key);
        // One offset partition entry per tx plus the leading zero.
        assert_eq!(spec.txs.container_offsets.len(), 3);
    }

    /// A primary that is up but simply does not hold the object yet (missing
    /// = an error inside the fetch unit) also fails over.
    #[tokio::test]
    async fn missing_primary_object_fails_over() {
        let primary = BlockDataArchive::new(MemoryStorage::new("empty-primary"));
        let fallback = populated_archive(7, 1).await;
        let source =
            ArchiverChainDataSource::external(primary.into(), Some(fallback.into())).unwrap();
        let block = source.fetch_finalized_block(7).await.unwrap();
        assert_eq!(block.header.number, 7);
        assert!(block.external.is_some());
    }

    /// External tip authority: readers resolve locators against the primary
    /// bucket, so the tip must NOT advance past it — a broken primary stalls
    /// tip advance instead of following the fallback's marker.
    #[tokio::test]
    async fn external_get_latest_is_primary_authoritative() {
        let fallback = populated_archive(9, 1).await;
        let source =
            ArchiverChainDataSource::external(broken_archive().into(), Some(fallback.into()))
                .unwrap();
        assert!(source.get_latest_uploaded().await.is_err());
    }

    /// Native locators are chain-data-owned (no reader binding to the source
    /// bucket), so the tip may follow the fallback when the primary is down.
    #[tokio::test]
    async fn native_get_latest_fails_over() {
        let fallback = populated_archive(9, 1).await;
        let source =
            ArchiverChainDataSource::native(broken_archive().into(), Some(fallback.into()));
        assert_eq!(source.get_latest_uploaded().await.unwrap(), Some(9));
    }

    /// A primary trace gap is covered by the fallback (strict pass) instead
    /// of being silently indexed as zero traces; absent everywhere it
    /// degrades to empty traces (the archiver's historic tolerance).
    #[tokio::test]
    async fn native_trace_gap_fails_over_before_degrading() {
        let primary = traceless_archive(7, 1).await;
        let fallback = traced_archive(7, 1).await;
        let source = ArchiverChainDataSource::native(primary.into(), Some(fallback.into()));
        let block = source.fetch_finalized_block(7).await.unwrap();
        assert!(!block.traces.is_empty(), "fallback's traces must serve");

        let lenient = ArchiverChainDataSource::native(traceless_archive(7, 1).await.into(), None);
        let block = lenient.fetch_finalized_block(7).await.unwrap();
        assert!(block.traces.is_empty(), "no fallback: degrade to empty");
    }

    /// Without a fallback a primary failure surfaces (single attempt; the
    /// outer retry loop is exercised through `fetch_finalized_block`).
    #[tokio::test]
    async fn no_fallback_surfaces_primary_error() {
        let source = ArchiverChainDataSource::external(broken_archive().into(), None).unwrap();
        assert!(source.fetch_block_once(7).await.is_err());
        assert!(source.get_latest_uploaded().await.is_err());
    }

    #[test]
    fn v0_objects_are_rejected() {
        let (block_raw, receipts_raw, traces_raw, ..) = encoded_objects(1);
        let mut v0_block = block_raw.clone();
        v0_block[1] = 0;
        assert!(scan_archive_object_offsets(&v0_block, &receipts_raw, &traces_raw).is_err());
        let mut v0_receipts = receipts_raw.clone();
        v0_receipts[1] = 0;
        assert!(scan_archive_object_offsets(&block_raw, &v0_receipts, &traces_raw).is_err());
        // Legacy (no sentinel) framing is also rejected.
        assert!(scan_archive_object_offsets(&block_raw[2..], &receipts_raw, &traces_raw).is_err());
    }
}
