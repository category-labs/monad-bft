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

//! Data track: per-epoch codec resolution, concurrent row encoding
//! (`encode_pack_entry` on blocking tasks, drained in block order), and pack
//! flushing that advances the `data_durable` frontier.

use std::{future::Future, sync::Arc};

use bytes::Bytes;
use futures::{stream::FuturesOrdered, StreamExt};
use tokio::sync::mpsc;
use tracing::Instrument;

use super::{
    probe::{IngestProbe, TIMING_TARGET},
    publisher::Progress,
    task_join_err, AssignedBlock, DataMsg, IngestMsg,
};
use crate::{
    engine::{
        digest::{block_content_digest, chain, family_content_digest, ChainDigest},
        family::PerFamily,
        row_codec::{RowCodec, DICT_VERSION_NONE},
        tables::{DictConfig, Tables},
    },
    error::{MonadChainDataError, Result},
    external::ExternalFamilyRegion,
    ingest_types::Hash32,
    logs::{digest_block_logs, encode_block_logs, flatten_logs},
    primitives::{
        records::{
            BlockBlobHeader, BlockRecord, FamilyWindowRecord, PrimaryId, ENCODING_EXTERNAL_V1,
        },
        EvmBlockHeader,
    },
    store::{BlobStore, MetaStore},
    traces::{digest_block_traces, encode_block_traces},
    txs::{collect_hash_locations, digest_block_txs, encode_block_txs, TxLocation},
};

#[derive(Debug, Clone, Copy)]
pub struct PackConfig {
    pub target_bytes: usize,
    pub max_blocks: usize,
}

/// The data track's write shape: pack sizing plus where payloads live.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DataTrackConfig {
    pub pack: PackConfig,
    pub payload: PayloadMode,
}

/// Where a block's row payloads live.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PayloadMode {
    /// Rows are re-encoded into chain-data-owned pack blobs (everything
    /// before external indexing existed).
    #[default]
    Native,
    /// Rows stay in the monad-archive objects the source ingests FROM; ingest
    /// writes only metadata + indexes whose locators point into those objects
    /// (`ENCODING_EXTERNAL_V1` headers). Every fetched block must carry an
    /// [`crate::external::ExternalPayloadSpec`]. Dictionaries are never
    /// trained (containers are uncompressed), and `row_chain` digests stay
    /// byte-identical to a native ingest of the same blocks. Unlike native
    /// ingest (which tolerates absent trace objects as zero trace rows), a
    /// missing locator cannot be represented, so every block in the ingested
    /// range must have all three archive objects present in V1+ framing.
    ExternalArchive,
}

/// The per-epoch row codecs the data track needs to frame a block's rows.
pub type Codecs = PerFamily<RowCodec>;

/// Resolves the row codecs for an epoch (`version`). Injected rather than built
/// into the engine: making a codec ready can train a dictionary, which reads
/// back the prior epoch's blocks — a reader-level capability.
pub trait CodecResolver: Send + Sync + 'static {
    /// Ensure the codecs for `version` are present, TRAINING and durably
    /// persisting a dictionary if one does not yet exist, blocking until ready.
    /// Called once per epoch boundary; single-flight with [`Self::prewarm`].
    fn ensure(&self, version: u32) -> impl Future<Output = Result<Codecs>> + Send;

    /// Fire-and-forget hint that the corpus for `version` is durable, so its
    /// dictionary can be trained in the background ahead of the epoch boundary,
    /// making the eventual `ensure` a fast path. Default no-op.
    fn prewarm(&self, version: u32) {
        let _ = version;
    }
}

/// One block's fully-encoded persistence artifacts, held until the pack flushes
/// (the source block is long gone by flush time).
pub(crate) struct PackEntry {
    pub combined_blob: Vec<u8>,
    pub log_header: BlockBlobHeader,
    pub tx_header: BlockBlobHeader,
    pub trace_header: BlockBlobHeader,
    pub record: BlockRecord,
    pub evm_header: EvmBlockHeader,
    pub hash_locations: Vec<(Hash32, TxLocation)>,
    /// The block's standalone content digest (computed in the parallel encode
    /// task); the serial pop_encode drain folds it into the running chain and
    /// stamps `record.row_chain`.
    pub content_digest: ChainDigest,
}

/// Floor on one entry's contribution toward `PackConfig::target_bytes`
/// (roughly a block's metadata-row footprint): external-payload entries carry
/// no blob at all, so a pure blob-size accounting would batch only on
/// `max_blocks`. Native blobs are effectively always larger than this, so the
/// floor leaves native batching unchanged.
const PACK_ENTRY_MIN_ACCOUNTED_BYTES: usize = 1024;

struct BlobPacker {
    cfg: PackConfig,
    entries: Vec<PackEntry>,
    last_block: u64,
    bytes: usize,
}

impl BlobPacker {
    fn new(cfg: PackConfig) -> Self {
        Self {
            cfg,
            entries: Vec::new(),
            last_block: 0,
            bytes: 0,
        }
    }
    fn push(&mut self, number: u64, entry: PackEntry) {
        self.bytes += entry
            .combined_blob
            .len()
            .max(PACK_ENTRY_MIN_ACCOUNTED_BYTES);
        self.last_block = number;
        self.entries.push(entry);
    }
    fn should_flush(&self) -> bool {
        self.bytes >= self.cfg.target_bytes || self.entries.len() >= self.cfg.max_blocks
    }
    /// Take the held entries and the highest block they cover, resetting the
    /// packer.
    fn take(&mut self) -> Option<(Vec<PackEntry>, u64)> {
        if self.entries.is_empty() {
            return None;
        }
        let entries = std::mem::take(&mut self.entries);
        let last_block = self.last_block;
        self.bytes = 0;
        Some((entries, last_block))
    }
}

/// Data track: CPU-bound encodes run on blocking tasks (up to
/// `encode_concurrency`), drained into the packer in strict block order.
///
/// Every barrier — epoch codec change, `BatchFlush`, `Checkpoint`, end-of-stream
/// — first drains ALL in-flight encodes AND awaits all in-flight pack flushes,
/// so a barrier never claims durability for a block whose rows aren't written
/// yet. A mid-stream `should_flush` only flushes the already-drained contiguous
/// prefix and is pipelined at depth 1 ([`DataPipeline::start_flush`]): the
/// store write runs in the background while further encodes drain.
///
/// `chain_seed` is block `resume`'s stored `row_chain`
/// (`EMPTY_DIGEST` from genesis/cold); each drained block folds its content
/// digest into the chain at [`DataPipeline::pop_encode`], which is already
/// strict block order. Re-ingesting a block after a crash recomputes the same
/// content digest from the same logical block and the same prior chain value,
/// so the rewrite is idempotent — identical record bytes — under both the
/// backfill-checkpoint and live-rebuild recovery regimes.
pub(crate) async fn run_data_track<M, B, R>(
    mut rx: mpsc::Receiver<DataMsg>,
    tables: Arc<Tables<M, B>>,
    resolver: R,
    progress: Arc<Progress>,
    cfg: DataTrackConfig,
    probe: Arc<IngestProbe>,
    chain_seed: ChainDigest,
) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
    R: CodecResolver,
{
    let dict_config = *tables.dicts().config();
    let epoch_blocks = dict_config.epoch_blocks;
    debug_assert!(epoch_blocks > 0, "epoch_blocks must be positive");
    let DataTrackConfig { pack, payload } = cfg;
    let external = payload == PayloadMode::ExternalArchive;
    // Encode is pure CPU; cap parallelism at core count.
    let encode_concurrency = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1);
    let mut codecs: Option<(u32, Codecs)> = None;
    let mut last_prewarmed = 0u32;
    let mut pipeline = DataPipeline {
        inflight: EncodePipeline::new(),
        packer: BlobPacker::new(pack),
        chain_head: chain_seed,
        flushes: FlushPipeline::new(),
        tables,
        progress,
        probe,
    };
    loop {
        // Time blocked here = the data track is starved.
        let recv_start = pipeline.probe.start();
        let msg = rx.recv().await;
        pipeline
            .probe
            .record(&pipeline.probe.data_recv_blocked_ns, recv_start);
        let Some(msg) = msg else { break };
        match msg {
            IngestMsg::Block(b) => {
                // External payloads are never dict-compressed: every block
                // pins version 0 (plain), so the epoch barrier and training
                // below never fire.
                let version = if external {
                    DICT_VERSION_NONE
                } else {
                    u32::try_from(b.number / epoch_blocks)
                        .map_err(|_| MonadChainDataError::Decode("dict epoch/version overflow"))?
                };
                let block_codecs = match &codecs {
                    Some((v, c)) if *v == version => c.clone(),
                    _ => {
                        // Epoch barrier: prior-epoch encodes must be flushed
                        // durable before the resolver reads those blocks back
                        // for dict training, or the dict would be
                        // nondeterministic.
                        pipeline.flush_barrier().await?;
                        let resolved = resolver.ensure(version).await?;
                        debug_assert!(
                            resolved.log.version() == version
                                && resolved.tx.version() == version
                                && resolved.trace.version() == version,
                            "resolver returned codecs for the wrong epoch: requested {version}, \
                             got log={} tx={} trace={}",
                            resolved.log.version(),
                            resolved.tx.version(),
                            resolved.trace.version(),
                        );
                        codecs.insert((version, resolved)).1.clone()
                    }
                };
                let block = b.clone();
                let task_probe = pipeline.probe.clone();
                pipeline
                    .inflight
                    .push_back(tokio::task::spawn_blocking(move || {
                        let encode_start = task_probe.start();
                        let entry = match payload {
                            PayloadMode::Native => encode_pack_entry(&block, &block_codecs),
                            PayloadMode::ExternalArchive => encode_external_pack_entry(&block),
                        };
                        task_probe.record(&task_probe.data_encode_ns, encode_start);
                        entry
                    }));
                while pipeline.inflight.len() >= encode_concurrency {
                    pipeline.pop_encode().await?;
                    if pipeline.packer.should_flush() {
                        pipeline.start_flush().await?;
                    }
                }
            }
            // Flush so data_durable reaches the tip before publish — but only
            // after every in-flight encode is packed and written.
            IngestMsg::BatchFlush => {
                pipeline.flush_barrier().await?;
            }
            // Full barrier, then signal the index track that row data is
            // durable so it may persist the snapshot.
            IngestMsg::Checkpoint(done) => {
                pipeline.flush_barrier().await?;
                let _ = done.send(());
            }
        }
        // External mode never trains: a prewarm would background-train a
        // dictionary from external blocks (and read their archive containers
        // back as if they were zstd frames).
        if !external {
            maybe_prewarm(
                &resolver,
                pipeline.progress.data_durable(),
                &dict_config,
                &mut last_prewarmed,
            );
        }
    }
    pipeline.flush_barrier().await
}

/// In-flight concurrent row encodes (each resolves to one block's
/// [`PackEntry`]), drained strictly in block order.
type EncodePipeline = FuturesOrdered<tokio::task::JoinHandle<Result<PackEntry>>>;

/// In-flight background pack flushes (each resolves to the last block it made
/// durable). Held at depth <= 1 by [`DataPipeline::start_flush`].
type FlushPipeline = FuturesOrdered<tokio::task::JoinHandle<Result<u64>>>;

/// The data track's mutable pipeline state: in-flight encodes draining into the
/// packer (folding the checksum chain on the serial path), and the background
/// pack flush advancing `data_durable`.
struct DataPipeline<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    inflight: EncodePipeline,
    packer: BlobPacker,
    chain_head: ChainDigest,
    flushes: FlushPipeline,
    tables: Arc<Tables<M, B>>,
    progress: Arc<Progress>,
    probe: Arc<IngestProbe>,
}

impl<M, B> DataPipeline<M, B>
where
    M: MetaStore,
    B: BlobStore,
{
    /// Begin flushing the packed entries in the background, pipelined at depth 1:
    /// any prior flush is awaited FIRST, so at most one flush is in flight,
    /// flushes complete in submission order, and `data_durable` only advances on
    /// flush completion. The next pack's encode drain overlaps this pack's store
    /// write. A background flush error is surfaced at the next `start_flush` /
    /// `flush_barrier` (when its `drain_flushes` joins the task), not inline;
    /// `data_durable` never advances past a failed pack, so nothing incorrect
    /// publishes — the only cost is up to one pack of error-surfacing latency.
    async fn start_flush(&mut self) -> Result<()> {
        self.drain_flushes().await?;
        let Some((entries, last_block)) = self.packer.take() else {
            return Ok(());
        };
        let tables = self.tables.clone();
        let probe = self.probe.clone();
        self.flushes.push_back(tokio::spawn(async move {
            // One `with_writes` for the whole pack, so durability of the pack ==
            // durability of the head.
            let flush_start = probe.start();
            let span = tracing::trace_span!(target: TIMING_TARGET, "flush_pack", last_block);
            stage_pack(&tables, entries).instrument(span).await?;
            probe.record(&probe.data_flush_ns, flush_start);
            Ok(last_block)
        }));
        Ok(())
    }

    /// Await every in-flight flush; each completion advances `data_durable`.
    async fn drain_flushes(&mut self) -> Result<()> {
        while let Some(joined) = self.flushes.next().await {
            self.progress
                .set_data_durable(joined.map_err(task_join_err)??);
        }
        Ok(())
    }

    /// Full durability barrier: every in-flight encode is packed, the residual
    /// pack is flushed, and ALL flushes (including that one) are durable on
    /// return. The checkpoint rendezvous and epoch dict training depend on this.
    async fn flush_barrier(&mut self) -> Result<()> {
        self.drain_encodes().await?;
        self.start_flush().await?;
        self.drain_flushes().await
    }

    /// Await the oldest in-flight encode (block order), fold it into the
    /// checksum chain, and push it to the packer. This serial drain is the ONLY
    /// place `chain_head` advances, so the fold order is exactly block order
    /// regardless of encode parallelism.
    async fn pop_encode(&mut self) -> Result<bool> {
        let Some(joined) = self.inflight.next().await else {
            return Ok(false);
        };
        let mut entry = joined.map_err(task_join_err)??;
        self.chain_head = chain(self.chain_head, entry.content_digest);
        entry.record.row_chain = self.chain_head;
        self.packer.push(entry.record.block_number, entry);
        Ok(true)
    }

    /// Drain all in-flight encodes into the packer in block order. Called at
    /// every ordering barrier so the subsequent flush covers a contiguous
    /// prefix.
    async fn drain_encodes(&mut self) -> Result<()> {
        while self.pop_encode().await? {}
        Ok(())
    }
}

/// Once the leading clamped-span blocks of the current epoch are durable, the
/// NEXT epoch's dict corpus is complete: hint the resolver to pre-train. Keyed
/// on the durable frontier so the sampled blocks are persisted before training
/// reads them back; idempotent per epoch via `last_prewarmed`.
///
/// The span MUST be the same clamped value [`DictConfig::training_range`]
/// samples (`clamped_sample_span`): the corpus is `[start, start + span)`, so
/// it is durable once `data_durable` reaches `start + span - 1` — with
/// `sample_span_blocks >= epoch_blocks` that is the epoch's last block, the
/// raw span would never trigger.
fn maybe_prewarm<R: CodecResolver>(
    resolver: &R,
    data_durable: u64,
    dict_config: &DictConfig,
    last_prewarmed: &mut u32,
) {
    let epoch_blocks = dict_config.epoch_blocks;
    let span = dict_config.clamped_sample_span();
    // `span <= 1`: prewarm buys nothing (`ensure` trains a one-block corpus
    // trivially at the barrier), and cold boot seeds `data_durable = 0` for
    // "nothing durable" — a span-1 trigger there would pre-train against a
    // missing corpus and durably persist the empty-dict sentinel for epoch 1.
    if epoch_blocks == 0 || span <= 1 || data_durable % epoch_blocks + 1 < span {
        return;
    }
    let next = data_durable / epoch_blocks + 1;
    let Ok(next) = u32::try_from(next) else {
        return;
    };
    if next > *last_prewarmed {
        *last_prewarmed = next;
        resolver.prewarm(next);
    }
}

/// Compress one block's rows into its combined blob + per-family headers.
/// Family blobs are concatenated `log ++ tx ++ trace`; each header's
/// `base_offset` is its region's start. `physical_key`/`physical_base_offset`
/// are stamped later by the store's block-blob coalescer at flush.
///
/// Also computes the block's standalone content digest (the per-row hashing
/// happens in the same encode pass, so it is almost free here); the chain fold
/// stays on the serial drain. `record.row_chain` is stamped there, not
/// here.
pub(crate) fn encode_pack_entry(b: &AssignedBlock, codecs: &Codecs) -> Result<PackEntry> {
    let logs = flatten_logs(&b.block)?;
    let (mut log_header, log_blob, log_rows_digest) = encode_block_logs(&logs, &codecs.log)?;
    let (mut tx_header, tx_blob, tx_rows_digest) = encode_block_txs(&b.block.txs, &codecs.tx)?;
    let (mut trace_header, trace_blob, trace_rows_digest) =
        encode_block_traces(&b.block.traces, &codecs.trace)?;

    let log_len = u32::try_from(log_blob.len())
        .map_err(|_| MonadChainDataError::Decode("block log blob too large"))?;
    let tx_len = u32::try_from(tx_blob.len())
        .map_err(|_| MonadChainDataError::Decode("block tx blob too large"))?;
    let trace_base = log_len
        .checked_add(tx_len)
        .ok_or(MonadChainDataError::Decode(
            "combined block blob base offset overflow",
        ))?;
    log_header.base_offset = 0;
    tx_header.base_offset = log_len;
    trace_header.base_offset = trace_base;

    let mut combined_blob = log_blob;
    combined_blob.extend_from_slice(&tx_blob);
    combined_blob.extend_from_slice(&trace_blob);

    let (record, content_digest) =
        block_record_and_digest(b, log_rows_digest, tx_rows_digest, trace_rows_digest);

    Ok(PackEntry {
        combined_blob,
        log_header,
        tx_header,
        trace_header,
        record,
        evm_header: b.block.header.clone(),
        hash_locations: collect_hash_locations(&b.block)?,
        content_digest,
    })
}

/// External-payload variant of [`encode_pack_entry`]: no blob is produced —
/// the three headers point into the source's archive objects
/// (`ENCODING_EXTERNAL_V1`, dict version 0) after the spec is validated
/// against the decoded block. The per-family row digests still run the native
/// row encoders over the decoded rows (encode, digest, discard), so
/// `row_chain` is mode-independent.
pub(crate) fn encode_external_pack_entry(b: &AssignedBlock) -> Result<PackEntry> {
    let spec = b
        .block
        .external
        .as_ref()
        .ok_or(MonadChainDataError::InvalidBlock(
            "external payload mode requires an external payload spec on every block",
        ))?;
    spec.validate(b.number, &b.block)?;

    let logs = flatten_logs(&b.block)?;
    let (record, content_digest) = block_record_and_digest(
        b,
        digest_block_logs(&logs),
        digest_block_txs(&b.block.txs),
        digest_block_traces(&b.block.traces),
    );

    Ok(PackEntry {
        combined_blob: Vec::new(),
        log_header: external_header(&spec.logs),
        tx_header: external_header(&spec.txs),
        trace_header: external_header(&spec.traces),
        record,
        evm_header: b.block.header.clone(),
        hash_locations: collect_hash_locations(&b.block)?,
        content_digest,
    })
}

/// Stamps one validated [`ExternalFamilyRegion`] into its block-blob header.
fn external_header(region: &ExternalFamilyRegion) -> BlockBlobHeader {
    BlockBlobHeader {
        offsets: region.container_offsets.clone(),
        dict_version: DICT_VERSION_NONE,
        base_offset: region.base_offset,
        physical_key: region.key.clone(),
        physical_base_offset: 0,
        encoding: ENCODING_EXTERNAL_V1,
        container_rows: region.container_rows.clone(),
        container_status: Bytes::from(region.container_status.clone()),
    }
}

/// The block's record (with the `row_chain` placeholder) and standalone
/// content digest, shared by both payload modes so their `row_chain` values
/// are byte-identical for the same logical block.
fn block_record_and_digest(
    b: &AssignedBlock,
    log_rows_digest: ChainDigest,
    tx_rows_digest: ChainDigest,
    trace_rows_digest: ChainDigest,
) -> (BlockRecord, ChainDigest) {
    let record = BlockRecord {
        block_number: b.number,
        block_hash: b.block.block_hash(),
        parent_hash: b.block.parent_hash(),
        logs: FamilyWindowRecord {
            first_primary_id: PrimaryId::from(b.ranges.log_first),
            count: b.ranges.log_count,
        },
        txs: FamilyWindowRecord {
            first_primary_id: PrimaryId::from(b.ranges.tx_first),
            count: b.ranges.tx_count,
        },
        traces: FamilyWindowRecord {
            first_primary_id: PrimaryId::from(b.ranges.trace_first),
            count: b.ranges.trace_count,
        },
        // Placeholder until the serial drain folds the chain (pop_encode).
        row_chain: crate::engine::digest::EMPTY_DIGEST,
    };

    let content_digest = block_content_digest(
        b.number,
        &record.block_hash,
        &record.parent_hash,
        &alloy_rlp::encode(&b.block.header),
        family_content_digest(record.logs, log_rows_digest),
        family_content_digest(record.txs, tx_rows_digest),
        family_content_digest(record.traces, trace_rows_digest),
    );
    (record, content_digest)
}

/// Stage a flushed pack's per-block artifacts (row blob, block metadata, hash
/// indexes) in one write session; its durability is what lets a completed
/// flush advance `data_durable`.
pub(crate) async fn stage_pack<M, B>(tables: &Tables<M, B>, entries: Vec<PackEntry>) -> Result<()>
where
    M: MetaStore,
    B: BlobStore,
{
    if entries.is_empty() {
        return Ok(());
    }
    tables
        .with_writes(|w| {
            Box::pin(async move {
                let tables = w.tables();
                let blocks = tables.blocks();
                for entry in entries {
                    let number = entry.record.block_number;
                    blocks.stage_metadata(
                        w,
                        number,
                        &entry.record,
                        &entry.evm_header,
                        Bytes::from(entry.log_header.encode()),
                        Bytes::from(entry.tx_header.encode()),
                        Bytes::from(entry.trace_header.encode()),
                    );
                    tables.stage_block_blob(w, number, entry.combined_blob);
                    blocks.stage_hash_index(w, &entry.record.block_hash, number);
                    for (tx_hash, location) in &entry.hash_locations {
                        tables.tx_hash_index().stage_put(w, tx_hash, *location);
                    }
                }
                Ok(())
            })
        })
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct Fake(Mutex<Vec<u32>>);
    impl CodecResolver for Fake {
        async fn ensure(&self, _v: u32) -> Result<Codecs> {
            Err(MonadChainDataError::Backend("unused".into()))
        }
        fn prewarm(&self, version: u32) {
            self.0.lock().unwrap().push(version);
        }
    }

    fn dict_config(epoch_blocks: u64, sample_span_blocks: u64) -> DictConfig {
        DictConfig {
            epoch_blocks,
            sample_span_blocks,
            ..DictConfig::default()
        }
    }

    #[test]
    fn maybe_prewarm_fires_once_per_epoch_when_corpus_durable() {
        let fake = Fake(Mutex::new(Vec::new()));
        let mut last = 0u32;
        let cfg = dict_config(100, 60);
        let fired = || fake.0.lock().unwrap().clone();

        // Corpus for epoch 1 is blocks [0, 60); through 58 it is incomplete.
        maybe_prewarm(&fake, 58, &cfg, &mut last);
        assert!(fired().is_empty());
        // Block 59 durable completes the corpus: pre-train epoch 1.
        maybe_prewarm(&fake, 59, &cfg, &mut last);
        assert_eq!(fired(), vec![1]);
        // Further durable progress in the same epoch: no repeat.
        maybe_prewarm(&fake, 99, &cfg, &mut last);
        assert_eq!(fired(), vec![1]);
        // Next epoch's corpus [100, 160) complete: pre-train epoch 2.
        maybe_prewarm(&fake, 159, &cfg, &mut last);
        assert_eq!(fired(), vec![1, 2]);
    }

    /// `sample_span_blocks >= epoch_blocks` is a tolerated config (clamped to
    /// `epoch_blocks`, see `DictConfig::training_range`): the corpus is the
    /// whole previous epoch, complete at its last durable block. The raw span
    /// would make the trigger unreachable.
    #[test]
    fn maybe_prewarm_fires_with_span_clamped_to_epoch() {
        let fake = Fake(Mutex::new(Vec::new()));
        let mut last = 0u32;
        let cfg = dict_config(100, 900_000);
        let fired = || fake.0.lock().unwrap().clone();

        // Epoch 0 not fully durable: corpus for epoch 1 incomplete.
        maybe_prewarm(&fake, 98, &cfg, &mut last);
        assert!(fired().is_empty());
        // Last block of epoch 0 durable: pre-train epoch 1, exactly once.
        maybe_prewarm(&fake, 99, &cfg, &mut last);
        maybe_prewarm(&fake, 150, &cfg, &mut last);
        assert_eq!(fired(), vec![1]);
        // Last block of epoch 1 durable: pre-train epoch 2.
        maybe_prewarm(&fake, 199, &cfg, &mut last);
        assert_eq!(fired(), vec![1, 2]);
    }

    /// A clamped span <= 1 never prewarms: `ensure` trains a one-block corpus
    /// trivially at the barrier, and cold boot seeds `data_durable = 0` for
    /// "nothing durable", so firing there would pre-train against a missing
    /// corpus and persist the empty-dict sentinel.
    #[test]
    fn maybe_prewarm_skips_degenerate_span() {
        let fake = Fake(Mutex::new(Vec::new()));
        let mut last = 0u32;

        // sample_span_blocks == 1: no fire on the cold-boot sentinel or later.
        let cfg = dict_config(100, 1);
        maybe_prewarm(&fake, 0, &cfg, &mut last);
        maybe_prewarm(&fake, 100, &cfg, &mut last);
        // epoch_blocks == 1 clamps any span to 1: same skip.
        let cfg = dict_config(1, 900_000);
        maybe_prewarm(&fake, 0, &cfg, &mut last);
        assert!(fake.0.lock().unwrap().is_empty());
    }
}
