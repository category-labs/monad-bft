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

#![allow(dead_code)]

pub mod observed_store;
use std::collections::HashSet;

use alloy_primitives::{Address, Bytes, Log, LogData, B256, U256};
use monad_query_engine::{
    bitmap::{BitmapPageArtifact, BitmapPageCounts},
    family::Family,
    tables::Tables,
    WriteSession,
};
use monad_query_primitives::{
    limits::QueryEnvelope,
    order::QueryOrder,
    records::{BlockRecord, FamilyWindowRecord, PrimaryId},
    CallKind, EvmBlockHeader,
};
use monad_query_read::{
    api::MonadChainDataService,
    logs::{LogFilter, LogsRelations, QueryLogsRequest},
    traces::{QueryTracesRequest, TraceFilter, TracesRelations},
    txs::{QueryTransactionsRequest, TxFilter, TxsRelations},
};
use monad_query_store::{BlobStore, CacheConfig, MetaStore};
/// Canonical population helpers, aliased so tests read `populate::*`.
#[allow(unused_imports)]
pub use monad_query_testkit as populate;
use monad_query_types::ingest_types::{FinalizedBlock, IngestTrace, IngestTx};

pub type Topic = B256;

pub fn test_header(number: u64, parent_hash: B256) -> EvmBlockHeader {
    EvmBlockHeader {
        number,
        parent_hash,
        ..EvmBlockHeader::default()
    }
}

pub fn chain_header(number: u64, parent: &EvmBlockHeader) -> EvmBlockHeader {
    test_header(number, parent.hash_slow())
}

/// Bounded block-range envelope.
pub fn envelope(from: u64, to: u64, order: QueryOrder, limit: usize) -> QueryEnvelope {
    QueryEnvelope {
        from_block: Some(from),
        to_block: Some(to),
        order,
        limit,
    }
}

/// Ascending envelope over `from..=to`.
pub fn ascending_envelope(from: u64, to: u64, limit: usize) -> QueryEnvelope {
    envelope(from, to, QueryOrder::Ascending, limit)
}

/// Descending envelope: `from` is the upper (newest) bound, `to` the lower.
pub fn descending_envelope(from: u64, to: u64, limit: usize) -> QueryEnvelope {
    envelope(from, to, QueryOrder::Descending, limit)
}

/// Log with the canonical 3-byte test payload.
pub fn log(address: Address, topics: Vec<Topic>) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(topics, Bytes::from(vec![1, 2, 3])),
    }
}

/// `count` copies of [`log`] with the same address and topics.
pub fn repeated_logs(address: Address, topics: Vec<Topic>, count: usize) -> Vec<Log> {
    std::iter::repeat_with(|| log(address, topics.clone()))
        .take(count)
        .collect()
}

/// AND-filter on a single address and topic0.
pub fn log_filter(address: Address, topic0: Topic) -> LogFilter {
    LogFilter {
        address: Some(HashSet::from([address])),
        topics: [Some(HashSet::from([topic0])), None, None, None],
    }
}

/// Address-only log filter.
pub fn address_filter(address: Address) -> LogFilter {
    LogFilter {
        address: Some(HashSet::from([address])),
        topics: [None, None, None, None],
    }
}

/// Logs query with default relations.
pub fn logs_request(envelope: QueryEnvelope, filter: LogFilter) -> QueryLogsRequest {
    QueryLogsRequest {
        envelope,
        filter,
        relations: LogsRelations::default(),
    }
}

/// Traces query with default relations.
pub fn traces_request(envelope: QueryEnvelope, filter: TraceFilter) -> QueryTracesRequest {
    QueryTracesRequest {
        envelope,
        filter,
        relations: TracesRelations::default(),
    }
}

/// From-filtered transactions query with default relations.
pub fn tx_query(from: Address, envelope: QueryEnvelope) -> QueryTransactionsRequest {
    QueryTransactionsRequest {
        envelope,
        filter: TxFilter {
            from: Some(HashSet::from([from])),
            ..Default::default()
        },
        relations: TxsRelations::default(),
    }
}

/// Logs-only block.
pub fn block_with_logs(header: EvmBlockHeader, logs_by_tx: Vec<Vec<Log>>) -> FinalizedBlock {
    FinalizedBlock {
        header,
        logs_by_tx,
        txs: Vec::new(),
        traces: Vec::new(),
        external: None,
    }
}

/// Header-only block with no logs, txs, or traces.
pub fn empty_block(header: EvmBlockHeader) -> FinalizedBlock {
    block_with_logs(header, vec![])
}

/// Logs-only chain of blocks `1..=count`, headers linked by `hash_slow`;
/// `make_logs` supplies each block's per-tx logs.
pub fn chain_of_blocks(
    count: u64,
    mut make_logs: impl FnMut(u64) -> Vec<Vec<Log>>,
) -> Vec<FinalizedBlock> {
    let mut parent_hash = B256::ZERO;
    (1..=count)
        .map(|number| {
            let header = test_header(number, parent_hash);
            parent_hash = header.hash_slow();
            block_with_logs(header, make_logs(number))
        })
        .collect()
}

/// Txs-only block; one empty log list per tx, as ingest expects.
pub fn block_with_txs(header: EvmBlockHeader, txs: Vec<IngestTx>) -> FinalizedBlock {
    FinalizedBlock {
        header,
        logs_by_tx: vec![Vec::new(); txs.len()],
        txs,
        traces: Vec::new(),
        external: None,
    }
}

/// Traces-only block.
pub fn block_with_traces(header: EvmBlockHeader, traces: Vec<IngestTrace>) -> FinalizedBlock {
    FinalizedBlock {
        header,
        logs_by_tx: Vec::new(),
        txs: Vec::new(),
        traces,
        external: None,
    }
}

/// Sets a deterministic tx hash on a built [`IngestTx`].
pub fn with_hash(mut tx: IngestTx, tx_hash: B256) -> IngestTx {
    tx.tx_hash = tx_hash;
    tx
}

/// Empty-window block record for header-staging tests.
pub fn block_record(number: u64) -> BlockRecord {
    let window = FamilyWindowRecord {
        first_primary_id: PrimaryId::ZERO,
        count: 0,
    };
    BlockRecord {
        block_number: number,
        block_hash: Default::default(),
        parent_hash: Default::default(),
        logs: window,
        txs: window,
        traces: window,
        row_chain: Default::default(),
    }
}

/// Loads block `n`'s [`BlockRecord`], panicking if it is absent.
pub async fn load_record<M, B>(service: &MonadChainDataService<M, B>, n: u64) -> BlockRecord
where
    M: MetaStore,
    B: BlobStore,
{
    service
        .tables()
        .blocks()
        .load_record(n)
        .await
        .unwrap_or_else(|e| panic!("load record {n}: {e:?}"))
        .unwrap_or_else(|| panic!("record {n} missing"))
}

/// Cache config for the table-level suites: defaults with the decoded-row
/// cache off, so backend read counters see every materialization.
pub fn test_cache_config() -> CacheConfig {
    CacheConfig {
        row_cache_bytes: 0,
        ..CacheConfig::default()
    }
}

/// Stages block `number`'s header metadata (empty family payloads) inside an
/// open write session.
pub fn stage_block_header<M, B>(tables: &Tables<M, B>, w: &mut WriteSession<'_, M, B>, number: u64)
where
    M: MetaStore,
    B: BlobStore,
{
    let header = EvmBlockHeader {
        number,
        ..Default::default()
    };
    tables.blocks().stage_metadata(
        w,
        number,
        &block_record(number),
        &header,
        bytes::Bytes::new(),
        bytes::Bytes::new(),
        bytes::Bytes::new(),
    );
}

/// Stages and commits block `number`'s header metadata via the production
/// staged-write path.
pub async fn stage_block<M, B>(tables: &Tables<M, B>, number: u64)
where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                stage_block_header(tables, w, number);
                Ok(())
            })
        })
        .await
        .expect("stage block header");
}

pub fn minimal_ingest_tx() -> IngestTx {
    ingest_tx(Address::ZERO, None, Vec::new())
}

/// `to = None` is contract creation; filters read `sender`, not the recovered signer.
pub fn ingest_tx(sender: Address, to: Option<Address>, input: Vec<u8>) -> IngestTx {
    use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Signature, TxKind};

    let signed = TxLegacy {
        chain_id: Some(1),
        nonce: 0,
        gas_price: 0,
        gas_limit: 21_000,
        to: to.map_or(TxKind::Create, TxKind::Call),
        value: U256::ZERO,
        input: input.into(),
    }
    .into_signed(Signature::test_signature());

    let mut signed_tx_bytes = Vec::new();
    TxEnvelope::Legacy(signed).encode_2718(&mut signed_tx_bytes);

    IngestTx {
        tx_hash: B256::ZERO,
        sender,
        signed_tx_bytes: signed_tx_bytes.into(),
    }
}

/// Successful (`status: 0`, `tx_status: true`) top-level zero-value trace with
/// canonical gas figures; override fields via struct-update syntax:
/// `IngestTrace { value: U256::from(50u64), ..base_trace(...) }`.
pub fn base_trace(tx_index: u32, typ: CallKind, from: Address, to: Option<Address>) -> IngestTrace {
    IngestTrace {
        typ,
        from,
        to,
        value: U256::ZERO,
        gas: 100_000,
        gas_used: 21_000,
        input: Bytes::new(),
        output: Bytes::new(),
        status: 0,
        depth: 0,
        tx_index,
        trace_address: vec![],
        tx_status: true,
    }
}

/// Successful top-level CALL; with `value > 0` it qualifies for `has_transfer`.
pub fn top_level_call(
    tx_index: u32,
    from: Address,
    to: Address,
    value: U256,
    input: Vec<u8>,
) -> IngestTrace {
    IngestTrace {
        value,
        input: Bytes::from(input),
        ..base_trace(tx_index, CallKind::Call, from, Some(to))
    }
}

pub fn nested_call(
    tx_index: u32,
    from: Address,
    to: Address,
    value: U256,
    input: Vec<u8>,
) -> IngestTrace {
    IngestTrace {
        depth: 1,
        trace_address: vec![0],
        ..top_level_call(tx_index, from, to, value, input)
    }
}

/// Durably writes one bitmap page artifact via the staged (production) write path.
pub async fn seed_bitmap_page_artifact<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    stream_id: &str,
    page_start: u64,
    artifact: &BitmapPageArtifact,
) where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables
                    .family(family)
                    .bitmap()
                    .stage_page_artifact(w, stream_id, page_start, artifact);
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page artifact");
}

/// Durably writes one open-page bitmap fragment via the staged write path.
pub async fn seed_bitmap_page_fragment<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    stream_id: &str,
    page_start: u64,
    flush_block: u64,
    blob: bytes::Bytes,
) where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables.family(family).bitmap().stage_page_fragment(
                    w,
                    stream_id,
                    page_start,
                    flush_block,
                    blob,
                );
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page fragment");
}

pub async fn seed_bitmap_page_counts<M, B>(
    tables: &Tables<M, B>,
    family: Family,
    stream_id: &str,
    group_start: u64,
    counts: &BitmapPageCounts,
) where
    M: MetaStore,
    B: BlobStore,
{
    tables
        .with_writes(|w| {
            Box::pin(async move {
                tables
                    .family(family)
                    .bitmap()
                    .stage_page_counts(w, stream_id, group_start, counts);
                Ok(())
            })
        })
        .await
        .expect("seed bitmap page counts");
}
