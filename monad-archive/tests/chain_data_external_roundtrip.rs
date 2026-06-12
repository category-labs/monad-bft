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

//! THE external-payload round-trip guard: blocks encoded with the REAL
//! archive encoders, offsets from the REAL scanner, ingested by the REAL
//! chain-data engine in external mode (locators into the archive objects,
//! no payload blobs), must answer every family query IDENTICALLY to a
//! native-mode ingest of the same logical blocks — and reach the same
//! `BlockRecord`s (including `row_chain`).

use std::{collections::HashSet, sync::Arc};

use alloy_consensus::{
    BlockBody, Header, Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction, TxEip1559,
    TxEip2930, TxLegacy,
};
use alloy_primitives::{
    Address, Bloom, Bytes, Log, LogData, Signature, TxKind, B256, U256, U64, U8,
};
use alloy_rlp::Encodable;
use monad_archive::{
    chain_data_source::ArchiverChainDataSource,
    kvstore::{memory::MemoryStorage, WritePolicy},
    model::{
        block_data_archive::{Block, BlockDataArchive, CallFrame, CallKind},
        BlockDataReaderErased,
    },
    prelude::{KVReader, LatestKind},
};
use monad_chain_data::{
    engine::family::BLOCK_BLOB_TABLE,
    testkit::{populate_via_engine, populate_via_engine_external},
    ChainDataIngestSource, FinalizedBlock, InMemoryExternalBlobReader, LogFilter,
    MonadChainDataService, QueryEnvelope, QueryLogsRequest, QueryOrder, QueryTracesRequest,
    QueryTransactionsRequest, QueryTransfersRequest, TraceFilter, TransferFilter, TxFilter,
};
use monad_eth_types::{ReceiptWithLogIndex, TxEnvelopeWithSender};

const SENDER_A: Address = Address::repeat_byte(0xa1);
const SENDER_B: Address = Address::repeat_byte(0xb2);
const LOG_ADDR: Address = Address::repeat_byte(0xcc);
const CALLEE: Address = Address::repeat_byte(0xdd);

fn legacy_tx(nonce: u64, sender: Address) -> TxEnvelopeWithSender {
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 7,
        gas_limit: 21_000,
        to: TxKind::Call(CALLEE),
        value: U256::from(nonce + 1),
        input: Bytes::from(vec![0x11, 0x22, 0x33, 0x44, 0x55]),
    };
    TxEnvelopeWithSender {
        tx: tx.into_signed(Signature::test_signature()).into(),
        sender,
    }
}

fn eip1559_tx(nonce: u64, sender: Address) -> TxEnvelopeWithSender {
    let tx = TxEip1559 {
        chain_id: 1,
        nonce,
        gas_limit: 60_000,
        max_fee_per_gas: 1_000,
        max_priority_fee_per_gas: 10,
        to: TxKind::Call(CALLEE),
        value: U256::from(5u64),
        input: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0x01, 0x02]),
        ..Default::default()
    };
    TxEnvelopeWithSender {
        tx: tx.into_signed(Signature::test_signature()).into(),
        sender,
    }
}

fn eip2930_tx(nonce: u64, sender: Address) -> TxEnvelopeWithSender {
    let tx = TxEip2930 {
        chain_id: 1,
        nonce,
        gas_price: 9,
        gas_limit: 30_000,
        // Contract creation: exercises the `to = None` paths.
        to: TxKind::Create,
        value: U256::ZERO,
        input: Bytes::from(vec![0x60, 0x80, 0x60, 0x40]),
        ..Default::default()
    };
    TxEnvelopeWithSender {
        tx: tx.into_signed(Signature::test_signature()).into(),
        sender,
    }
}

fn log(topic: u8, data: &[u8]) -> Log {
    Log {
        address: LOG_ADDR,
        data: LogData::new_unchecked(vec![B256::repeat_byte(topic)], Bytes::copy_from_slice(data)),
    }
}

fn receipt(status: bool, logs: Vec<Log>, starting_log_index: u64) -> ReceiptWithLogIndex {
    ReceiptWithLogIndex {
        receipt: ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt {
                status: status.into(),
                cumulative_gas_used: 21_000,
                logs,
            },
            Bloom::repeat_byte(0x0f),
        )),
        starting_log_index,
    }
}

#[allow(clippy::too_many_arguments)]
fn frame(
    typ: CallKind,
    from: Address,
    to: Option<Address>,
    value: u64,
    status: u8,
    depth: u64,
    input: &[u8],
) -> CallFrame {
    CallFrame {
        typ,
        flags: U64::ZERO,
        from,
        to,
        value: U256::from(value),
        gas: U64::from(100_000u64),
        gas_used: U64::from(21_000u64),
        input: Bytes::copy_from_slice(input),
        output: Bytes::new(),
        status: U8::from(status),
        depth: U64::from(depth),
        // Always present so concatenated frames stay self-delimiting.
        logs: Some(vec![]),
    }
}

fn trace_blob(frames: Vec<CallFrame>) -> Vec<u8> {
    let nested: Vec<Vec<CallFrame>> = vec![frames];
    let mut blob = Vec::new();
    nested.encode(&mut blob);
    blob
}

struct FixtureBlock {
    block: Block,
    receipts: Vec<ReceiptWithLogIndex>,
    traces: Vec<Vec<u8>>,
}

/// Blocks 1..=4: multiple tx types, txs with 0 and many logs, nested trace
/// trees, a reverted tx (status bits + transfer exclusion), a frameless
/// trace container, and an entirely empty block.
fn fixture_blocks() -> Vec<FixtureBlock> {
    let mut parent_hash = B256::ZERO;
    let mut header = |number: u64| {
        let header = Header {
            number,
            parent_hash,
            gas_limit: 30_000_000,
            timestamp: 1_700_000_000 + number,
            base_fee_per_gas: Some(100),
            ..Default::default()
        };
        parent_hash = header.hash_slow();
        header
    };

    // Block 1: three txs (legacy / 1559 / 2930-create); tx 1 reverted.
    let b1 = FixtureBlock {
        block: Block {
            header: header(1),
            body: BlockBody {
                transactions: vec![
                    legacy_tx(0, SENDER_A),
                    eip1559_tx(1, SENDER_B),
                    eip2930_tx(2, SENDER_A),
                ],
                ommers: vec![],
                withdrawals: None,
            },
        },
        receipts: vec![
            receipt(true, vec![log(0x10, b"a"), log(0x11, b"bb")], 0),
            receipt(false, vec![], 2),
            receipt(true, vec![log(0x12, b"ccc")], 2),
        ],
        traces: vec![
            // tx 0: root -> {A -> A1, B}; A1 carries a value transfer.
            trace_blob(vec![
                frame(
                    CallKind::Call,
                    SENDER_A,
                    Some(CALLEE),
                    1,
                    0,
                    0,
                    &[0x11, 0x22, 0x33, 0x44],
                ),
                frame(CallKind::Call, CALLEE, Some(SENDER_B), 0, 0, 1, &[]),
                frame(CallKind::Call, SENDER_B, Some(CALLEE), 9, 0, 2, &[]),
                frame(CallKind::StaticCall, CALLEE, Some(SENDER_A), 0, 0, 1, &[]),
            ]),
            // tx 1: reverted (frame status non-zero, receipt false): its
            // value-carrying frame must NOT surface as a transfer.
            trace_blob(vec![frame(
                CallKind::Call,
                SENDER_B,
                Some(CALLEE),
                7,
                1,
                0,
                &[0xde, 0xad, 0xbe, 0xef],
            )]),
            // tx 2: contract creation (`to` resolved by the tracer).
            trace_blob(vec![frame(
                CallKind::Create,
                SENDER_A,
                Some(CALLEE),
                0,
                0,
                0,
                &[0x60, 0x80],
            )]),
        ],
    };

    // Block 2: empty.
    let b2 = FixtureBlock {
        block: Block {
            header: header(2),
            body: BlockBody {
                transactions: vec![],
                ommers: vec![],
                withdrawals: None,
            },
        },
        receipts: vec![],
        traces: vec![],
    };

    // Block 3: one tx with many logs and a value transfer.
    let b3 = FixtureBlock {
        block: Block {
            header: header(3),
            body: BlockBody {
                transactions: vec![eip1559_tx(3, SENDER_B)],
                ommers: vec![],
                withdrawals: None,
            },
        },
        receipts: vec![receipt(
            true,
            (0..5).map(|i| log(0x20 + i, &[i; 4])).collect(),
            0,
        )],
        traces: vec![trace_blob(vec![frame(
            CallKind::Call,
            SENDER_B,
            Some(CALLEE),
            1_000,
            0,
            0,
            &[0xaa, 0xbb, 0xcc, 0xdd],
        )])],
    };

    // Block 4: two txs; tx 0's trace container decodes to ZERO frames (an
    // empty container between populated neighbors).
    let b4 = FixtureBlock {
        block: Block {
            header: header(4),
            body: BlockBody {
                transactions: vec![legacy_tx(4, SENDER_A), legacy_tx(5, SENDER_B)],
                ommers: vec![],
                withdrawals: None,
            },
        },
        receipts: vec![
            receipt(true, vec![], 0),
            receipt(true, vec![log(0x30, b"zz")], 0),
        ],
        traces: vec![
            trace_blob(vec![]),
            trace_blob(vec![frame(
                CallKind::SelfDestruct,
                SENDER_B,
                Some(CALLEE),
                3,
                0,
                0,
                &[],
            )]),
        ],
    };

    vec![b1, b2, b3, b4]
}

struct Fixture {
    external_blocks: Vec<FinalizedBlock>,
    native_blocks: Vec<FinalizedBlock>,
    reader: Arc<InMemoryExternalBlobReader>,
}

/// Archives the fixture with the real encoders, fetches it back through the
/// external source (real scanner), and mirrors the raw objects into an
/// in-memory external reader.
async fn build_fixture() -> Fixture {
    let archive = BlockDataArchive::new(MemoryStorage::new("bdr"));
    let blocks = fixture_blocks();
    let last = blocks.len() as u64;
    for fixture in &blocks {
        let number = fixture.block.header.number;
        archive
            .archive_block(fixture.block.clone(), WritePolicy::NoClobber)
            .await
            .unwrap();
        archive
            .archive_receipts(fixture.receipts.clone(), number, WritePolicy::NoClobber)
            .await
            .unwrap();
        archive
            .archive_traces(fixture.traces.clone(), number, WritePolicy::NoClobber)
            .await
            .unwrap();
    }
    archive
        .update_latest(last, LatestKind::Uploaded)
        .await
        .unwrap();

    let reader = Arc::new(InMemoryExternalBlobReader::default());
    for number in 1..=last {
        for key in [
            archive.block_key(number),
            archive.receipts_key(number),
            archive.traces_key(number),
        ] {
            let object = archive.store.get(&key).await.unwrap().expect("object");
            reader.insert(key.into_bytes(), object);
        }
    }

    let source =
        ArchiverChainDataSource::external(BlockDataReaderErased::from(archive), None).unwrap();
    let mut external_blocks = Vec::new();
    for number in 1..=last {
        external_blocks.push(source.fetch_finalized_block(number).await.unwrap());
    }
    let native_blocks: Vec<FinalizedBlock> = external_blocks
        .iter()
        .cloned()
        .map(|mut block| {
            block.external = None;
            block
        })
        .collect();

    Fixture {
        external_blocks,
        native_blocks,
        reader,
    }
}

fn envelope(order: QueryOrder) -> QueryEnvelope {
    // `from` is the starting (newest-first for descending) bound.
    let (from_block, to_block) = match order {
        QueryOrder::Ascending => (1, 4),
        QueryOrder::Descending => (4, 1),
    };
    QueryEnvelope {
        from_block: Some(from_block),
        to_block: Some(to_block),
        order,
        limit: 100,
    }
}

fn set<T: Eq + std::hash::Hash>(values: impl IntoIterator<Item = T>) -> Option<HashSet<T>> {
    Some(values.into_iter().collect())
}

/// Every family query — indexed and block-scan, both orders — plus the
/// tx-hash point lookup must answer identically across payload modes, and
/// the stored `BlockRecord`s (including `row_chain`) must be equal.
#[tokio::test(flavor = "multi_thread")]
async fn external_ingest_answers_identically_to_native() {
    let fixture = build_fixture().await;
    let tx_hashes: Vec<B256> = fixture
        .external_blocks
        .iter()
        .flat_map(|b| b.txs.iter().map(|tx| tx.tx_hash))
        .collect();
    assert_eq!(tx_hashes.len(), 6);

    let native_store = populate_via_engine(fixture.native_blocks).await;
    let external_store =
        populate_via_engine_external(fixture.external_blocks, fixture.reader).await;

    // External mode writes metadata + indexes only: no pack blob objects.
    let external_pack_blobs = external_store
        .blob
        .blob_snapshot()
        .keys()
        .filter(|(table, _)| *table == BLOCK_BLOB_TABLE)
        .count();
    assert_eq!(
        external_pack_blobs, 0,
        "external mode must stage no pack blobs"
    );

    let native = native_store.reader();
    let external = external_store.reader();

    // (b) BlockRecord equality, including the chained row digest.
    for number in 1..=4u64 {
        let native_record = native.tables().blocks().load_record(number).await.unwrap();
        let external_record = external
            .tables()
            .blocks()
            .load_record(number)
            .await
            .unwrap();
        assert_eq!(native_record, external_record, "block record {number}");
        assert!(native_record.is_some());
    }

    for order in [QueryOrder::Ascending, QueryOrder::Descending] {
        // Indexed paths: every clause kind that has an index.
        let tx_request = QueryTransactionsRequest {
            envelope: envelope(order),
            filter: TxFilter {
                from: set([SENDER_A, SENDER_B]),
                ..Default::default()
            },
            ..Default::default()
        };
        let native_txs = native.query_transactions(tx_request.clone()).await.unwrap();
        let external_txs = external.query_transactions(tx_request).await.unwrap();
        assert_eq!(native_txs, external_txs, "indexed txs ({order:?})");
        assert_eq!(native_txs.txs.len(), 6);

        let log_request = QueryLogsRequest {
            envelope: envelope(order),
            filter: LogFilter {
                address: set([LOG_ADDR]),
                ..Default::default()
            },
            ..Default::default()
        };
        let native_logs = native.query_logs(log_request.clone()).await.unwrap();
        let external_logs = external.query_logs(log_request).await.unwrap();
        assert_eq!(native_logs, external_logs, "indexed logs ({order:?})");
        assert_eq!(native_logs.logs.len(), 9);

        let trace_request = QueryTracesRequest {
            envelope: envelope(order),
            filter: TraceFilter {
                from: set([SENDER_A, SENDER_B, CALLEE]),
                ..Default::default()
            },
            ..Default::default()
        };
        let native_traces = native.query_traces(trace_request.clone()).await.unwrap();
        let external_traces = external.query_traces(trace_request).await.unwrap();
        assert_eq!(native_traces, external_traces, "indexed traces ({order:?})");
        // 6 frames in block 1, 1 in block 3, 1 in block 4 (tx 0 is frameless).
        assert_eq!(native_traces.traces.len(), 8);

        let transfer_request = QueryTransfersRequest {
            envelope: envelope(order),
            filter: TransferFilter::default(),
            ..Default::default()
        };
        let native_transfers = native
            .query_transfers(transfer_request.clone())
            .await
            .unwrap();
        let external_transfers = external.query_transfers(transfer_request).await.unwrap();
        assert_eq!(
            native_transfers, external_transfers,
            "transfers ({order:?})"
        );
        // Reverted tx 1 of block 1 must NOT contribute despite value > 0.
        assert_eq!(native_transfers.transfers.len(), 4);

        // Block-scan paths: no indexed clause.
        let scan_txs_request = QueryTransactionsRequest {
            envelope: envelope(order),
            ..Default::default()
        };
        assert_eq!(
            native
                .query_transactions(scan_txs_request.clone())
                .await
                .unwrap(),
            external.query_transactions(scan_txs_request).await.unwrap(),
            "scan txs ({order:?})"
        );
        let scan_logs_request = QueryLogsRequest {
            envelope: envelope(order),
            ..Default::default()
        };
        assert_eq!(
            native.query_logs(scan_logs_request.clone()).await.unwrap(),
            external.query_logs(scan_logs_request).await.unwrap(),
            "scan logs ({order:?})"
        );
        let scan_traces_request = QueryTracesRequest {
            envelope: envelope(order),
            ..Default::default()
        };
        assert_eq!(
            native
                .query_traces(scan_traces_request.clone())
                .await
                .unwrap(),
            external.query_traces(scan_traces_request).await.unwrap(),
            "scan traces ({order:?})"
        );
    }

    // (c) tx-hash point lookups.
    for tx_hash in tx_hashes {
        let native_hit = native.get_transaction(tx_hash).await.unwrap();
        let external_hit = external.get_transaction(tx_hash).await.unwrap();
        assert_eq!(native_hit, external_hit, "get_transaction {tx_hash}");
        assert!(native_hit.is_some());
    }
}

/// Reading an external store without archive access configured must fail
/// loudly (`MissingData`), never silently return empty rows.
#[tokio::test(flavor = "multi_thread")]
async fn external_store_without_archive_reader_errors() {
    let fixture = build_fixture().await;
    let external_store =
        populate_via_engine_external(fixture.external_blocks, fixture.reader).await;

    // A service over the same stores WITHOUT the external reader attached.
    let blind = MonadChainDataService::new(
        external_store.meta.clone(),
        external_store.blob.clone(),
        monad_chain_data::QueryLimits::UNLIMITED,
    );
    let err = blind
        .query_transactions(QueryTransactionsRequest {
            envelope: envelope(QueryOrder::Ascending),
            filter: TxFilter {
                from: set([SENDER_A]),
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, monad_chain_data::MonadChainDataError::MissingData(_)),
        "expected MissingData, got {err:?}"
    );
}
