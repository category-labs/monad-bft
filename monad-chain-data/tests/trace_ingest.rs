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

use alloy_primitives::{Address, U256};
use monad_chain_data::{
    compute_trace_addresses, traces::BlockBlobHeader, Family, FinalizedBlock, InMemoryBlobStore,
    InMemoryMetaStore, MonadChainDataService, PrimaryId, QueryLimits, B256,
};

mod common;

use common::{chain_header, nested_call, test_header, top_level_call};

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_trace_artifacts_for_block_with_traces() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let traces = vec![
        top_level_call(0, addr(1), addr(2), U256::from(100u64), vec![0xaa; 8]),
        nested_call(0, addr(2), addr(3), U256::from(50u64), vec![]),
        top_level_call(1, addr(4), addr(5), U256::ZERO, vec![]),
    ];

    let outcome = service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces,
        })
        .await
        .expect("ingest block 1");

    assert_eq!(outcome.written_traces, 3);
    assert_eq!(outcome.block_record.traces.count, 3);
    assert_eq!(
        outcome.block_record.traces.first_primary_id,
        PrimaryId::ZERO
    );

    let trace_family = service.tables().family(Family::Trace);
    let header_bytes = trace_family
        .load_block_header(1)
        .await
        .expect("load trace header")
        .expect("trace header present");
    let trace_header = BlockBlobHeader::decode(&header_bytes).expect("decode trace header");
    assert_eq!(trace_header.row_count(), 3);

    let blob = service
        .tables()
        .load_block_blob(1)
        .await
        .expect("load trace blob")
        .expect("trace blob present");
    assert_eq!(
        blob.len(),
        usize::try_from(*trace_header.offsets.last().unwrap()).unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn trace_id_window_advances_across_blocks() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    let outcome1 = service
        .ingest_block(FinalizedBlock {
            header: h1.clone(),
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![
                top_level_call(0, addr(1), addr(2), U256::from(1u64), vec![]),
                top_level_call(1, addr(3), addr(4), U256::ZERO, vec![]),
            ],
        })
        .await
        .expect("ingest block 1");
    assert_eq!(
        outcome1.block_record.traces.first_primary_id,
        PrimaryId::ZERO
    );
    assert_eq!(outcome1.block_record.traces.count, 2);

    let h2 = chain_header(2, &h1);
    let outcome2 = service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![],
            txs: vec![],
            traces: vec![top_level_call(0, addr(1), addr(2), U256::ZERO, vec![])],
        })
        .await
        .expect("ingest block 2");
    assert_eq!(
        outcome2.block_record.traces.first_primary_id,
        PrimaryId::new(2)
    );
    assert_eq!(outcome2.block_record.traces.count, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_handles_empty_traces() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let outcome = service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: vec![common::minimal_ingest_tx()],
            traces: vec![],
        })
        .await
        .expect("ingest empty traces");
    assert_eq!(outcome.written_traces, 0);
    assert_eq!(outcome.block_record.traces.count, 0);
}

#[test]
fn trace_address_unit_root_chain() {
    // root -> A -> A1, A2; root -> B; root -> C -> C1 -> C1a
    let depths = [0u32, 1, 2, 2, 1, 1, 2, 3];
    let addresses = compute_trace_addresses(depths).expect("compute");
    assert_eq!(
        addresses,
        vec![
            vec![],
            vec![0],
            vec![0, 0],
            vec![0, 1],
            vec![1],
            vec![2],
            vec![2, 0],
            vec![2, 0, 0],
        ]
    );
}
