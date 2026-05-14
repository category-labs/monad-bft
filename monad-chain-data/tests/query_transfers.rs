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

use std::collections::HashSet;

use alloy_primitives::U256;
use monad_chain_data::{
    Address, CallKind, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore,
    MonadChainDataService, QueryEnvelope, QueryLimits, QueryOrder, QueryTransfersRequest,
    TransferFilter, TransfersRelations, B256,
};

mod common;

use common::{make_trace, test_header};

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn build_service() -> MonadChainDataService<InMemoryMetaStore, InMemoryBlobStore> {
    MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn transfers_excludes_zero_value_delegate_and_failed_frames() {
    let service = build_service();
    let from_ = addr(0xaa);
    let to_ = addr(0xbb);
    let other = addr(0xcc);

    // Mix of: (tx_index, depth, kind, value, status, tx_status)
    let traces = vec![
        // 0: top-level Call, value > 0, success — qualifies
        make_trace(
            0,
            0,
            vec![],
            CallKind::Call,
            from_,
            Some(to_),
            U256::from(100u64),
            vec![],
            0,
            true,
        ),
        // 1: top-level DelegateCall, value > 0 — never qualifies
        make_trace(
            1,
            0,
            vec![],
            CallKind::DelegateCall,
            from_,
            Some(to_),
            U256::from(50u64),
            vec![],
            0,
            true,
        ),
        // 2: top-level Call, value == 0 — never qualifies
        make_trace(
            2,
            0,
            vec![],
            CallKind::Call,
            from_,
            Some(to_),
            U256::ZERO,
            vec![],
            0,
            true,
        ),
        // 3: top-level Call, value > 0 but tx reverted
        make_trace(
            3,
            0,
            vec![],
            CallKind::Call,
            from_,
            Some(to_),
            U256::from(10u64),
            vec![],
            0,
            false,
        ),
        // 4: top-level Call, value > 0 but frame status != 0
        make_trace(
            4,
            0,
            vec![],
            CallKind::Call,
            from_,
            Some(to_),
            U256::from(10u64),
            vec![],
            1,
            true,
        ),
        // 5: nested Call under tx 0 — qualifies, value > 0 success
        make_trace(
            0,
            1,
            vec![0],
            CallKind::Call,
            from_,
            Some(other),
            U256::from(5u64),
            vec![],
            0,
            true,
        ),
        // 6: top-level SelfDestruct, success — qualifies
        make_trace(
            5,
            0,
            vec![],
            CallKind::SelfDestruct,
            from_,
            Some(to_),
            U256::from(1u64),
            vec![],
            0,
            true,
        ),
    ];

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces,
        })
        .await
        .expect("ingest");

    let resp = service
        .query_transfers(QueryTransfersRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: TransferFilter::default(),
            relations: TransfersRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.transfers.len(), 3);
    let kinds: Vec<CallKind> = resp.transfers.iter().map(|t| t.typ).collect();
    assert!(kinds.contains(&CallKind::Call));
    assert!(kinds.contains(&CallKind::SelfDestruct));
}

#[tokio::test(flavor = "current_thread")]
async fn transfers_filter_is_top_level_true_drops_nested() {
    let service = build_service();
    let from_ = addr(1);
    let to_ = addr(2);

    let traces = vec![
        make_trace(
            0,
            0,
            vec![],
            CallKind::Call,
            from_,
            Some(to_),
            U256::from(10u64),
            vec![],
            0,
            true,
        ),
        make_trace(
            0,
            1,
            vec![0],
            CallKind::Call,
            from_,
            Some(to_),
            U256::from(5u64),
            vec![],
            0,
            true,
        ),
    ];

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces,
        })
        .await
        .expect("ingest");

    let resp = service
        .query_transfers(QueryTransfersRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: TransferFilter {
                is_top_level: Some(true),
                ..Default::default()
            },
            relations: TransfersRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.transfers.len(), 1);
    assert!(resp.transfers[0].is_top_level());
}

#[tokio::test(flavor = "current_thread")]
async fn transfers_filter_by_from() {
    let service = build_service();
    let alice = addr(0xaa);
    let bob = addr(0xbb);
    let recipient = addr(0x11);

    let traces = vec![
        make_trace(
            0,
            0,
            vec![],
            CallKind::Call,
            alice,
            Some(recipient),
            U256::from(10u64),
            vec![],
            0,
            true,
        ),
        make_trace(
            1,
            0,
            vec![],
            CallKind::Call,
            bob,
            Some(recipient),
            U256::from(10u64),
            vec![],
            0,
            true,
        ),
    ];

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![],
            txs: vec![],
            traces,
        })
        .await
        .expect("ingest");

    let resp = service
        .query_transfers(QueryTransfersRequest {
            envelope: QueryEnvelope {
                from_block: Some(1),
                to_block: Some(1),
                order: QueryOrder::Ascending,
                limit: 100,
            },
            filter: TransferFilter {
                from: Some(HashSet::from([alice])),
                ..Default::default()
            },
            relations: TransfersRelations::default(),
        })
        .await
        .expect("query");

    assert_eq!(resp.transfers.len(), 1);
    assert_eq!(resp.transfers[0].from, alice);
}
