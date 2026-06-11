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
    Address, CallKind, IngestTrace, QueryTransfersRequest, TransferFilter, TransfersRelations, B256,
};

mod common;

use common::{
    ascending_envelope, base_trace, block_with_traces, nested_call, test_header, top_level_call,
};

fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

#[tokio::test(flavor = "current_thread")]
async fn transfers_excludes_zero_value_delegate_and_failed_frames() {
    let caller = addr(0xaa);
    let callee = addr(0xbb);
    let other = addr(0xcc);

    let traces = vec![
        // 0: top-level Call, value > 0, success — qualifies
        top_level_call(0, caller, callee, U256::from(100u64), vec![]),
        // 1: top-level DelegateCall, value > 0 — never qualifies
        IngestTrace {
            typ: CallKind::DelegateCall,
            ..top_level_call(1, caller, callee, U256::from(50u64), vec![])
        },
        // 2: top-level Call, value == 0 — never qualifies
        top_level_call(2, caller, callee, U256::ZERO, vec![]),
        // 3: top-level Call, value > 0 but tx reverted
        IngestTrace {
            tx_status: false,
            ..top_level_call(3, caller, callee, U256::from(10u64), vec![])
        },
        // 4: top-level Call, value > 0 but frame status != 0
        IngestTrace {
            status: 1,
            ..top_level_call(4, caller, callee, U256::from(10u64), vec![])
        },
        // 5: nested Call under tx 0 — qualifies, value > 0 success
        nested_call(0, caller, other, U256::from(5u64), vec![]),
        // 6: top-level SelfDestruct, success — qualifies
        IngestTrace {
            value: U256::from(1u64),
            ..base_trace(5, CallKind::SelfDestruct, caller, Some(callee))
        },
    ];

    let blocks = vec![block_with_traces(test_header(1, B256::ZERO), traces)];
    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let resp = service
        .query_transfers(QueryTransfersRequest {
            envelope: ascending_envelope(1, 1, 100),
            filter: TransferFilter::default(),
            relations: TransfersRelations::default(),
        })
        .await
        .expect("query");

    let got: Vec<(u32, CallKind, U256)> = resp
        .transfers
        .iter()
        .map(|t| (t.tx_index, t.typ, t.value))
        .collect();
    assert_eq!(
        got,
        vec![
            (0, CallKind::Call, U256::from(100u64)),
            (0, CallKind::Call, U256::from(5u64)),
            (5, CallKind::SelfDestruct, U256::from(1u64)),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn transfers_filter_is_top_level_true_drops_nested() {
    let caller = addr(1);
    let callee = addr(2);

    let traces = vec![
        top_level_call(0, caller, callee, U256::from(10u64), vec![]),
        nested_call(0, caller, callee, U256::from(5u64), vec![]),
    ];

    let blocks = vec![block_with_traces(test_header(1, B256::ZERO), traces)];
    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let resp = service
        .query_transfers(QueryTransfersRequest {
            envelope: ascending_envelope(1, 1, 100),
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
    let alice = addr(0xaa);
    let bob = addr(0xbb);
    let recipient = addr(0x11);

    let traces = vec![
        top_level_call(0, alice, recipient, U256::from(10u64), vec![]),
        top_level_call(1, bob, recipient, U256::from(10u64), vec![]),
    ];

    let blocks = vec![block_with_traces(test_header(1, B256::ZERO), traces)];
    let store = common::populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let resp = service
        .query_transfers(QueryTransfersRequest {
            envelope: ascending_envelope(1, 1, 100),
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
