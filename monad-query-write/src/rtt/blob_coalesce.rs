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

//! Row queries must survive block-blob coalescing: when one flush stages more
//! blob bytes than `BLOCK_BLOB_COALESCE_TARGET_BYTES`, the coalescer splits
//! the batch into multiple shared objects (plus possibly a lone trailing op)
//! and rewrites every affected metadata header's physical locator. Single-file
//! coverage elsewhere never crosses the target, so the group-boundary and
//! lone-op paths are exercised only here.

use alloy_consensus::Transaction as _;
use super::prelude::*;

const SENDER: Address = Address::repeat_byte(0xaa);
const RECIPIENT: Address = Address::repeat_byte(0x11);
const BLOCK_COUNT: u64 = 7;
// Big enough that ~2.5 blocks cross the 512 KiB coalesce target, so a flush
// of several blocks produces multiple groups and a lone trailing op.
const CALLDATA_LEN: usize = 200 * 1024;

/// Deterministic zstd-resistant bytes (splitmix-style), distinct per seed so
/// no two blocks' blobs compress or dedupe together.
fn incompressible(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) as u8
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn row_queries_survive_block_blob_coalescing() {
    let mut headers = vec![test_header(1, B256::ZERO)];
    for n in 2..=BLOCK_COUNT {
        headers.push(chain_header(n, headers.last().expect("non-empty")));
    }
    let blocks = headers
        .into_iter()
        .enumerate()
        .map(|(i, header)| {
            block_with_txs(
                header,
                vec![ingest_tx(
                    SENDER,
                    Some(RECIPIENT),
                    incompressible(i as u64 + 1, CALLDATA_LEN),
                )],
            )
        })
        .collect();
    let store = populate::populate_via_engine(blocks).await;
    let service = reader(&store);

    let mut coalesced = 0;
    let mut repointed_past_base = 0;
    for n in 1..=BLOCK_COUNT {
        let header = service
            .tables()
            .family(Family::Tx)
            .load_blob_header(n)
            .await
            .unwrap_or_else(|e| panic!("block {n}: tx blob header unreadable: {e}"))
            .unwrap_or_else(|| panic!("block {n}: tx metadata row missing"));
        assert!(
            !header.is_external(),
            "block {n}: native populate produced an external header"
        );
        if header.physical_key.first() == Some(&b'c') {
            coalesced += 1;
        }
        if header.physical_base_offset > 0 {
            repointed_past_base += 1;
        }

        let response = service
            .query_transactions(QueryTransactionsRequest {
                envelope: ascending_envelope(n, n, 10),
                filter: TxFilter::default(),
                relations: TxsRelations::default(),
            })
            .await
            .unwrap_or_else(|e| panic!("block {n}: query_transactions failed: {e}"));
        assert_eq!(response.txs.len(), 1, "block {n}: expected its single tx");
        let tx = &response.txs[0];
        assert_eq!(tx.block_number, n, "block {n}: wrong block materialized");
        assert_eq!(tx.sender, SENDER, "block {n}: wrong sender materialized");
        assert_eq!(
            tx.envelope.input().as_ref(),
            incompressible(n, CALLDATA_LEN),
            "block {n}: calldata mismatch (rows sliced from wrong blob offsets)"
        );
    }

    // The premise of this test: one flush stages all seven ~200 KiB blobs, so
    // the coalescer must form several shared objects. If these counts drop the
    // fixture sizes no longer cross the coalesce target and the test is
    // silently exercising nothing.
    assert!(
        coalesced >= 4,
        "expected most blocks repointed into shared 'c' objects, got {coalesced}"
    );
    assert!(
        repointed_past_base >= 2,
        "expected later group members at non-zero base offsets, got {repointed_past_base}"
    );
}
