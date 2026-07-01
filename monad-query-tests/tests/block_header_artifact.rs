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

use alloy_primitives::{aliases::B64, Address, Bloom, Bytes as AlloyBytes, U256};
use monad_query_tests::prelude::*;
#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_block_header() {
    let mut header = test_header(1, B256::ZERO);
    header.beneficiary = Address::repeat_byte(0xAB);
    header.gas_limit = 30_000_000;
    header.gas_used = 21_000;
    header.timestamp = 1_700_000_000;
    header.extra_data = AlloyBytes::from_static(b"monad-test");

    let store = populate::populate_via_engine(vec![empty_block(header.clone())]).await;
    let service = store.reader();

    let loaded = service
        .tables()
        .blocks()
        .load_header(1)
        .await
        .expect("load header")
        .expect("header present");

    assert_eq!(loaded, header);
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_writes_no_legacy_tables() {
    let store = populate::populate_via_engine(vec![empty_block(test_header(1, B256::ZERO))]).await;

    let kv = store.meta.kv_snapshot();
    assert!(kv
        .keys()
        .any(|(table, _)| *table == TableId::new("block_metadata")));
    for legacy_table in [
        "block_record",
        "block_header",
        "log_block_header",
        "tx_block_header",
        "trace_block_header",
    ] {
        assert!(
            !kv.keys()
                .any(|(table, _)| *table == TableId::new(legacy_table)),
            "ingest should not write legacy {legacy_table} rows"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn load_header_returns_none_for_missing_block() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let missing = service
        .tables()
        .blocks()
        .load_header(42)
        .await
        .expect("load_header");

    assert!(missing.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn block_header_roundtrips_with_all_optional_fields_populated() {
    let header = EvmBlockHeader {
        parent_hash: B256::ZERO,
        ommers_hash: B256::repeat_byte(2),
        beneficiary: Address::repeat_byte(3),
        state_root: B256::repeat_byte(4),
        transactions_root: B256::repeat_byte(5),
        receipts_root: B256::repeat_byte(6),
        logs_bloom: Bloom::repeat_byte(7),
        difficulty: U256::from(0x1234u64),
        number: 1,
        gas_limit: 30_000_000,
        gas_used: 15_000_000,
        timestamp: 1_700_000_000,
        extra_data: AlloyBytes::from_static(&[1, 2, 3, 4]),
        mix_hash: B256::repeat_byte(8),
        nonce: B64::repeat_byte(9),
        base_fee_per_gas: Some(7u64.pow(10)),
        withdrawals_root: Some(B256::repeat_byte(10)),
        blob_gas_used: Some(131_072),
        excess_blob_gas: Some(262_144),
        parent_beacon_block_root: Some(B256::repeat_byte(11)),
        requests_hash: Some(B256::repeat_byte(12)),
        block_access_list_hash: Some(B256::repeat_byte(13)),
        slot_number: Some(14),
    };

    let store = populate::populate_via_engine(vec![empty_block(header.clone())]).await;
    let service = store.reader();

    let loaded = service
        .tables()
        .blocks()
        .load_header(1)
        .await
        .expect("load header")
        .expect("header present");

    assert_eq!(loaded, header);
}
