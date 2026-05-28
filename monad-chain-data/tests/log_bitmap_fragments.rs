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

use monad_chain_data::{
    engine::bitmap::{decode_bitmap_blob, sharded_stream_id, STREAM_PAGE_LOCAL_ID_SPAN},
    Address, Bytes, Family, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataService, QueryLimits, Topic, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_log_bitmap_fragments_for_address_and_topics() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![
                log(
                    Address::repeat_byte(7),
                    vec![B256::repeat_byte(9), B256::repeat_byte(10)],
                ),
                log(Address::repeat_byte(7), vec![B256::repeat_byte(9)]),
            ]],
            txs: Vec::new(),
        })
        .await
        .expect("ingest block 1");

    let addr_stream = sharded_stream_id("addr", Address::repeat_byte(7).as_slice(), 0);
    let addr_fragments = service
        .tables()
        .family(Family::Log)
        .load_bitmap_fragments(&addr_stream, 0)
        .await
        .expect("load address fragments");
    assert_eq!(addr_fragments.len(), 1);
    let addr_bitmap = decode_bitmap_blob(addr_fragments[0].as_ref()).expect("decode address");
    assert_eq!(addr_bitmap.count, 2);
    assert_eq!(addr_bitmap.min_local, 0);
    assert_eq!(addr_bitmap.max_local, 1);
    assert!(addr_bitmap.bitmap.contains(0));
    assert!(addr_bitmap.bitmap.contains(1));

    let topic0_stream = sharded_stream_id("topic0", B256::repeat_byte(9).as_slice(), 0);
    let topic0_fragments = service
        .tables()
        .family(Family::Log)
        .load_bitmap_fragments(&topic0_stream, 0)
        .await
        .expect("load topic0 fragments");
    let topic0_bitmap = decode_bitmap_blob(topic0_fragments[0].as_ref()).expect("decode topic0");
    assert_eq!(topic0_bitmap.count, 2);
    assert!(topic0_bitmap.bitmap.contains(0));
    assert!(topic0_bitmap.bitmap.contains(1));

    let topic1_stream = sharded_stream_id("topic1", B256::repeat_byte(10).as_slice(), 0);
    let topic1_fragments = service
        .tables()
        .family(Family::Log)
        .load_bitmap_fragments(&topic1_stream, 0)
        .await
        .expect("load topic1 fragments");
    let topic1_bitmap = decode_bitmap_blob(topic1_fragments[0].as_ref()).expect("decode topic1");
    assert_eq!(topic1_bitmap.count, 1);
    assert!(topic1_bitmap.bitmap.contains(0));
    assert!(!topic1_bitmap.bitmap.contains(1));
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_log_bitmap_fragments_across_page_boundaries() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    service
        .ingest_block(FinalizedBlock {
            header: h1,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(1),
                vec![B256::repeat_byte(3)],
                usize::try_from(STREAM_PAGE_LOCAL_ID_SPAN - 2).expect("page span fits usize"),
            )],
            txs: Vec::new(),
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(
                Address::repeat_byte(7),
                vec![B256::repeat_byte(9)],
                4,
            )],
            txs: Vec::new(),
        })
        .await
        .expect("ingest block 2");

    let target_stream = sharded_stream_id("addr", Address::repeat_byte(7).as_slice(), 0);
    let first_page = service
        .tables()
        .family(Family::Log)
        .load_bitmap_fragments(&target_stream, 0)
        .await
        .expect("load first page");
    let second_page = service
        .tables()
        .family(Family::Log)
        .load_bitmap_fragments(&target_stream, STREAM_PAGE_LOCAL_ID_SPAN)
        .await
        .expect("load second page");

    let first_page_bitmap = decode_bitmap_blob(first_page[0].as_ref()).expect("decode first page");
    assert_eq!(first_page_bitmap.count, 2);
    assert!(first_page_bitmap
        .bitmap
        .contains(STREAM_PAGE_LOCAL_ID_SPAN - 2));
    assert!(first_page_bitmap
        .bitmap
        .contains(STREAM_PAGE_LOCAL_ID_SPAN - 1));

    let second_page_bitmap =
        decode_bitmap_blob(second_page[0].as_ref()).expect("decode second page");
    assert_eq!(second_page_bitmap.count, 2);
    assert!(second_page_bitmap
        .bitmap
        .contains(STREAM_PAGE_LOCAL_ID_SPAN));
    assert!(second_page_bitmap
        .bitmap
        .contains(STREAM_PAGE_LOCAL_ID_SPAN + 1));
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_empty_block_writes_no_log_bitmap_fragments() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![]],
            txs: Vec::new(),
        })
        .await
        .expect("ingest block 1");

    let addr_stream = sharded_stream_id("addr", Address::repeat_byte(7).as_slice(), 0);
    let fragments = service
        .tables()
        .family(Family::Log)
        .load_bitmap_fragments(&addr_stream, 0)
        .await
        .expect("load fragments");
    assert!(fragments.is_empty());
}

fn repeated_logs(address: Address, topics: Vec<Topic>, count: usize) -> Vec<Log> {
    std::iter::repeat_with(|| log(address, topics.clone()))
        .take(count)
        .collect()
}

fn log(address: Address, topics: Vec<Topic>) -> Log {
    Log {
        address,
        data: LogData::new_unchecked(topics, Bytes::from(vec![1, 2, 3])),
    }
}
