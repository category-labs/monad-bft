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
    engine::primary_dir::{
        PrimaryDirBucket, PrimaryDirEntry, PrimaryDirFragment, DIRECTORY_BUCKET_SIZE,
    },
    Address, Bytes, Family, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{chain_header, observed_store::ObservedMetaStore, test_header};

#[tokio::test(flavor = "current_thread")]
async fn ingest_compacts_a_sealed_directory_bucket_when_crossing_the_boundary() {
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
                usize::try_from(DIRECTORY_BUCKET_SIZE - 2).expect("bucket size fits usize"),
            )],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(4)],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 2");

    let bucket = service
        .tables()
        .family(Family::Log)
        .load_bucket(0)
        .await
        .expect("load compacted bucket")
        .expect("bucket should be compacted");
    assert_eq!(
        bucket,
        PrimaryDirBucket {
            entries: vec![
                PrimaryDirEntry {
                    block_number: 1,
                    first_primary_id: 0,
                },
                PrimaryDirEntry {
                    block_number: 2,
                    first_primary_id: DIRECTORY_BUCKET_SIZE - 2,
                },
            ],
            end_primary_id_exclusive: DIRECTORY_BUCKET_SIZE + 2,
        }
    );

    let fragments = service
        .tables()
        .family(Family::Log)
        .load_bucket_fragments(0)
        .await
        .expect("load fragments");
    assert_eq!(
        fragments,
        vec![
            PrimaryDirFragment {
                block_number: 1,
                first_primary_id: 0,
                end_primary_id_exclusive: DIRECTORY_BUCKET_SIZE - 2,
            },
            PrimaryDirFragment {
                block_number: 2,
                first_primary_id: DIRECTORY_BUCKET_SIZE - 2,
                end_primary_id_exclusive: DIRECTORY_BUCKET_SIZE + 2,
            },
        ]
    );

    assert!(
        service
            .tables()
            .family(Family::Log)
            .load_bucket(DIRECTORY_BUCKET_SIZE)
            .await
            .expect("load frontier bucket")
            .is_none(),
        "frontier bucket should still resolve from fragments",
    );
}

fn repeated_logs(count: usize) -> Vec<Log> {
    std::iter::repeat_with(log).take(count).collect()
}

fn log() -> Log {
    Log {
        address: Address::repeat_byte(7),
        data: LogData::new_unchecked(vec![B256::repeat_byte(9)], Bytes::from(vec![1, 2, 3])),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bucket_compaction_uses_open_index_without_prefix_scan() {
    let meta_store = ObservedMetaStore::counting();
    let counters = meta_store.counters.clone();
    let service = MonadChainDataService::new(
        meta_store,
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);

    service
        .ingest_block(FinalizedBlock {
            header: h1,
            logs_by_tx: vec![repeated_logs(
                usize::try_from(DIRECTORY_BUCKET_SIZE - 2).expect("bucket size fits usize"),
            )],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 1");

    let before = counters.snapshot();
    service
        .ingest_blocks(vec![FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(4)],
            txs: Vec::new(),
            traces: vec![],
        }])
        .await
        .expect("ingest block 2");
    let after = counters.snapshot();

    assert_eq!(
        after.2, before.2,
        "hot compaction path should use open-index fragments, not list_prefix",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn empty_families_index_no_directory_fragments() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    service
        .ingest_block(FinalizedBlock {
            header: h1,
            logs_by_tx: vec![vec![log()]],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block");

    let stats = service.open_index_stats();
    assert_eq!(
        stats.directory_keys, 1,
        "only the log family minted ids, so only its open bucket is indexed",
    );
    assert_eq!(
        stats.directory_blocks, 1,
        "empty tx/trace windows mint no ids and index no fragments",
    );
    assert!(
        stats.fragment_value_bytes > 0,
        "the id-producing log family still retains its fragment for hot compaction"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn recovery_after_empty_head_keeps_open_bucket_fragments_for_seal() {
    let meta = InMemoryMetaStore::default();
    let blob = InMemoryBlobStore::default();

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    {
        let service =
            MonadChainDataService::new(meta.clone(), blob.clone(), QueryLimits::UNLIMITED);
        // Block 1 fills most of bucket 0: log ids [0, DIRECTORY_BUCKET_SIZE - 2).
        service
            .ingest_block(FinalizedBlock {
                header: h1,
                logs_by_tx: vec![repeated_logs(
                    usize::try_from(DIRECTORY_BUCKET_SIZE - 2).expect("bucket size fits usize"),
                )],
                txs: Vec::new(),
                traces: vec![],
            })
            .await
            .expect("ingest block 1");
        // Block 2 is empty, so the published head's log window has count 0 and
        // (with empty fragments removed) block 2 writes no directory fragment.
        // Bucket 0 stays open, holding only block 1's fragment.
        service
            .ingest_block(FinalizedBlock {
                header: h2,
                logs_by_tx: vec![vec![]],
                txs: Vec::new(),
                traces: vec![],
            })
            .await
            .expect("ingest block 2");
    }

    // Simulate a restart: a fresh service over the same stores must rebuild the
    // open bucket from durable fragments even though the published head (block 2)
    // is empty — otherwise the seal below would compact an incomplete summary.
    let service = MonadChainDataService::new(meta.clone(), blob.clone(), QueryLimits::UNLIMITED);

    // Block 3 mints 4 ids [DIRECTORY_BUCKET_SIZE - 2, DIRECTORY_BUCKET_SIZE + 2),
    // crossing the bucket-0 boundary and sealing it.
    service
        .ingest_block(FinalizedBlock {
            header: h3,
            logs_by_tx: vec![repeated_logs(4)],
            txs: Vec::new(),
            traces: vec![],
        })
        .await
        .expect("ingest block 3");

    // The sealed summary must still include block 1's fragment that pre-dated the
    // empty-head restart; if recovery had skipped reloading the open bucket, ids
    // [0, DIRECTORY_BUCKET_SIZE - 2) would be missing from the summary entirely.
    let bucket = service
        .tables()
        .family(Family::Log)
        .load_bucket(0)
        .await
        .expect("load compacted bucket")
        .expect("bucket should be sealed");
    assert_eq!(
        bucket,
        PrimaryDirBucket {
            entries: vec![
                PrimaryDirEntry {
                    block_number: 1,
                    first_primary_id: 0,
                },
                PrimaryDirEntry {
                    block_number: 3,
                    first_primary_id: DIRECTORY_BUCKET_SIZE - 2,
                },
            ],
            end_primary_id_exclusive: DIRECTORY_BUCKET_SIZE + 2,
        }
    );
}
