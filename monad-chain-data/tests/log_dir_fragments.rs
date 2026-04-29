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
    kernel::primary_dir::{PrimaryDirFragment, DIRECTORY_BUCKET_SIZE},
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataService, B256,
};

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_log_dir_fragment_for_single_bucket_block() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![log(), log()]],
        })
        .await
        .expect("ingest block 1");

    let fragments = service
        .tables()
        .logs()
        .load_bucket_fragments(0)
        .await
        .expect("load fragments");

    assert_eq!(
        fragments,
        vec![PrimaryDirFragment {
            block_number: 1,
            first_primary_id: 0,
            end_primary_id_exclusive: 2,
        }]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_zero_width_fragment_for_empty_block() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![vec![]],
        })
        .await
        .expect("ingest block 1");

    let fragments = service
        .tables()
        .logs()
        .load_bucket_fragments(0)
        .await
        .expect("load fragments");

    assert_eq!(
        fragments,
        vec![PrimaryDirFragment {
            block_number: 1,
            first_primary_id: 0,
            end_primary_id_exclusive: 0,
        }]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_spanning_fragment_in_each_covered_bucket() {
    let service =
        MonadChainDataService::new(InMemoryMetaStore::default(), InMemoryBlobStore::default());

    service
        .ingest_block(FinalizedBlock {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            parent_hash: B256::ZERO,
            logs_by_tx: vec![repeated_logs(
                usize::try_from(DIRECTORY_BUCKET_SIZE - 2).expect("bucket size fits usize"),
            )],
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            block_number: 2,
            block_hash: B256::repeat_byte(2),
            parent_hash: B256::repeat_byte(1),
            logs_by_tx: vec![repeated_logs(4)],
        })
        .await
        .expect("ingest block 2");

    let first_partition = service
        .tables()
        .logs()
        .load_bucket_fragments(0)
        .await
        .expect("load first partition");
    let second_partition = service
        .tables()
        .logs()
        .load_bucket_fragments(DIRECTORY_BUCKET_SIZE)
        .await
        .expect("load second partition");

    let expected = PrimaryDirFragment {
        block_number: 2,
        first_primary_id: DIRECTORY_BUCKET_SIZE - 2,
        end_primary_id_exclusive: DIRECTORY_BUCKET_SIZE + 2,
    };

    assert!(first_partition.contains(&expected));
    assert_eq!(second_partition, vec![expected]);
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
