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
    engine::primary_dir::{PrimaryDirBucket, PrimaryDirFragment, DIRECTORY_BUCKET_SIZE},
    Address, Bytes, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{chain_header, test_header};

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
        })
        .await
        .expect("ingest block 1");

    service
        .ingest_block(FinalizedBlock {
            header: h2,
            logs_by_tx: vec![repeated_logs(4)],
        })
        .await
        .expect("ingest block 2");

    let bucket = service
        .tables()
        .logs()
        .load_bucket(0)
        .await
        .expect("load compacted bucket")
        .expect("bucket should be compacted");
    assert_eq!(
        bucket,
        PrimaryDirBucket {
            start_block: 1,
            first_primary_ids: vec![0, DIRECTORY_BUCKET_SIZE - 2, DIRECTORY_BUCKET_SIZE + 2],
        }
    );

    let fragments = service
        .tables()
        .logs()
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
            .logs()
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
