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
    FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::{chain_header, test_header};

#[tokio::test(flavor = "current_thread")]
async fn ingest_persists_block_hash_index_entry() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let header = test_header(1, B256::ZERO);
    let block_hash = header.hash_slow();

    service
        .ingest_block(FinalizedBlock {
            header,
            logs_by_tx: vec![vec![]],
        })
        .await
        .expect("ingest block 1");

    let resolved = service
        .tables()
        .blocks()
        .block_number_by_hash(&block_hash)
        .await
        .expect("lookup");

    assert_eq!(resolved, Some(1));
}

#[tokio::test(flavor = "current_thread")]
async fn lookup_returns_none_for_unknown_hash() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let missing = service
        .tables()
        .blocks()
        .block_number_by_hash(&B256::repeat_byte(0xFF))
        .await
        .expect("lookup");

    assert!(missing.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn multi_block_chain_indexes_each_hash_distinctly() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let hash_1 = h1.hash_slow();
    let hash_2 = h2.hash_slow();
    let hash_3 = h3.hash_slow();

    for header in [h1, h2, h3] {
        service
            .ingest_block(FinalizedBlock {
                header,
                logs_by_tx: vec![vec![]],
            })
            .await
            .expect("ingest");
    }

    let blocks = service.tables().blocks();
    assert_eq!(blocks.block_number_by_hash(&hash_1).await.unwrap(), Some(1));
    assert_eq!(blocks.block_number_by_hash(&hash_2).await.unwrap(), Some(2));
    assert_eq!(blocks.block_number_by_hash(&hash_3).await.unwrap(), Some(3));
}
