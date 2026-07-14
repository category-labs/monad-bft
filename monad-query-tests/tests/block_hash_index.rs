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

use monad_query_tests::prelude::*;
#[tokio::test(flavor = "current_thread")]
async fn ingest_indexes_each_block_hash_distinctly() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let hash_1 = h1.hash_slow();
    let hash_2 = h2.hash_slow();
    let hash_3 = h3.hash_slow();

    let blocks: Vec<FinalizedBlock> = [h1, h2, h3].into_iter().map(empty_block).collect();
    let store = populate::populate_via_engine(blocks).await;
    let service = store.reader();

    let block_tables = service.tables().blocks();
    assert_eq!(
        block_tables.block_number_by_hash(&hash_1).await.unwrap(),
        Some(1)
    );
    assert_eq!(
        block_tables.block_number_by_hash(&hash_2).await.unwrap(),
        Some(2)
    );
    assert_eq!(
        block_tables.block_number_by_hash(&hash_3).await.unwrap(),
        Some(3)
    );
    assert_eq!(
        block_tables
            .block_number_by_hash(&B256::repeat_byte(0xFF))
            .await
            .unwrap(),
        None
    );
}
