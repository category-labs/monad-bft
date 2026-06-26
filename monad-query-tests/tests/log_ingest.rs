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
async fn ingest_assigns_contiguous_log_id_windows_across_empty_blocks() {
    let h1 = test_header(1, B256::ZERO);
    let h2 = chain_header(2, &h1);
    let h3 = chain_header(3, &h2);

    let store = populate::populate_via_engine(vec![
        block_with_logs(
            h1,
            vec![vec![
                log(Address::repeat_byte(3), vec![B256::repeat_byte(4)]),
                log(Address::repeat_byte(3), vec![B256::repeat_byte(4)]),
            ]],
        ),
        block_with_logs(h2, vec![vec![]]),
        block_with_logs(
            h3,
            vec![vec![log(
                Address::repeat_byte(3),
                vec![B256::repeat_byte(4)],
            )]],
        ),
    ])
    .await;
    let service = store.reader();

    let service = &service;
    let load_block = |number: u64| async move {
        service
            .tables()
            .blocks()
            .load_record(number)
            .await
            .expect("load block")
            .expect("block record")
    };

    let block_1 = load_block(1).await;
    let block_2 = load_block(2).await;
    let block_3 = load_block(3).await;

    assert_eq!(block_1.logs.first_primary_id, PrimaryId::new(0));
    assert_eq!(block_1.logs.count, 2);
    assert_eq!(block_2.logs.first_primary_id, PrimaryId::new(2));
    assert_eq!(block_2.logs.count, 0);
    assert_eq!(block_3.logs.first_primary_id, PrimaryId::new(2));
    assert_eq!(block_3.logs.count, 1);
}
