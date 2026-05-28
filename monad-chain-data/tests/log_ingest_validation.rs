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
    Bytes, Family, FinalizedBlock, InMemoryBlobStore, InMemoryMetaStore, Log, LogData,
    MonadChainDataError, MonadChainDataService, QueryLimits, B256,
};

mod common;

use common::test_header;

#[tokio::test(flavor = "current_thread")]
async fn ingest_rejects_log_with_more_than_four_topics_before_writing_log_artifacts() {
    let service = MonadChainDataService::new(
        InMemoryMetaStore::default(),
        InMemoryBlobStore::default(),
        QueryLimits::UNLIMITED,
    );

    let err = service
        .ingest_block(FinalizedBlock {
            header: test_header(1, B256::ZERO),
            logs_by_tx: vec![vec![Log {
                address: Default::default(),
                data: LogData::new_unchecked(
                    vec![
                        B256::repeat_byte(1),
                        B256::repeat_byte(2),
                        B256::repeat_byte(3),
                        B256::repeat_byte(4),
                        B256::repeat_byte(5),
                    ],
                    Bytes::new(),
                ),
            }]],
            txs: Vec::new(),
        })
        .await
        .expect_err("log with too many topics should fail ingest");

    assert!(
        matches!(
            err,
            MonadChainDataError::InvalidRequest("log topics exceed 4")
        ),
        "expected invalid request for topic count, got {err:?}"
    );

    let log_family = service.tables().family(Family::Log);
    assert!(log_family
        .load_block_header(1)
        .await
        .expect("load log header")
        .is_none());
    assert!(log_family
        .load_block_blob(1)
        .await
        .expect("load log blob")
        .is_none());
}
