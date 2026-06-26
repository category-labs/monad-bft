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

//! Shared error and result types for the queryX (chain-data) crate family.

use thiserror::Error;

pub type Result<T> = std::result::Result<T, MonadChainDataError>;

#[derive(Debug, Error)]
pub enum MonadChainDataError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("decode error: {0}")]
    Decode(&'static str),
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("invalid block: {0}")]
    InvalidBlock(&'static str),
    #[error("missing data: {0}")]
    MissingData(&'static str),
    /// A sealed directory bucket (entire id range below the family frontier)
    /// had no compacted summary. Ingestion flushes sealed-bucket summaries
    /// before publishing the head, so this means the ingestion/compaction
    /// commit contract is broken; surfaced loudly rather than masked by a
    /// fragment scan.
    #[error(
        "sealed primary directory bucket {bucket_start} missing its compacted summary; \
         the ingestion/compaction commit contract is broken"
    )]
    SealedDirectoryBucketMissingSummary { bucket_start: u64 },
    /// A sealed page named by the open page group's stream inventory had no
    /// compacted bitmap artifact (recovery's page-count rebuild): the
    /// ingestion/compaction commit contract is broken. Surfaced loudly rather
    /// than omitted from the page-count manifest, where the page would read
    /// as count 0 and silently drop real matches.
    #[error(
        "sealed stream {stream_id} page {page_start} missing its compacted bitmap \
         artifact; the ingestion/compaction commit contract is broken"
    )]
    SealedPageMissingArtifact { stream_id: String, page_start: u64 },
    /// The primary directory resolved an id through its inventory, but the
    /// backing store (a compacted bucket or the open-page fragments) did not
    /// contain it: the ingestion/compaction commit contract is broken.
    #[error(
        "primary directory {backing} missing queried id {id}; \
         the ingestion/compaction commit contract is broken"
    )]
    PrimaryDirectoryMissingId { id: u64, backing: &'static str },
    #[error("limit exceeded ({kind}): max_limit={max_limit}, max_block_range={max_block_range}")]
    LimitExceeded {
        kind: LimitExceededKind,
        max_limit: usize,
        max_block_range: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitExceededKind {
    Limit,
    BlockRange,
}

impl std::fmt::Display for LimitExceededKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Limit => f.write_str("limit"),
            Self::BlockRange => f.write_str("block range"),
        }
    }
}
