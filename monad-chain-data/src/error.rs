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

use thiserror::Error;

use crate::primitives::limits::LimitExceededKind;

pub type Result<T> = std::result::Result<T, MonadChainDataError>;

#[derive(Debug, Error)]
pub enum MonadChainDataError {
    #[error("backend error: {0}")]
    Backend(String),
    #[error("decode error: {0}")]
    Decode(&'static str),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("missing data: {0}")]
    MissingData(&'static str),
    /// A directory bucket the resolver classified as sealed (its entire 10k
    /// id range lies below the published-head family frontier) had no
    /// compacted summary. Ingestion flushes every sealed bucket's summary in
    /// the same `WriteSession` as the batch, before the publication CAS, so a
    /// sealed bucket without a summary means the ingestion/compaction commit
    /// contract is broken. Surfaced loudly rather than masked by a fragment
    /// scan.
    #[error(
        "sealed primary directory bucket {bucket_start} missing its compacted summary; \
         the ingestion/compaction commit contract is broken"
    )]
    SealedDirectoryBucketMissingSummary { bucket_start: u64 },
    /// A page the open-stream inventory recorded a touch for, in a shard that
    /// fully sealed in this batch, had no compacted bitmap artifact. On a
    /// sealing shard every referenced page must already have a compacted
    /// artifact (fresh in this batch's compacted pages or durable from a prior
    /// batch), so a missing one means the ingestion/compaction commit contract
    /// is broken. Surfaced loudly rather than silently dropped from the
    /// page-count manifest: a page absent from a PRESENT manifest reads as
    /// count 0 on the query side, so on a sealed shard it would be skipped with
    /// zero fetches, silently dropping real matches.
    #[error(
        "sealing shard stream {stream_id} page {page_start_local} missing its compacted bitmap \
         artifact; the ingestion/compaction commit contract is broken"
    )]
    SealedShardPageMissingArtifact {
        stream_id: String,
        page_start_local: u32,
    },
    #[error("limit exceeded ({kind}): max_limit={max_limit}, max_block_range={max_block_range}")]
    LimitExceeded {
        kind: LimitExceededKind,
        max_limit: usize,
        max_block_range: u64,
    },
}
