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

use bytes::Bytes;

use crate::{
    error::{MonadChainDataError, Result},
    store::{KvTable, MetaStore, ScannableKvTable},
};

pub const DIRECTORY_BUCKET_SIZE: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct PrimaryDirBucket {
    pub start_block: u64,
    pub first_primary_ids: Vec<u64>,
}

impl PrimaryDirBucket {
    /// Constructs a bucket after enforcing the sentinel and nondecreasing
    /// invariants. All bucket producers — RLP decode and ingest-side
    /// compaction — funnel through here so the type's invariants live in
    /// one place.
    pub(crate) fn new(start_block: u64, first_primary_ids: Vec<u64>) -> Result<Self> {
        if first_primary_ids.len() < 2 {
            return Err(MonadChainDataError::Decode(
                "primary directory bucket missing sentinel",
            ));
        }
        if first_primary_ids
            .windows(2)
            .any(|window| window[0] > window[1])
        {
            return Err(MonadChainDataError::Decode(
                "primary directory bucket ids must be nondecreasing",
            ));
        }
        Ok(Self {
            start_block,
            first_primary_ids,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let raw: Self = alloy_rlp::decode_exact(bytes)
            .map_err(|_| MonadChainDataError::Decode("invalid primary directory bucket rlp"))?;
        Self::new(raw.start_block, raw.first_primary_ids)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
pub struct PrimaryDirFragment {
    pub block_number: u64,
    pub first_primary_id: u64,
    pub end_primary_id_exclusive: u64,
}

impl PrimaryDirFragment {
    pub fn encode(&self) -> Vec<u8> {
        alloy_rlp::encode(self)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        alloy_rlp::decode_exact(bytes).map_err(|_| {
            crate::error::MonadChainDataError::Decode("invalid primary directory fragment rlp")
        })
    }
}

pub struct PrimaryDirTables<M: MetaStore> {
    fragments: ScannableKvTable<M>,
    buckets: KvTable<M>,
}

impl<M: MetaStore> PrimaryDirTables<M> {
    pub fn new(fragments: ScannableKvTable<M>, buckets: KvTable<M>) -> Self {
        Self { fragments, buckets }
    }

    /// Writes a directory fragment for the given block into every 10k bucket
    /// its log-ID window overlaps. A single block can span multiple buckets
    /// when its ID range crosses a 10k boundary, so the same fragment is stored
    /// in each one to allow readers to locate it from any covered bucket.
    pub async fn persist_block_fragment(
        &self,
        block_number: u64,
        first_primary_id: u64,
        count: u32,
    ) -> Result<()> {
        let fragment = PrimaryDirFragment {
            block_number,
            first_primary_id,
            end_primary_id_exclusive: first_primary_id.saturating_add(u64::from(count)),
        };

        let mut current_bucket_start = bucket_start(first_primary_id);
        let last_bucket_start = if count == 0 {
            current_bucket_start
        } else {
            bucket_start(fragment.end_primary_id_exclusive.saturating_sub(1))
        };

        loop {
            self.put_fragment(current_bucket_start, block_number, &fragment)
                .await?;
            if current_bucket_start == last_bucket_start {
                break;
            }
            current_bucket_start = current_bucket_start.saturating_add(DIRECTORY_BUCKET_SIZE);
        }

        Ok(())
    }

    /// Loads the compacted summary for one sealed 10k bucket.
    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        let key = u64_key(bucket_start);
        let Some(record) = self.buckets.get(&key).await? else {
            return Ok(None);
        };

        Ok(Some(PrimaryDirBucket::decode(&record.value)?))
    }

    /// Loads all retained fragments for one 10k bucket.
    pub async fn load_bucket_fragments(
        &self,
        bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        let partition = u64_key(bucket_start);
        let page = self
            .fragments
            .list_prefix(&partition, &[], None, usize::MAX)
            .await?;
        let mut fragments = Vec::with_capacity(page.keys.len());

        for clustering in page.keys {
            let record = self.fragments.get(&partition, &clustering).await?.ok_or(
                crate::error::MonadChainDataError::MissingData(
                    "missing primary directory fragment",
                ),
            )?;
            fragments.push(PrimaryDirFragment::decode(&record.value)?);
        }

        Ok(fragments)
    }

    pub async fn put_bucket(&self, bucket_start: u64, bucket: &PrimaryDirBucket) -> Result<()> {
        let key = u64_key(bucket_start);
        self.buckets.put(&key, Bytes::from(bucket.encode())).await?;
        Ok(())
    }

    async fn put_fragment(
        &self,
        bucket_start: u64,
        block_number: u64,
        fragment: &PrimaryDirFragment,
    ) -> Result<()> {
        let partition = u64_key(bucket_start);
        let clustering = u64_key(block_number);
        self.fragments
            .put(&partition, &clustering, Bytes::from(fragment.encode()))
            .await?;
        Ok(())
    }
}

pub fn bucket_start(primary_id: u64) -> u64 {
    aligned_u64_start(primary_id, DIRECTORY_BUCKET_SIZE)
}

fn aligned_u64_start(value: u64, alignment: u64) -> u64 {
    value - (value % alignment)
}

fn u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}
