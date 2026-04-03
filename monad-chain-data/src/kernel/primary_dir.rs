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
    error::Result,
    store::{MetaStore, ScannableKvTable},
};

pub const DIRECTORY_SUB_BUCKET_SIZE: u64 = 10_000;

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
}

impl<M: MetaStore> PrimaryDirTables<M> {
    pub fn new(fragments: ScannableKvTable<M>) -> Self {
        Self { fragments }
    }

    /// Writes a directory fragment for the given block into every sub-bucket
    /// its log-ID window overlaps. A single block can span multiple sub-buckets
    /// when its ID range crosses a 10k boundary, so the same fragment is stored
    /// in each one to allow readers to locate it from any covered sub-bucket.
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

        let mut current_sub_bucket_start = sub_bucket_start(first_primary_id);
        let last_sub_bucket_start = if count == 0 {
            current_sub_bucket_start
        } else {
            sub_bucket_start(fragment.end_primary_id_exclusive.saturating_sub(1))
        };

        loop {
            self.put_fragment(current_sub_bucket_start, block_number, &fragment)
                .await?;
            if current_sub_bucket_start == last_sub_bucket_start {
                break;
            }
            current_sub_bucket_start =
                current_sub_bucket_start.saturating_add(DIRECTORY_SUB_BUCKET_SIZE);
        }

        Ok(())
    }

    pub async fn load_sub_bucket_fragments(
        &self,
        sub_bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        let partition = u64_key(sub_bucket_start);
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

    async fn put_fragment(
        &self,
        sub_bucket_start: u64,
        block_number: u64,
        fragment: &PrimaryDirFragment,
    ) -> Result<()> {
        let partition = u64_key(sub_bucket_start);
        let clustering = u64_key(block_number);
        self.fragments
            .put(&partition, &clustering, Bytes::from(fragment.encode()))
            .await?;
        Ok(())
    }
}

pub fn sub_bucket_start(primary_id: u64) -> u64 {
    aligned_u64_start(primary_id, DIRECTORY_SUB_BUCKET_SIZE)
}

fn aligned_u64_start(value: u64, alignment: u64) -> u64 {
    value - (value % alignment)
}

fn u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}
