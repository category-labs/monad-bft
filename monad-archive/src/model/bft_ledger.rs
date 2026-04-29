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

use alloy_rlp::{Decodable, Encodable};
use monad_consensus_types::{block::ConsensusBlockHeader, payload::ConsensusBlockBody};
use monad_node_config::{ExecutionProtocolType, SignatureCollectionType, SignatureType};
use monad_types::{BlockId, Hash};

use crate::{
    kvstore::WritePolicy,
    model::{
        bft_paths,
        index_backend::{IndexBackend, IndexBackendErased, KvIndexBackend},
    },
    prelude::*,
};

pub type BftBlockBody = ConsensusBlockBody<ExecutionProtocolType>;
pub type BftBlockHeader =
    ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

#[derive(Clone)]
pub struct BftBlockModel {
    pub kv: KVStoreErased,
    pub index: IndexBackendErased,
}

impl BftBlockModel {
    /// Build a model whose index uses the same kv as headers/bodies via the
    /// per-key local layout. Suitable for the live archiver and the migration
    /// indexer.
    pub fn new(kv: KVStoreErased) -> Self {
        let index: IndexBackendErased = KvIndexBackend::new(kv.clone()).into();
        Self::with_index(kv, index)
    }

    /// Build a model with an explicitly chosen index backend (e.g. paged for
    /// the S3 upload phase).
    pub fn with_index(kv: KVStoreErased, index: IndexBackendErased) -> Self {
        Self { kv, index }
    }

    pub async fn get_id_by_num(&self, num: u64) -> Result<BlockId> {
        self.get_id_by_num_opt(num)
            .await?
            .ok_or_eyre("Block number not found")
    }

    pub async fn get_id_by_num_opt(&self, num: u64) -> Result<Option<BlockId>> {
        self.index.get_id(num).await
    }

    pub async fn put_index(&self, seq_num: u64, header_id: &BlockId) -> Result<()> {
        self.index.put_id(seq_num, *header_id).await
    }

    pub async fn get_header_by_id(&self, id: &BlockId) -> Result<BftBlockHeader> {
        let key = bft_paths::header_path(id);
        let value = self.kv.get(&key).await?.ok_or_eyre("Header not found")?;
        BftBlockHeader::decode(&mut &value[..]).wrap_err("Failed to decode header")
    }

    pub async fn put_header_bytes(&self, id: &BlockId, bytes: &[u8]) -> Result<()> {
        let key = bft_paths::header_path(id);
        self.kv
            .put(key, bytes.to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    pub async fn put_header(&self, header: &BftBlockHeader) -> Result<()> {
        let id = header.get_id();
        let mut buf = Vec::new();
        header.encode(&mut buf);
        self.put_header_bytes(&id, &buf).await
    }

    pub async fn get_body_by_id(&self, id: &BlockId) -> Result<BftBlockBody> {
        let key = bft_paths::body_path(&id.0);
        let value = self.kv.get(&key).await?.ok_or_eyre("Body not found")?;
        BftBlockBody::decode(&mut &value[..]).wrap_err("Failed to decode body")
    }

    pub async fn put_body(&self, id: &Hash, body: &BftBlockBody) -> Result<()> {
        let mut buf = Vec::new();
        body.encode(&mut buf);
        self.put_body_bytes(id, &buf).await
    }

    pub async fn put_body_bytes(&self, id: &Hash, bytes: &[u8]) -> Result<()> {
        let key = bft_paths::body_path(id);
        self.kv
            .put(key, bytes.to_vec(), WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    pub async fn get_latest_uploaded(&self) -> Result<Option<u64>> {
        let key = bft_paths::latest_uploaded_key();
        let Some(bytes) = self.kv.get(key).await? else {
            return Ok(None);
        };
        let arr: [u8; 8] = bytes.as_ref().try_into().map_err(|_| {
            eyre!(
                "Invalid latest_uploaded value length at key {key}: expected 8, got {}",
                bytes.len()
            )
        })?;
        Ok(Some(u64::from_be_bytes(arr)))
    }

    pub async fn set_latest_uploaded(&self, latest_uploaded: u64) -> Result<()> {
        self.kv
            .put(
                bft_paths::latest_uploaded_key(),
                latest_uploaded.to_be_bytes().to_vec(),
                WritePolicy::AllowOverwrite,
            )
            .await?;
        Ok(())
    }
}
