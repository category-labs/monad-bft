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

//! Page-aware abstraction over the seq_num → BlockId index.
//!
//! Local migration writes one key per seq_num via `KvIndexBackend`. The S3
//! upload phase and the live archiver use a paged backend (added in a later
//! commit) that packs `INDEX_PAGE_SIZE` entries into a single contiguous
//! 32-byte-stride blob per page. Both impls speak the same trait so the
//! indexer doesn't have to care which one is in use.

use enum_dispatch::enum_dispatch;
use eyre::{eyre, Context, Result};
use monad_types::{BlockId, Hash};

use crate::{
    kvstore::{KVReader, KVStore, KVStoreErased, WritePolicy},
    model::bft_paths,
};

#[enum_dispatch]
pub trait IndexBackend {
    async fn get_id(&self, seq_num: u64) -> Result<Option<BlockId>>;
    async fn put_id(&self, seq_num: u64, id: BlockId) -> Result<()>;
    async fn put_ids_batch(&self, entries: &[(u64, BlockId)]) -> Result<()>;
}

#[enum_dispatch(IndexBackend)]
#[derive(Clone)]
pub enum IndexBackendErased {
    KvIndexBackend,
}

#[derive(Clone)]
pub struct KvIndexBackend {
    pub kv: KVStoreErased,
}

impl KvIndexBackend {
    pub fn new(kv: KVStoreErased) -> Self {
        Self { kv }
    }

    fn encode_id(id: BlockId) -> Vec<u8> {
        hex::encode(id.0).into_bytes()
    }

    fn decode_id(key: &str, value: &[u8]) -> Result<BlockId> {
        let bytes = hex::decode(value)
            .wrap_err_with(|| format!("Failed to decode hex block id at {key}"))?;
        let len = bytes.len();
        let arr: [u8; 32] = bytes
            .try_into()
            .map_err(|_| eyre!("Invalid block id length at {key}: expected 32, got {len}"))?;
        Ok(BlockId(Hash(arr)))
    }
}

impl IndexBackend for KvIndexBackend {
    async fn get_id(&self, seq_num: u64) -> Result<Option<BlockId>> {
        let key = bft_paths::index_per_key_path(seq_num);
        let Some(value) = self.kv.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(Self::decode_id(&key, &value)?))
    }

    async fn put_id(&self, seq_num: u64, id: BlockId) -> Result<()> {
        let key = bft_paths::index_per_key_path(seq_num);
        self.kv
            .put(key, Self::encode_id(id), WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }

    async fn put_ids_batch(&self, entries: &[(u64, BlockId)]) -> Result<()> {
        let kvs = entries
            .iter()
            .map(|(seq, id)| (bft_paths::index_per_key_path(*seq), Self::encode_id(*id)));
        self.kv
            .bulk_put(kvs, WritePolicy::AllowOverwrite)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kvstore::memory::MemoryStorage;

    fn block_id(byte: u8) -> BlockId {
        BlockId(Hash([byte; 32]))
    }

    fn fresh_kv_backend() -> KvIndexBackend {
        let kv: KVStoreErased = MemoryStorage::new("idx").into();
        KvIndexBackend::new(kv)
    }

    #[tokio::test]
    async fn get_returns_none_for_missing() {
        let b = fresh_kv_backend();
        assert!(b.get_id(0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn put_then_get_round_trip() {
        let b = fresh_kv_backend();
        let id = block_id(0xAB);
        b.put_id(42, id).await.unwrap();
        assert_eq!(b.get_id(42).await.unwrap(), Some(id));
    }

    #[tokio::test]
    async fn put_ids_batch_writes_all_entries() {
        let b = fresh_kv_backend();
        let entries = vec![(1, block_id(0x01)), (2, block_id(0x02)), (3, block_id(0x03))];
        b.put_ids_batch(&entries).await.unwrap();
        for (seq, expected) in entries {
            assert_eq!(b.get_id(seq).await.unwrap(), Some(expected));
        }
    }

    #[tokio::test]
    async fn put_id_overwrites() {
        let b = fresh_kv_backend();
        b.put_id(7, block_id(0x01)).await.unwrap();
        b.put_id(7, block_id(0x02)).await.unwrap();
        assert_eq!(b.get_id(7).await.unwrap(), Some(block_id(0x02)));
    }
}
