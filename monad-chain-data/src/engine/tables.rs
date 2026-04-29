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

use std::collections::BTreeMap;

use alloy_rlp::Decodable;
use bytes::Bytes;

use crate::{
    engine::{
        bitmap::{BitmapFragmentWrite, BitmapPageMeta, BitmapTables},
        family::Family,
        primary_dir::{PrimaryDirBucket, PrimaryDirFragment, PrimaryDirTables},
    },
    error::{MonadChainDataError, Result},
    family::{FinalizedBlock, Hash32},
    primitives::{
        state::{BlockRecord, PublicationState},
        EvmBlockHeader,
    },
    store::{BlobStore, BlobTable, KvTable, MetaStore, ScannableTableId, TableId},
    txs::TxHashIndexTable,
};

pub struct Tables<M: MetaStore, B: BlobStore> {
    publication: PublicationTables<M>,
    blocks: BlockTables<M>,
    tx_hash_index: TxHashIndexTable<M>,
    families: BTreeMap<Family, FamilyTables<M, B>>,
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        let mut families = BTreeMap::new();
        families.insert(
            Family::Log,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Log),
        );
        families.insert(
            Family::Tx,
            FamilyTables::new(meta_store.clone(), blob_store, Family::Tx),
        );
        Self {
            publication: PublicationTables::new(meta_store.clone()),
            blocks: BlockTables::new(meta_store.clone()),
            tx_hash_index: TxHashIndexTable::new(meta_store),
            families,
        }
    }

    pub fn publication(&self) -> &PublicationTables<M> {
        &self.publication
    }

    pub fn blocks(&self) -> &BlockTables<M> {
        &self.blocks
    }

    pub fn tx_hash_index(&self) -> &TxHashIndexTable<M> {
        &self.tx_hash_index
    }

    /// Returns the table set for a family. Panics if the family was not
    /// registered at construction; the `Family` enum is closed, so missing
    /// a variant here is a programmer error, not a data error.
    pub fn family(&self, family: Family) -> &FamilyTables<M, B> {
        self.families
            .get(&family)
            .expect("family registered at construction")
    }
}

fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub struct PublicationTables<M: MetaStore> {
    publication_state: KvTable<M>,
}

impl<M: MetaStore> PublicationTables<M> {
    pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
    pub const PUBLICATION_STATE_KEY: &[u8] = b"state";

    fn new(meta_store: M) -> Self {
        Self {
            publication_state: meta_store.table(Self::PUBLICATION_STATE_TABLE),
        }
    }

    pub async fn load_published_head(&self) -> Result<Option<u64>> {
        let Some(bytes) = self
            .publication_state
            .get(Self::PUBLICATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(
            PublicationState::decode(&bytes)?.indexed_finalized_head,
        ))
    }

    pub async fn store_state(&self, state: PublicationState) -> Result<()> {
        self.publication_state
            .put(Self::PUBLICATION_STATE_KEY, Bytes::from(state.encode()))
            .await?;
        Ok(())
    }
}

pub struct BlockTables<M: MetaStore> {
    block_records: KvTable<M>,
    block_headers: KvTable<M>,
    block_hash_to_number_index: KvTable<M>,
}

impl<M: MetaStore> BlockTables<M> {
    pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");
    pub const BLOCK_HEADER_TABLE: TableId = TableId::new("block_header");
    pub const BLOCK_HASH_TO_NUMBER_INDEX_TABLE: TableId =
        TableId::new("block_hash_to_number_index");

    fn new(meta_store: M) -> Self {
        Self {
            block_records: meta_store.table(Self::BLOCK_RECORD_TABLE),
            block_headers: meta_store.table(Self::BLOCK_HEADER_TABLE),
            block_hash_to_number_index: meta_store.table(Self::BLOCK_HASH_TO_NUMBER_INDEX_TABLE),
        }
    }

    pub async fn load_record(&self, block_number: u64) -> Result<Option<BlockRecord>> {
        let key = block_number_key(block_number);
        let Some(bytes) = self.block_records.get(&key).await? else {
            return Ok(None);
        };
        Ok(Some(BlockRecord::decode(&bytes)?))
    }

    pub async fn store_record(&self, block_number: u64, block_record: &BlockRecord) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_records
            .put(&key, Bytes::from(block_record.encode()))
            .await?;
        Ok(())
    }

    pub async fn load_header(&self, block_number: u64) -> Result<Option<EvmBlockHeader>> {
        let key = block_number_key(block_number);
        let Some(bytes) = self.block_headers.get(&key).await? else {
            return Ok(None);
        };
        let header = EvmBlockHeader::decode(&mut bytes.as_ref())
            .map_err(|_| MonadChainDataError::Decode("invalid block header rlp"))?;
        Ok(Some(header))
    }

    pub async fn store_header(&self, block_number: u64, header: &EvmBlockHeader) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_headers
            .put(&key, Bytes::from(alloy_rlp::encode(header)))
            .await?;
        Ok(())
    }

    /// Resolves a block hash to its block number via the hash-to-number index.
    /// A returned `Some(n)` is not by itself a guarantee that block `n` is
    /// fully published — the index entry is written before the publication
    /// head advances, so a hash hit may name a block whose record/header is
    /// not yet visible. Callers that turn the number into a record/header
    /// load should expect `MissingData` if `n > published_head` or if the
    /// follow-up loads fail.
    pub async fn block_number_by_hash(&self, block_hash: &Hash32) -> Result<Option<u64>> {
        let Some(value) = self
            .block_hash_to_number_index
            .get(block_hash.as_slice())
            .await?
        else {
            return Ok(None);
        };
        let bytes: [u8; 8] = value
            .as_ref()
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("invalid block_hash_to_number_index value"))?;
        Ok(Some(u64::from_be_bytes(bytes)))
    }

    pub async fn store_hash_index(&self, block_hash: &Hash32, block_number: u64) -> Result<()> {
        self.block_hash_to_number_index
            .put(
                block_hash.as_slice(),
                Bytes::copy_from_slice(&block_number.to_be_bytes()),
            )
            .await?;
        Ok(())
    }

    /// Validates that a new block extends the currently published chain.
    pub async fn validate_continuity(
        &self,
        block: &FinalizedBlock,
        current_head: Option<u64>,
    ) -> Result<Option<BlockRecord>> {
        match current_head {
            None => {
                if block.block_number() != 1 {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "first ingested block must be block 1 in the first pass",
                    ));
                }
                Ok(None)
            }
            Some(head) => {
                if block.block_number() != head + 1 {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "block_number must extend the published head contiguously",
                    ));
                }

                let previous = self.load_record(head).await?.ok_or(
                    crate::error::MonadChainDataError::MissingData("missing previous block record"),
                )?;
                if previous.block_hash != block.parent_hash() {
                    return Err(crate::error::MonadChainDataError::InvalidRequest(
                        "parent_hash must match the previous published block",
                    ));
                }
                Ok(Some(previous))
            }
        }
    }
}

pub struct FamilyTables<M: MetaStore, B: BlobStore> {
    block_headers: KvTable<M>,
    block_blobs: BlobTable<B>,
    dir: PrimaryDirTables<M>,
    bitmap: BitmapTables<M>,
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    fn new(meta_store: M, blob_store: B, family: Family) -> Self {
        let ids = family.table_ids();
        Self {
            block_headers: meta_store.table(ids.block_header),
            block_blobs: blob_store.table(ids.block_blob),
            dir: PrimaryDirTables::new(
                meta_store.scannable_table(ids.dir_by_block),
                meta_store.table(ids.dir_bucket),
            ),
            bitmap: BitmapTables::new(
                meta_store.scannable_table(ids.bitmap_by_block),
                meta_store.table(ids.bitmap_page_meta),
                meta_store.table(ids.bitmap_page_blob),
                meta_store.scannable_table(ids.open_bitmap_stream),
            ),
        }
    }

    pub fn dir(&self) -> &PrimaryDirTables<M> {
        &self.dir
    }

    pub fn bitmap(&self) -> &BitmapTables<M> {
        &self.bitmap
    }

    pub fn bitmap_by_block_table(&self) -> ScannableTableId {
        self.bitmap.fragments_table()
    }

    /// Loads the raw per-block header bytes for this family. The codec is
    /// the consumer's responsibility; the engine treats the value as opaque.
    pub async fn load_block_header(&self, block_number: u64) -> Result<Option<Bytes>> {
        let key = block_number_key(block_number);
        self.block_headers.get(&key).await
    }

    pub async fn store_block_header(&self, block_number: u64, bytes: Bytes) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_headers.put(&key, bytes).await?;
        Ok(())
    }

    pub async fn load_block_blob(
        &self,
        block_number: u64,
    ) -> Result<Option<alloy_primitives::Bytes>> {
        let key = block_number_key(block_number);
        Ok(self.block_blobs.get(&key).await?.map(Into::into))
    }

    pub async fn read_block_blob_range(
        &self,
        block_number: u64,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<bytes::Bytes>> {
        let key = block_number_key(block_number);
        self.block_blobs
            .read_range(&key, start, end_exclusive)
            .await
    }

    pub async fn store_block_blob(&self, block_number: u64, block_log_blob: Vec<u8>) -> Result<()> {
        let key = block_number_key(block_number);
        self.block_blobs
            .put(&key, Bytes::from(block_log_blob))
            .await?;
        Ok(())
    }

    pub async fn load_bucket_fragments(
        &self,
        bucket_start: u64,
    ) -> Result<Vec<PrimaryDirFragment>> {
        self.dir.load_bucket_fragments(bucket_start).await
    }

    pub async fn load_bucket(&self, bucket_start: u64) -> Result<Option<PrimaryDirBucket>> {
        self.dir.load_bucket(bucket_start).await
    }

    pub async fn store_bitmap_fragment(
        &self,
        fragment: &BitmapFragmentWrite,
        block_number: u64,
    ) -> Result<()> {
        self.bitmap.store_fragment(fragment, block_number).await
    }

    pub async fn load_bitmap_fragments(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Vec<Bytes>> {
        self.bitmap
            .load_fragments(stream_id, page_start_local)
            .await
    }

    /// Loads the compacted page metadata for one sealed stream page.
    pub async fn load_bitmap_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<BitmapPageMeta>> {
        self.bitmap
            .load_page_meta(stream_id, page_start_local)
            .await
    }

    pub async fn store_bitmap_page_meta(
        &self,
        stream_id: &str,
        page_start_local: u32,
        meta: &BitmapPageMeta,
    ) -> Result<()> {
        self.bitmap
            .store_page_meta(stream_id, page_start_local, meta)
            .await
    }

    /// Loads the compacted bitmap blob for one sealed stream page.
    pub async fn load_bitmap_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
    ) -> Result<Option<Bytes>> {
        self.bitmap
            .load_page_blob(stream_id, page_start_local)
            .await
    }

    pub async fn store_bitmap_page_blob(
        &self,
        stream_id: &str,
        page_start_local: u32,
        bitmap_blob: Bytes,
    ) -> Result<()> {
        self.bitmap
            .store_page_blob(stream_id, page_start_local, bitmap_blob)
            .await
    }

    /// Loads the open stream inventory for one frontier page.
    pub async fn load_open_bitmap_streams(&self, global_page_start: u64) -> Result<Vec<String>> {
        self.bitmap.load_open_streams(global_page_start).await
    }

    /// Records any newly touched streams in the open inventory for one page.
    ///
    /// This is intentionally append-only in the current slice so replay can
    /// never lose open-stream membership through a partial delete+rewrite.
    pub async fn record_open_bitmap_streams(
        &self,
        global_page_start: u64,
        streams: &std::collections::BTreeSet<String>,
    ) -> Result<()> {
        self.bitmap
            .record_open_streams(global_page_start, streams)
            .await
    }
}
