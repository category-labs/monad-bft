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

use std::{collections::BTreeMap, future::Future, pin::Pin};

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
    store::{
        BlobStore, BlobTableId, CacheConfig, CachedBlobTable, CachedKvTable, CachedScannableTable,
        CasOutcome, CasVersion, MetaStore, MetaStoreCas, PublicationCasParams, ScannableTableId,
        TableId, WriteSession,
    },
    txs::TxHashIndexTable,
};

pub struct Tables<M: MetaStore, B: BlobStore> {
    meta_store: M,
    blob_store: B,
    blocks: BlockTables<M>,
    tx_hash_index: TxHashIndexTable<M>,
    families: BTreeMap<Family, FamilyTables<M, B>>,
}

impl<M: MetaStore, B: BlobStore> Tables<M, B> {
    pub fn new(meta_store: M, blob_store: B) -> Self {
        Self::with_cache_config(meta_store, blob_store, CacheConfig::default())
    }

    pub fn with_cache_config(meta_store: M, blob_store: B, cache: CacheConfig) -> Self {
        let mut families = BTreeMap::new();
        families.insert(
            Family::Log,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Log, cache),
        );
        families.insert(
            Family::Tx,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Tx, cache),
        );
        families.insert(
            Family::Trace,
            FamilyTables::new(meta_store.clone(), blob_store.clone(), Family::Trace, cache),
        );
        Self {
            blocks: BlockTables::new(meta_store.clone(), cache),
            tx_hash_index: TxHashIndexTable::new(meta_store.clone(), cache),
            meta_store,
            blob_store,
            families,
        }
    }

    pub fn meta_store(&self) -> &M {
        &self.meta_store
    }

    pub fn blob_store(&self) -> &B {
        &self.blob_store
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

    pub async fn with_writes<F>(&self, f: F) -> Result<()>
    where
        F: for<'s> FnOnce(
            &'s WriteSession<'s, M, B>,
        )
            -> Pin<Box<dyn Future<Output = Result<()>> + Send + 's>>,
    {
        let session = WriteSession::new(self);
        let closure_result = f(&session).await;
        if closure_result.is_err() {
            session.invalidate_populated();
            return closure_result;
        }
        let meta_ops = session.take_meta();
        let blob_ops = session.take_blob();
        let flush = futures::try_join!(
            self.meta_store.apply_writes(meta_ops),
            self.blob_store.apply_writes(blob_ops),
        );
        match flush {
            Ok(_) => Ok(()),
            Err(e) => {
                session.invalidate_populated();
                Err(e)
            }
        }
    }

    pub async fn with_writes_and_cas<F>(
        &self,
        cas: PublicationCasParams,
        f: F,
    ) -> Result<CasOutcome>
    where
        F: for<'s> FnOnce(
            &'s WriteSession<'s, M, B>,
        )
            -> Pin<Box<dyn Future<Output = Result<()>> + Send + 's>>,
    {
        let session = WriteSession::new(self);
        let closure_result = f(&session).await;
        if let Err(e) = closure_result {
            session.invalidate_populated();
            return Err(e);
        }
        let meta_ops = session.take_meta();
        let blob_ops = session.take_blob();
        let flush = futures::try_join!(
            self.meta_store.apply_writes_with_cas(meta_ops, cas),
            self.blob_store.apply_writes(blob_ops),
        );
        match flush {
            Ok((outcome, _)) => {
                if matches!(outcome, CasOutcome::Conflict { .. }) {
                    // CAS conflict means data writes did not land (atomic).
                    // The cache speculatively holds them; drop those entries
                    // so reads observe ground truth.
                    session.invalidate_populated();
                }
                Ok(outcome)
            }
            Err(e) => {
                session.invalidate_populated();
                Err(e)
            }
        }
    }

    pub(crate) fn populate_kv_cache(&self, table: TableId, key: &[u8], value: Bytes) {
        if let Some(c) = self.find_kv_cache(table) {
            c.populate(key, value);
        }
    }

    pub(crate) fn populate_scan_cache(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) {
        if let Some(c) = self.find_scan_cache(table) {
            c.populate(partition, clustering, value);
        }
    }

    pub(crate) fn populate_blob_cache(&self, table: BlobTableId, key: &[u8], value: Bytes) {
        if let Some(c) = self.find_blob_cache(table) {
            c.populate(key, value);
        }
    }

    pub(crate) fn evict_kv_cache(&self, table: TableId, key: &[u8]) {
        if let Some(c) = self.find_kv_cache(table) {
            c.evict(key);
        }
    }

    pub(crate) fn evict_scan_cache(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) {
        if let Some(c) = self.find_scan_cache(table) {
            c.evict(partition, clustering);
        }
    }

    pub(crate) fn evict_blob_cache(&self, table: BlobTableId, key: &[u8]) {
        if let Some(c) = self.find_blob_cache(table) {
            c.evict(key);
        }
    }

    fn find_kv_cache(&self, table: TableId) -> Option<&CachedKvTable<M>> {
        if let Some(c) = self.blocks.find_kv_cache(table) {
            return Some(c);
        }
        if table == TxHashIndexTable::<M>::TABLE {
            return Some(self.tx_hash_index.cached_table());
        }
        for fam in self.families.values() {
            if let Some(c) = fam.find_kv_cache(table) {
                return Some(c);
            }
        }
        None
    }

    fn find_scan_cache(&self, table: ScannableTableId) -> Option<&CachedScannableTable<M>> {
        for fam in self.families.values() {
            if let Some(c) = fam.find_scan_cache(table) {
                return Some(c);
            }
        }
        None
    }

    fn find_blob_cache(&self, table: BlobTableId) -> Option<&CachedBlobTable<B>> {
        for fam in self.families.values() {
            if let Some(c) = fam.find_blob_cache(table) {
                return Some(c);
            }
        }
        None
    }
}

fn block_number_key(block_number: u64) -> [u8; 8] {
    block_number.to_be_bytes()
}

pub struct PublicationTables<M: MetaStoreCas> {
    meta_store: M,
}

impl<M: MetaStoreCas> PublicationTables<M> {
    pub const PUBLICATION_STATE_TABLE: TableId = TableId::new("publication_state");
    pub const PUBLICATION_STATE_KEY: &[u8] = b"state";

    pub(crate) fn new(meta_store: M) -> Self {
        Self { meta_store }
    }

    /// Loads the current publication state along with its CAS version.
    /// Writers must thread the version into the matching [`Self::cas_advance`]
    /// call; readers that only want the head can use [`Self::load_published_head`].
    pub async fn load_state(&self) -> Result<Option<(CasVersion, PublicationState)>> {
        let Some((version, bytes)) = self
            .meta_store
            .cas_get(Self::PUBLICATION_STATE_TABLE, Self::PUBLICATION_STATE_KEY)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some((version, PublicationState::decode(&bytes)?)))
    }

    pub async fn load_published_head(&self) -> Result<Option<u64>> {
        Ok(self
            .load_state()
            .await?
            .map(|(_, state)| state.indexed_finalized_head))
    }

    /// Atomically advances the publication state. `expected` must be the
    /// version returned by the load that produced `next`'s preconditions —
    /// writer+standby failover is the case this guards. On version mismatch
    /// this returns `Err(FencedOut)` rather than [`CasOutcome::Conflict`]
    /// so callers can `?` through ingest paths.
    pub async fn cas_advance(
        &self,
        expected: Option<CasVersion>,
        next: PublicationState,
    ) -> Result<()> {
        let outcome = self
            .meta_store
            .cas_put(
                Self::PUBLICATION_STATE_TABLE,
                Self::PUBLICATION_STATE_KEY,
                expected,
                Bytes::from(next.encode()),
            )
            .await?;
        self.cas_outcome_into_result(outcome).await
    }

    /// Folds a [`CasOutcome`] from a publication-state write into the
    /// service-level `Result`. Centralized so the Phase B path in
    /// `MonadChainDataService::ingest_blocks` and the Phase-B-skipped path
    /// in [`Self::cas_advance`] map `Conflict` to `FencedOut` identically.
    pub(crate) async fn cas_outcome_into_result(&self, outcome: CasOutcome) -> Result<()> {
        match outcome {
            CasOutcome::Applied { .. } => Ok(()),
            CasOutcome::Conflict { .. } => {
                let current_head = self.load_published_head().await?;
                Err(MonadChainDataError::FencedOut { current_head })
            }
        }
    }
}

pub struct BlockTables<M: MetaStore> {
    block_records: CachedKvTable<M>,
    block_headers: CachedKvTable<M>,
    block_hash_to_number_index: CachedKvTable<M>,
}

impl<M: MetaStore> BlockTables<M> {
    pub const BLOCK_RECORD_TABLE: TableId = TableId::new("block_record");
    pub const BLOCK_HEADER_TABLE: TableId = TableId::new("block_header");
    pub const BLOCK_HASH_TO_NUMBER_INDEX_TABLE: TableId =
        TableId::new("block_hash_to_number_index");

    fn new(meta_store: M, cache: CacheConfig) -> Self {
        Self {
            block_records: CachedKvTable::new(
                meta_store.table(Self::BLOCK_RECORD_TABLE),
                cache.block_record_entries,
            ),
            block_headers: CachedKvTable::new(
                meta_store.table(Self::BLOCK_HEADER_TABLE),
                cache.block_header_entries,
            ),
            block_hash_to_number_index: CachedKvTable::new(
                meta_store.table(Self::BLOCK_HASH_TO_NUMBER_INDEX_TABLE),
                cache.block_hash_to_number_entries,
            ),
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

    pub fn stage_record(
        &self,
        meta: &mut M::Batch,
        block_number: u64,
        block_record: &BlockRecord,
    ) {
        let key = block_number_key(block_number);
        self.block_records
            .put_into(meta, &key, Bytes::from(block_record.encode()));
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

    pub fn stage_header(&self, meta: &mut M::Batch, block_number: u64, header: &EvmBlockHeader) {
        let key = block_number_key(block_number);
        self.block_headers
            .put_into(meta, &key, Bytes::from(alloy_rlp::encode(header)));
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

    pub fn stage_hash_index(
        &self,
        meta: &mut M::Batch,
        block_hash: &Hash32,
        block_number: u64,
    ) {
        self.block_hash_to_number_index.put_into(
            meta,
            block_hash.as_slice(),
            Bytes::copy_from_slice(&block_number.to_be_bytes()),
        );
    }

    /// Validates that a new block extends the currently published chain.
    pub(crate) fn find_kv_cache(&self, table: TableId) -> Option<&CachedKvTable<M>> {
        if table == Self::BLOCK_RECORD_TABLE {
            Some(&self.block_records)
        } else if table == Self::BLOCK_HEADER_TABLE {
            Some(&self.block_headers)
        } else if table == Self::BLOCK_HASH_TO_NUMBER_INDEX_TABLE {
            Some(&self.block_hash_to_number_index)
        } else {
            None
        }
    }

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
    block_headers: CachedKvTable<M>,
    block_blobs: CachedBlobTable<B>,
    dir: PrimaryDirTables<M>,
    bitmap: BitmapTables<M>,
}

impl<M: MetaStore, B: BlobStore> FamilyTables<M, B> {
    fn new(meta_store: M, blob_store: B, family: Family, cache: CacheConfig) -> Self {
        let ids = family.table_ids();
        Self {
            block_headers: CachedKvTable::new(
                meta_store.table(ids.block_header),
                cache.block_header_entries,
            ),
            block_blobs: CachedBlobTable::new(
                blob_store.table(ids.block_blob),
                cache.block_blob_entries,
            ),
            dir: PrimaryDirTables::new(
                CachedScannableTable::new(
                    meta_store.scannable_table(ids.dir_by_block),
                    cache.dir_by_block_entries,
                ),
                CachedKvTable::new(meta_store.table(ids.dir_bucket), cache.dir_bucket_entries),
            ),
            bitmap: BitmapTables::new(
                CachedScannableTable::new(
                    meta_store.scannable_table(ids.bitmap_by_block),
                    cache.bitmap_by_block_entries,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_meta),
                    cache.bitmap_page_meta_entries,
                ),
                CachedKvTable::new(
                    meta_store.table(ids.bitmap_page_blob),
                    cache.bitmap_page_blob_entries,
                ),
                CachedScannableTable::new(
                    meta_store.scannable_table(ids.open_bitmap_stream),
                    cache.open_bitmap_stream_entries,
                ),
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

    pub fn stage_block_blob(&self, blob: &mut B::Batch, block_number: u64, block_log_blob: Vec<u8>) {
        let key = block_number_key(block_number);
        self.block_blobs
            .put_into(blob, &key, Bytes::from(block_log_blob));
    }

    pub fn stage_block_header(&self, meta: &mut M::Batch, block_number: u64, bytes: Bytes) {
        let key = block_number_key(block_number);
        self.block_headers.put_into(meta, &key, bytes);
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

    pub(crate) fn find_kv_cache(&self, table: TableId) -> Option<&CachedKvTable<M>> {
        if table == self.block_headers.table_id() {
            Some(&self.block_headers)
        } else if table == self.dir.buckets_cache().table_id() {
            Some(self.dir.buckets_cache())
        } else if table == self.bitmap.page_meta_cache().table_id() {
            Some(self.bitmap.page_meta_cache())
        } else if table == self.bitmap.page_blobs_cache().table_id() {
            Some(self.bitmap.page_blobs_cache())
        } else {
            None
        }
    }

    pub(crate) fn find_scan_cache(
        &self,
        table: ScannableTableId,
    ) -> Option<&CachedScannableTable<M>> {
        if table == self.dir.fragments_cache().table_id() {
            Some(self.dir.fragments_cache())
        } else if table == self.bitmap.fragments_cache().table_id() {
            Some(self.bitmap.fragments_cache())
        } else if table == self.bitmap.open_streams_cache().table_id() {
            Some(self.bitmap.open_streams_cache())
        } else {
            None
        }
    }

    pub(crate) fn find_blob_cache(&self, table: BlobTableId) -> Option<&CachedBlobTable<B>> {
        if table == self.block_blobs.table_id() {
            Some(&self.block_blobs)
        } else {
            None
        }
    }
}
