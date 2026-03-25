use bytes::Bytes;

use crate::{
    error::Result,
    kernel::cache::HashMapTableBytesCache,
    store::traits::{BlobStore, BlobTable},
};

pub struct CachedBlobTable<B: BlobStore> {
    blob_table: BlobTable<B>,
    pub cache: HashMapTableBytesCache,
}

impl<B: BlobStore> CachedBlobTable<B> {
    pub fn new(blob_table: BlobTable<B>, cache: HashMapTableBytesCache) -> Self {
        Self { blob_table, cache }
    }

    pub async fn get_by_key(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }
        let Some(bytes) = self.blob_table.get(key).await? else {
            return Ok(None);
        };
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(Some(bytes))
    }

    pub async fn put_by_key(&self, key: &[u8], bytes: Bytes) -> Result<()> {
        self.blob_table.put(key, bytes.clone()).await?;
        self.cache.put(key, bytes.clone(), bytes.len());
        Ok(())
    }
}
