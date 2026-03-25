use std::marker::PhantomData;

use bytes::Bytes;

use crate::{
    error::Result,
    kernel::{
        cache::{HashMapTableBytesCache, TableCacheMetrics},
        codec::StorageCodec,
    },
    store::traits::{KvTable, MetaStore, PutCond},
};

pub trait TableValueCodec: StorageCodec {}

impl<T: StorageCodec> TableValueCodec for T {}

#[derive(Clone)]
pub struct CachedPointTable<M: MetaStore, V> {
    table: KvTable<M>,
    cache: HashMapTableBytesCache,
    _marker: PhantomData<V>,
}

impl<M: MetaStore, V> CachedPointTable<M, V> {
    pub fn new(table: KvTable<M>, cache: HashMapTableBytesCache) -> Self {
        Self {
            table,
            cache,
            _marker: PhantomData,
        }
    }

    pub fn metrics(&self) -> TableCacheMetrics {
        self.cache.metrics_snapshot()
    }
}

impl<M: MetaStore, V: TableValueCodec> CachedPointTable<M, V> {
    pub async fn get_decoded(&self, key: &[u8]) -> Result<Option<V>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(V::decode(&bytes)?));
        }

        let Some(record) = self.table.get(key).await? else {
            return Ok(None);
        };
        self.cache
            .put(key, record.value.clone(), record.value.len());
        Ok(Some(V::decode(&record.value)?))
    }

    pub async fn put_encoded(&self, key: &[u8], value: &V) -> Result<()> {
        let encoded = value.encode();
        let _ = self.table.put(key, encoded.clone(), PutCond::Any).await?;
        self.cache.put(key, encoded.clone(), encoded.len());
        Ok(())
    }
}

impl<M: MetaStore> CachedPointTable<M, Bytes> {
    pub async fn get_bytes(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(bytes) = self.cache.get(key) {
            return Ok(Some(bytes));
        }

        let Some(record) = self.table.get(key).await? else {
            return Ok(None);
        };
        self.cache
            .put(key, record.value.clone(), record.value.len());
        Ok(Some(record.value))
    }

    pub async fn put_bytes(&self, key: &[u8], value: Bytes) -> Result<()> {
        let _ = self.table.put(key, value.clone(), PutCond::Any).await?;
        self.cache.put(key, value.clone(), value.len());
        Ok(())
    }
}
