use bytes::Bytes;
use eyre::Result;

use super::{memory::MemoryStorage, s3::S3Bucket, KVStoreErased};

/// Enum for object storage implementations that support range reads.
/// This is used by ModelV2 for reading partial data from S3 references.
#[derive(Clone)]
pub enum ObjectStore {
    S3(S3Bucket),
    Memory(MemoryStorage),
}

impl ObjectStore {
    /// Get a range of bytes from an object.
    /// The range is inclusive of start and end positions.
    pub async fn get_range(&self, key: &str, start: i32, end: i32) -> Result<Bytes> {
        match self {
            ObjectStore::S3(s3) => s3.get_range(key, start, end).await,
            ObjectStore::Memory(mem) => mem.get_range(key, start, end).await,
        }
    }
}

impl From<ObjectStore> for KVStoreErased {
    fn from(val: ObjectStore) -> Self {
        match val {
            ObjectStore::S3(s3) => KVStoreErased::S3Bucket(s3),
            ObjectStore::Memory(mem) => KVStoreErased::MemoryStorage(mem),
        }
    }
}
