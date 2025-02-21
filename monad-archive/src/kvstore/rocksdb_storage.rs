use std::{path::Path, sync::Arc};

use bytes::Bytes;
use eyre::{Context, ContextCompat, Result};
use rocksdb::DB;

use crate::{cli::RocksDbCliArgs, prelude::*};

#[derive(Clone)]
pub struct RocksDbClient {
    pub db: Arc<DB>,
    pub name: String,
}

impl RocksDbClient {
    pub fn new(path: impl AsRef<Path>) -> Result<RocksDbClient> {
        Ok(RocksDbClient {
            db: Arc::new(DB::open_default(path.as_ref())?),
            name: format!(
                "rocksdb://{}",
                path.as_ref()
                    .to_str()
                    .wrap_err_with(|| format!("Path not valid utf-8. {:?}", path.as_ref()))?
            ),
        })
    }
}

impl TryFrom<&RocksDbCliArgs> for RocksDbClient {
    type Error = eyre::Error;

    fn try_from(value: &RocksDbCliArgs) -> std::result::Result<Self, Self::Error> {
        Self::new(&value.db_path)
    }
}

impl KVReader for RocksDbClient {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.db
            .get(key)
            .wrap_err_with(|| format!("Failed to read key from rocks_db: {key}"))
            .map_err(Into::into)
            .map(|opt| opt.map(Into::into))
    }
}

impl KVStore for RocksDbClient {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        self.db.put(key.as_ref(), data).map_err(Into::into)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let prefix_bytes = prefix.as_bytes();

        // Create iterator with prefix seeking
        let iter = self.db.prefix_iterator(prefix_bytes);

        // Collect all matching keys
        for item in iter {
            let (key, _) = item?;
            if let Ok(key_str) = String::from_utf8(key.to_vec()) {
                objects.push(key_str);
            }
        }

        Ok(objects)
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        self.db.delete(key.as_ref()).map_err(Into::into)
    }
}
