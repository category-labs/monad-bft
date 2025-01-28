use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use alloy_primitives::{hex::ToHexExt, TxHash};
use alloy_rlp::{Decodable, Encodable};
use bytes::Bytes;
use eyre::Result;
use tokio::fs;

use crate::prelude::*;

#[derive(Clone)]
pub struct FsStorage {
    pub blob: PathBuf,
    pub index: PathBuf,
    pub name: String,
}

impl FsStorage {
    pub async fn new(path: PathBuf) -> Result<FsStorage> {
        let name = path.to_string_lossy().to_string();
        let blob = path.join("blob");
        let index = path.join("index");
        fs::create_dir_all(&blob).await?;
        fs::create_dir_all(&index).await?;
        Ok(FsStorage { blob, index, name })
    }
}

impl BlobReader for FsStorage {
    async fn read(&self, key: &str) -> Result<Bytes> {
        Ok(fs::read(self.blob.join(key)).await?.into())
    }
}

impl BlobStore for FsStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        fs::write(self.blob.join(key), data)
            .await
            .map_err(Into::into)
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let keys = recursive_all_keys(&self.blob, "".as_ref()).await?;

        Ok(keys
            .into_iter()
            .filter_map(|key| {
                if key.starts_with(prefix) {
                    key.to_str().map(ToOwned::to_owned)
                } else {
                    None
                }
            })
            .collect())
    }
}

async fn recursive_all_keys(base: &Path, path: &Path) -> Result<Vec<PathBuf>> {
    let mut read_dir = fs::read_dir(path).await?;
    let mut keys = Vec::with_capacity(128);
    while let Some(entry) = read_dir.next_entry().await? {
        let meta = entry.metadata().await?;
        if meta.is_dir() {
            // NOTE: box pin needed bc of recursion
            let mut future = Box::pin(recursive_all_keys(base, &entry.path())).await?;
            keys.append(&mut future);
        } else {
            let key = entry.path();
            keys.push(key.strip_prefix(base)?.to_path_buf());
        }
    }

    Ok(keys)
}

impl IndexStoreReader for FsStorage {
    async fn bulk_get(&self, keys: &[TxHash]) -> Result<HashMap<TxHash, TxIndexedData>> {
        // NOTE: This is an unfortunate amount of cloning going on...
        let mut results = HashMap::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(key).await? {
                results.insert(*key, value);
            }
        }
        Ok(results)
    }

    async fn get(&self, key: &TxHash) -> Result<Option<TxIndexedData>> {
        let key = key.encode_hex();
        let path = self.index.join(key);
        if !fs::try_exists(&path).await? {
            return Ok(None);
        }
        let data = fs::read(&path).await?;
        let data = TxIndexedData::decode(&mut data.as_slice())?;
        Ok(Some(data))
    }
}

// NOTE: we're doing blocking io here even though we're in async. This should be moved to
// spawn blocking if this code will be used in rpc
impl IndexStore for FsStorage {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()> {
        let mut buf = Vec::with_capacity(1024);
        for data in kvs {
            let key = data.tx.tx.tx_hash().encode_hex();
            buf.clear();
            data.encode(&mut buf);
            fs::write(self.index.join(key), &buf).await?;
        }
        Ok(())
    }
}
