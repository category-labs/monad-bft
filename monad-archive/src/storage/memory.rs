use std::{collections::HashMap, sync::Arc};

use alloy_primitives::TxHash;
use bytes::Bytes;
use eyre::{OptionExt, Result};
use tokio::sync::Mutex;

use crate::prelude::*;

#[derive(Clone)]
pub struct MemoryStorage {
    pub db: Arc<Mutex<HashMap<String, Bytes>>>,
    pub index: Arc<Mutex<HashMap<TxHash, TxIndexedData>>>,
    pub name: String,
}

impl MemoryStorage {
    pub fn new(name: impl Into<String>) -> MemoryStorage {
        MemoryStorage {
            db: Arc::new(Mutex::new(HashMap::default())),
            index: Arc::new(Mutex::new(HashMap::default())),
            name: name.into(),
        }
    }
}

impl BlobReader for MemoryStorage {
    async fn read(&self, key: &str) -> Result<Bytes> {
        self.db
            .lock()
            .await
            .get(key)
            .ok_or_eyre("Key not found")
            .map(ToOwned::to_owned)
    }
}

impl BlobStore for MemoryStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        self.db.lock().await.insert(key.to_owned(), data.into());
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .db
            .lock()
            .await
            .keys()
            .filter_map(|k| {
                if k.starts_with(prefix) {
                    Some(k.to_owned())
                } else {
                    None
                }
            })
            .collect())
    }
}

impl IndexStoreReader for MemoryStorage {
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
        Ok(self.index.lock().await.get(key).map(ToOwned::to_owned))
    }
}

// NOTE: we're doing blocking io here even though we're in async. This should be moved to
// spawn blocking if this code will be used in rpc
impl IndexStore for MemoryStorage {
    async fn bulk_put(&self, kvs: impl Iterator<Item = TxIndexedData>) -> Result<()> {
        for data in kvs {
            let key = data.tx.tx_hash();
            self.index.lock().await.insert(*key, data);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{
        Receipt, ReceiptEnvelope, ReceiptWithBloom, SignableTransaction, TxEip1559, TxEnvelope,
    };
    use alloy_primitives::{Bloom, Log, B256, U256};
    use alloy_signer::SignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_basic_blob_operations() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        // Test upload and read
        let key = "test-key";
        let data = b"hello world".to_vec();
        storage.upload(key, data.clone()).await?;

        let result = storage.read(key).await?;
        assert_eq!(result, Bytes::from(data));

        // Test non-existent key
        let error = storage.read("non-existent").await.unwrap_err();
        assert!(error.to_string().contains("Key not found"));

        Ok(())
    }

    #[tokio::test]
    async fn test_scan_prefix() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        // Upload test data
        storage.upload("test1", b"data1".to_vec()).await?;
        storage.upload("test2", b"data2".to_vec()).await?;
        storage.upload("other", b"data3".to_vec()).await?;

        // Test scanning with prefix
        let results = storage.scan_prefix("test").await?;
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"test1".to_string()));
        assert!(results.contains(&"test2".to_string()));

        // Test scanning with non-matching prefix
        let results = storage.scan_prefix("xyz").await?;
        assert!(results.is_empty());

        Ok(())
    }

    fn mock_tx(salt: u64) -> TxEnvelope {
        let tx = TxEip1559 {
            nonce: salt,
            gas_limit: 456 + salt,
            max_fee_per_gas: 789,
            max_priority_fee_per_gas: 135,
            ..Default::default()
        };
        let signer = PrivateKeySigner::from_bytes(&B256::from(U256::from(123))).unwrap();
        let sig = signer.sign_hash_sync(&tx.signature_hash()).unwrap();
        let tx = tx.into_signed(sig);
        TxEnvelope::from(tx)
    }

    fn mock_rx() -> ReceiptEnvelope {
        ReceiptEnvelope::Eip1559(ReceiptWithBloom::new(
            Receipt::<Log> {
                logs: vec![],
                status: alloy_consensus::Eip658Value::Eip658(true),
                cumulative_gas_used: 55,
            },
            Bloom::repeat_byte(b'a'),
        ))
    }

    #[tokio::test]
    async fn test_basic_index_operations() -> Result<()> {
        let storage = MemoryStorage::new("test-bucket");

        // Create test transaction data
        let tx = mock_tx(1);
        let tx_hash = *tx.tx_hash(); // Get the actual hash from the transaction

        let test_data = TxIndexedData {
            tx,
            trace: vec![1, 2, 3],
            receipt: mock_rx(),
            header_subset: HeaderSubset {
                block_number: 1,
                base_fee_per_gas: Default::default(),
                ..Default::default()
            },
        };

        // Test bulk put and get
        storage.bulk_put(std::iter::once(test_data.clone())).await?;

        // Test single get
        let result = storage.get(&tx_hash).await?;
        assert!(result.is_some());

        // Test bulk get
        let results = storage.bulk_get(&[tx_hash]).await?;
        assert_eq!(results.len(), 1);
        assert!(results.contains_key(&tx_hash));

        Ok(())
    }

    #[tokio::test]
    async fn test_bucket_name() {
        let name = "test-bucket";
        let storage = MemoryStorage::new(name);
        assert_eq!(storage.bucket_name(), name);
    }
}
