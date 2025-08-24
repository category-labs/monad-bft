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

use bytes::Bytes;
use eyre::{Context, Result};
use mongodb::{
    bson::{doc, Binary},
    options::{ClientOptions, CollectionOptions, WriteConcern},
    Client, Collection,
};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    kvstore::{KVStoreType, MetricsResultExt},
    prelude::*,
};

const MAX_CONNECTION_POOL_SIZE: u32 = 50;
const CHUNK_SIZE: usize = 1024 * 1024 * 15; // 15MB
const DEFAULT_MAX_TIME_SECS: u64 = 5;

#[derive(Clone)]
pub struct MongoDbStorage {
    pub client: Client,
    pub(crate) collection: Collection<KeyValueDocument>,
    pub db_name: String,
    pub max_time_get: Duration,
    name: String,
    metrics: Metrics,
}

#[derive(Serialize, Deserialize)]
pub struct KeyValueDocument {
    pub _id: String,
    pub value: Option<Binary>,
    /// If the size of value is above CHUNK_SIZE, data is stored in `chunks` documents with _id's {id}_chunk_{chunk_idx}
    pub chunks: Option<u32>,
}

impl KeyValueDocument {
    pub async fn resolve(
        self,
        collection: &Collection<KeyValueDocument>,
    ) -> Result<(String, Bytes)> {
        match (self.value, self.chunks) {
            (Some(value), None) => Ok((self._id, Bytes::from(value.bytes))),
            (None, Some(chunks)) => {
                info!(num_chunks = chunks, "Resolving chunks for {}", self._id);
                let keys = (0..chunks)
                    .map(|chunk_num| chunk_id(&self._id, chunk_num))
                    .collect::<Vec<_>>();

                let mut chunks = collection
                    .find(doc! { "_id": {"$in": &keys} })
                    .await?
                    .map(|x| {
                        let doc = x.wrap_err("Failed to get chunk")?;
                        eyre::Ok((doc._id, doc.value.wrap_err("Chunk has no value")?.bytes))
                    })
                    .try_collect::<HashMap<String, Vec<u8>>>()
                    .await?;

                info!(
                    num_chunks = chunks.len(),
                    "Resolved chunks for {}", self._id
                );

                // Sadly we didn't use zero padded chunk ids, so we need to sort the results by using the original keys
                let mut bytes = Vec::with_capacity(chunks.len() * CHUNK_SIZE);
                for key in keys {
                    let Some(chunk) = chunks.remove(&key) else {
                        return Err(eyre!("Chunk not found for key: {key}"));
                    };
                    bytes.extend_from_slice(&chunk);
                }
                // let bytes = chunks.values().flatten().cloned().collect::<Vec<_>>();

                Ok((self._id, Bytes::from(bytes)))
            }
            _ => unreachable!("KeyValueDocument should either have value or chunks"),
        }
    }
}

fn chunk_id(id: &str, chunk_idx: u32) -> String {
    format!("{}_chunk_{}", id, chunk_idx)
}

pub async fn new_client(connection_string: &str) -> Result<Client> {
    let mut client_options = ClientOptions::parse(connection_string).await?;
    client_options.max_pool_size = Some(MAX_CONNECTION_POOL_SIZE);
    client_options.connect_timeout = Some(Duration::from_secs(1));

    let client = Client::with_options(client_options)?;
    trace!("MongoDB client created successfully");
    Ok(client)
}

impl MongoDbStorage {
    pub async fn new_index_store(
        connection_string: &str,
        database: &str,
        metrics: Metrics,
    ) -> Result<Self> {
        trace!(
            "Creating MongoDB index store with connection: {}, database: {}",
            connection_string,
            database
        );
        Self::new(connection_string, database, "tx_index", metrics).await
    }

    pub async fn new_block_store(
        connection_string: &str,
        database: &str,
        metrics: Metrics,
    ) -> Result<Self> {
        trace!(
            "Creating MongoDB block store with connection: {}, database: {}",
            connection_string,
            database
        );
        Self::new(connection_string, database, "block_level", metrics).await
    }

    pub async fn new(
        connection_string: &str,
        database: &str,
        collection_name: &str,
        metrics: Metrics,
    ) -> Result<Self> {
        info!(
            "Initializing MongoDB connection to {}/{}",
            connection_string, database
        );

        let client = new_client(connection_string).await?;

        let db = client.database(database);

        let collection_exists = db
            .list_collection_names()
            .await?
            .contains(&collection_name.to_string());

        if !collection_exists {
            info!("Collection '{}' not found, creating...", collection_name);

            // Create capped collection if it doesn't exist
            db.create_collection(collection_name).await?;

            info!("Collection '{}' created successfully", collection_name);
        } else {
            info!("Collection '{}' already exists", collection_name);
        }

        // Ensure writes are journaled before returning
        debug!("Configuring collection with journaled write concern");
        let collection = db.collection_with_options(
            collection_name,
            CollectionOptions::builder()
                .write_concern(Some(WriteConcern::builder().journal(Some(true)).build()))
                .build(),
        );

        let storage = Self {
            client,
            collection,
            db_name: database.to_string(),
            name: format!("mongodb://{database}/{collection_name}"),
            metrics,
            max_time_get: Duration::from_secs(DEFAULT_MAX_TIME_SECS),
        };

        info!("MongoDB storage initialized: {}", storage.name);
        Ok(storage)
    }
}

impl KVReader for MongoDbStorage {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let start = Instant::now();
        match self
            .collection
            .find_one(doc! { "_id": key })
            .max_time(self.max_time_get)
            .await
            .wrap_err("MongoDB get operation failed")
            .write_get_metrics_on_err(start.elapsed(), KVStoreType::Mongo, &self.metrics)?
        {
            Some(doc) => doc
                .resolve(&self.collection)
                .await
                .map(|x| Some(x.1))
                .write_get_metrics(start.elapsed(), KVStoreType::Mongo, &self.metrics),

            None => Ok(None).write_get_metrics(start.elapsed(), KVStoreType::Mongo, &self.metrics),
        }
    }

    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, Bytes>> {
        let start = Instant::now();
        let find_result = self
            .collection
            .find(doc! { "_id": {"$in": keys} })
            .max_time(self.max_time_get)
            .await
            .write_get_metrics(start.elapsed(), KVStoreType::Mongo, &self.metrics)
            .wrap_err("MongoDB get operation failed")?
            .map(|x| x.wrap_err("MongoDB get operation failed"));

        find_result
            .and_then(|x| async { x.resolve(&self.collection).await })
            .try_collect::<HashMap<String, Bytes>>()
            .await
            .wrap_err("MongoDB bulk_get operation failed")
            .write_get_metrics(start.elapsed(), KVStoreType::Mongo, &self.metrics)
    }
}

impl KVStore for MongoDbStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        let start = Instant::now();
        let doc = if data.len() > CHUNK_SIZE {
            for (chunk_num, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
                let doc = KeyValueDocument {
                    _id: chunk_id(key.as_ref(), chunk_num as u32),
                    value: Some(Binary {
                        subtype: mongodb::bson::spec::BinarySubtype::Generic,
                        bytes: chunk.to_vec(),
                    }),
                    chunks: None,
                };
                // TODO: parallelize
                self.collection
                    .replace_one(doc! { "_id": doc._id.clone() }, doc)
                    .upsert(true)
                    .await
                    .wrap_err_with(|| {
                        format!("MongoDB put operation failed for chunk {}", chunk_num)
                    })
                    .write_put_metrics_on_err(start.elapsed(), KVStoreType::Mongo, &self.metrics)?;
            }

            KeyValueDocument {
                _id: key.as_ref().to_string(),
                value: None,
                chunks: Some(data.len().div_ceil(CHUNK_SIZE) as u32),
            }
        } else {
            KeyValueDocument {
                _id: key.as_ref().to_string(),
                value: Some(Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: data,
                }),
                chunks: None,
            }
        };

        self.collection
            .replace_one(doc! { "_id": key.as_ref() }, doc)
            .upsert(true)
            .await
            .wrap_err("MongoDB put operation failed")
            .write_put_metrics(start.elapsed(), KVStoreType::Mongo, &self.metrics)?;

        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let filter = doc! {
            "_id": {
                "$regex": format!("^{}", regex::escape(prefix))
            }
        };

        let mut keys = Vec::new();
        let mut cursor = self
            .collection
            .find(filter)
            .await
            .wrap_err("MongoDB scan operation failed")?;

        while let Some(doc) = cursor.try_next().await? {
            keys.push(doc._id);
        }

        Ok(keys)
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        self.collection
            .delete_one(doc! { "_id": key.as_ref() })
            .await
            .wrap_err_with(|| format!("Failed to delete key {}", key.as_ref()))?;
        Ok(())
    }
}

#[cfg(test)]
pub mod mongo_tests {
    use super::*;
    use crate::{cli::get_aws_config, test_utils::TestMongoContainer};

    async fn setup() -> Result<(TestMongoContainer, MongoDbStorage)> {
        let container = TestMongoContainer::new().await?;

        let storage = MongoDbStorage::new(
            &container.uri,
            "test_db",
            "test_collection",
            Metrics::none(),
        )
        .await?;

        Ok((container, storage))
    }

    #[ignore]
    #[tokio::test]
    async fn test_basic_operations() {
        let (_container, storage) = setup().await.unwrap();

        // Test put
        let key = "test_key";
        let value = b"test_value".to_vec();
        storage.put(key, value.clone()).await.unwrap();

        // Test get
        let result = storage.get(key).await.unwrap().unwrap();
        assert_eq!(result.as_ref(), value.as_slice());

        // Test get nonexistent
        let result = storage.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[ignore]
    #[tokio::test]
    async fn test_large_value() {
        let (_container, storage) = setup().await.unwrap();

        // Test put
        let key = "test_key";
        let mut value = b"a".repeat(CHUNK_SIZE * 10);
        value.extend(&b"b".repeat(CHUNK_SIZE * 5));
        value.extend(&b"c".repeat(CHUNK_SIZE * 5));

        storage.put(key, value.clone()).await.unwrap();

        // Test get
        let result = storage.get(key).await.unwrap().unwrap();
        println!("result size: {:?}", result.len());
        assert_eq!(result.as_ref(), value.as_slice());

        // Test get nonexistent
        let result = storage.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[ignore]
    #[tokio::test]
    async fn test_large_value_testnet_block_33174572() {
        let (_container, storage) = setup().await.unwrap();

        let reader = Bucket::new(
            "testnet-ltu-032-0".to_string(),
            &get_aws_config(None, 60).await,
            Metrics::none(),
        );
        let reader = BlockDataArchive::new(reader);

        let block_number = 33174572;
        let block_data = reader
            .get_block_data_with_offsets(block_number)
            .await
            .unwrap();

        let writer = BlockDataArchive::new(storage.clone());
        writer.archive_block(block_data.block).await.unwrap();
        writer
            .archive_traces(block_data.traces, block_number)
            .await
            .unwrap();
        writer
            .archive_receipts(block_data.receipts, block_number)
            .await
            .unwrap();

        let mongo_block_data = writer
            .get_block_data_with_offsets(block_number)
            .await
            .unwrap();

        let indexer = TxIndexArchiver::new(storage.clone(), reader, 1024 * 1024 * 10);
        indexer
            .index_block(
                mongo_block_data.block,
                mongo_block_data.traces,
                mongo_block_data.receipts,
                mongo_block_data.offsets,
            )
            .await
            .unwrap();
    }

    #[ignore]
    #[tokio::test]
    async fn test_bulk_operations() {
        let (_container, storage) = setup().await.unwrap();

        let kvs: Vec<(String, Vec<u8>)> = vec![
            ("key1".to_string(), b"value1".to_vec()),
            ("key2".to_string(), b"value2".to_vec()),
            ("key3".to_string(), b"value3".to_vec()),
        ];

        storage.bulk_put(kvs.clone()).await.unwrap();

        for (key, value) in kvs {
            let result = storage.get(&key).await.unwrap().unwrap();
            assert_eq!(result.as_ref(), value.as_slice());
        }

        // Test bulk_get
        let keys = vec![
            "key1".to_string(),
            "key2".to_string(),
            "nonexistent".to_string(),
        ];

        let results = storage.bulk_get(&keys).await.unwrap();

        assert_eq!(results.len(), 2); // Should only have the two existing keys
        assert_eq!(results.get("key1").unwrap().as_ref(), b"value1");
        assert_eq!(results.get("key2").unwrap().as_ref(), b"value2");
        assert!(!results.contains_key("nonexistent"));
    }

    #[ignore]
    #[tokio::test]
    async fn test_prefix_scan() {
        let (_container, storage) = setup().await.unwrap();

        // Insert test data
        let kvs = vec![
            ("prefix1_a".to_string(), b"value1".to_vec()),
            ("prefix1_b".to_string(), b"value2".to_vec()),
            ("prefix2_a".to_string(), b"value3".to_vec()),
        ];

        storage.bulk_put(kvs).await.unwrap();

        // Test prefix scanning
        let results = storage.scan_prefix("prefix1_").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"prefix1_a".to_string()));
        assert!(results.contains(&"prefix1_b".to_string()));

        let results = storage.scan_prefix("prefix2_").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results.contains(&"prefix2_a".to_string()));
    }
}
