use bytes::Bytes;
use eyre::{Context, Result};
use mongodb::{
    bson::{doc, Binary},
    options::{ClientOptions, CollectionOptions, WriteConcern},
    Client, Collection,
};
use serde::{Deserialize, Serialize};

use crate::prelude::*;

#[derive(Clone)]
pub struct MongoDbStorage {
    collection: Collection<KeyValueDocument>,
    name: String,
}

#[derive(Serialize, Deserialize)]
struct KeyValueDocument {
    _id: String,
    value: Binary,
}

impl MongoDbStorage {
    pub async fn new_index_store(
        connection_string: &str,
        database: &str,
        create_with_capped_size: Option<u64>,
    ) -> Result<Self> {
        Self::new(
            connection_string,
            database,
            "tx_index",
            create_with_capped_size,
        )
        .await
    }

    pub async fn new_block_store(
        connection_string: &str,
        database: &str,
        create_with_capped_size: Option<u64>,
    ) -> Result<Self> {
        Self::new(
            connection_string,
            database,
            "block_level",
            create_with_capped_size,
        )
        .await
    }

    pub async fn new(
        connection_string: &str,
        database: &str,
        collection_name: &str,
        create_with_capped_size: Option<u64>,
    ) -> Result<Self> {
        let client_options = ClientOptions::parse(connection_string).await?;
        let client = Client::with_options(client_options)?;
        let db = client.database(database);

        if !db
            .list_collection_names()
            .await?
            .contains(&collection_name.to_string())
        {
            // If we aren't creating the collection and it's not found, error
            let Some(max_size_bytes) = create_with_capped_size else {
                bail!("Collection not found: {}", collection_name);
            };

            // Create capped collection if it doesn't exist
            db.create_collection(collection_name)
                .capped(true)
                .size(max_size_bytes)
                .await?;
        }

        // Ensure writes are journaled before returning
        let collection = db.collection_with_options(
            collection_name,
            CollectionOptions::builder()
                .write_concern(Some(WriteConcern::builder().journal(Some(true)).build()))
                .build(),
        );

        Ok(Self {
            collection,
            name: format!("mongodb://{database}/{collection_name}"),
        })
    }
}

impl KVReader for MongoDbStorage {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        match self
            .collection
            .find_one(doc! { "_id": key })
            .await
            .wrap_err("MongoDB get operation failed")?
        {
            Some(doc) => Ok(Some(Bytes::from(doc.value.bytes))),
            None => Ok(None),
        }
    }
}

impl KVStore for MongoDbStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        let doc = KeyValueDocument {
            _id: key.as_ref().to_string(),
            value: Binary {
                subtype: mongodb::bson::spec::BinarySubtype::Generic,
                bytes: data,
            },
        };

        self.collection
            .replace_one(doc! { "_id": key.as_ref() }, doc)
            .upsert(true)
            .await
            .wrap_err("MongoDB put operation failed")?;

        Ok(())
    }

    async fn bulk_put(&self, kvs: impl IntoIterator<Item = (String, Vec<u8>)>) -> Result<()> {
        let documents: Vec<KeyValueDocument> = kvs
            .into_iter()
            .map(|(key, value)| KeyValueDocument {
                _id: key,
                value: Binary {
                    subtype: mongodb::bson::spec::BinarySubtype::Generic,
                    bytes: value,
                },
            })
            .collect();

        if documents.is_empty() {
            return Ok(());
        }

        self.collection
            .insert_many(documents)
            .ordered(false)
            .await
            .wrap_err("MongoDB bulk insert operation failed")?;

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
}

#[cfg(all(test, feature = "mongodb-integration-tests"))]
mod tests {
    use std::process::Command;

    use mongodb::bson::uuid::Uuid;

    use super::*;

    pub struct TestMongoContainer {
        container_id: String,
    }

    impl TestMongoContainer {
        pub async fn new() -> Result<Self> {
            let container_name = format!("mongo_test_{}", Uuid::new());
            let port = 27017;

            // Start container
            let output = Command::new("docker")
                .args([
                    "run",
                    "-d",
                    "-p",
                    &format!("{port}:27017"),
                    "--name",
                    &container_name,
                    "mongo:latest",
                ])
                .output()
                .wrap_err("Failed to start MongoDB container")?;

            let container_id = String::from_utf8(output.stdout)
                .wrap_err("Invalid container ID output")?
                .trim()
                .to_string();

            // Poll until MongoDB is ready
            let client_options = ClientOptions::parse(format!("mongodb://localhost:{port}"))
                .await
                .unwrap();
            let max_attempts = 30; // 30 * 200ms = 6 seconds max
            let mut attempt = 0;

            while attempt < max_attempts {
                match Client::with_options(client_options.clone()) {
                    Ok(client) => {
                        // Try to actually connect and run a command
                        match client.list_database_names().await {
                            Ok(_) => return Ok(Self { container_id }),
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                attempt += 1;
                                continue;
                            }
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        attempt += 1;
                        continue;
                    }
                }
            }

            bail!("MongoDB container failed to become ready")
        }
    }

    impl Drop for TestMongoContainer {
        fn drop(&mut self) {
            Command::new("docker")
                .args(["stop", &self.container_id])
                .output()
                .expect("Failed to stop MongoDB container");
        }
    }

    async fn setup() -> Result<(TestMongoContainer, MongoDbStorage)> {
        let container = TestMongoContainer::new().await?;

        let storage = MongoDbStorage::new(
            "mongodb://localhost:27017",
            "test_db",
            "test_collection",
            Some(1024 * 1024), // 1MB cap
        )
        .await?;

        Ok((container, storage))
    }

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
    }

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
