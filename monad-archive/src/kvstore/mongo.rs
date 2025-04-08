use bytes::Bytes;
use eyre::{Context, Result};
use mongodb::{
    bson::{doc, Binary},
    options::{ClientOptions, CollectionOptions, WriteConcern},
    Client, Collection,
};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::prelude::*;

const MAX_CONNECTION_POOL_SIZE: u32 = 50;
const CHUNK_SIZE: usize = 1024 * 1024 * 15; // 15MB

#[derive(Clone)]
pub struct MongoDbStorage {
    pub client: Client,
    pub(crate) collection: Collection<KeyValueDocument>,
    pub db_name: String,
    name: String,
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
                let keys = (0..chunks)
                    .map(|chunk_num| chunk_id(&self._id, chunk_num))
                    .collect::<Vec<_>>();
                let chunks = collection
                    .find(doc! { "_id": {"$in": keys} })
                    .await?
                    .map(|x| x.wrap_err("Failed to get chunk"))
                    .try_fold(Vec::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk.value.wrap_err("Chunk has no value")?.bytes);
                        Ok::<_, eyre::Error>(acc)
                    })
                    .await
                    .wrap_err("Failed to resolve chunks")?;

                Ok((self._id, Bytes::from(chunks)))
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
        create_with_capped_size: Option<u64>,
    ) -> Result<Self> {
        trace!(
            "Creating MongoDB index store with connection: {}, database: {}",
            connection_string,
            database
        );
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
        trace!(
            "Creating MongoDB block store with connection: {}, database: {}",
            connection_string,
            database
        );
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
        info!(
            "Initializing MongoDB connection to {}/{}",
            connection_string, database
        );

        let client = new_client(connection_string).await?;

        let db = client.database(database);
        debug!("Using database: {}", database);

        let collection_exists = db
            .list_collection_names()
            .await?
            .contains(&collection_name.to_string());

        if !collection_exists {
            debug!("Collection '{}' not found", collection_name);

            // If we aren't creating the collection and it's not found, error
            let Some(max_size_gb) = create_with_capped_size else {
                warn!(
                    "Collection '{}' not found and no capped size provided",
                    collection_name
                );
                bail!("Collection not found: {}", collection_name);
            };
            let max_size_bytes = max_size_gb * 2u64.pow(30);

            info!(
                "Creating capped collection '{}' with size: {} GB ({} bytes)",
                collection_name, max_size_gb, max_size_bytes
            );

            // Create capped collection if it doesn't exist
            db.create_collection(collection_name)
                .capped(true)
                .size(max_size_bytes)
                .await?;

            debug!(
                "Capped collection '{}' created successfully",
                collection_name
            );
        } else {
            trace!("Collection '{}' already exists", collection_name);
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
        };

        info!("MongoDB storage initialized: {}", storage.name);
        Ok(storage)
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
            Some(doc) => doc.resolve(&self.collection).await.map(|x| Some(x.1)),
            None => Ok(None),
        }
    }

    async fn bulk_get(&self, keys: &[String]) -> Result<HashMap<String, Bytes>> {
        self.collection
            .find(doc! { "_id": {"$in": keys} })
            .await
            .wrap_err("MongoDB get operation failed")?
            .map(|x| x.wrap_err("MongoDB get operation failed"))
            .and_then(|x| async { x.resolve(&self.collection).await })
            .try_collect::<HashMap<String, Bytes>>()
            .await
            .wrap_err("MongoDB bulk_get operation failed")
    }
}

impl KVStore for MongoDbStorage {
    fn bucket_name(&self) -> &str {
        &self.name
    }

    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
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
                    })?;
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
            .wrap_err("MongoDB put operation failed")?;

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
    use std::{process::Command, sync::atomic::AtomicU16};

    use mongodb::bson::uuid::Uuid;
    use serial_test::serial;

    use super::*;

    pub struct TestMongoContainer {
        pub container_id: String,
        pub port: u16,
    }

    static NEXT_PORT: AtomicU16 = AtomicU16::new(27017);

    impl TestMongoContainer {
        pub async fn new() -> Result<Self> {
            let container_name = format!("mongo_test_{}", Uuid::new());
            let port = NEXT_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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

            println!(
                "Starting MongoDB container: {}, {}",
                container_name, container_id
            );

            let output = Command::new("docker")
                .args(["ps"])
                .output()
                .expect("Failed to list containers");

            println!("Containers: {}", String::from_utf8(output.stdout).unwrap());

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
                            Ok(_) => return Ok(Self { container_id, port }),
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
            println!("Stopping MongoDB container: {}", self.container_id);
            Command::new("docker")
                .args(["stop", &self.container_id])
                .output()
                .expect("Failed to stop MongoDB container");
            Command::new("docker")
                .args(["rm", &self.container_id])
                .output()
                .expect("Failed to remove MongoDB container");
        }
    }

    async fn setup() -> Result<(TestMongoContainer, MongoDbStorage)> {
        let container = TestMongoContainer::new().await?;

        let storage = MongoDbStorage::new(
            &format!("mongodb://localhost:{}", container.port),
            "test_db",
            "test_collection",
            Some(5), // 5gb cap
        )
        .await?;

        Ok((container, storage))
    }

    #[ignore]
    #[tokio::test]
    #[serial]
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
    #[serial]
    async fn test_large_value() {
        let (_container, storage) = setup().await.unwrap();

        // Test put
        let key = "test_key";
        let value = b"a".repeat(CHUNK_SIZE * 10);
        storage.put(key, value.clone()).await.unwrap();

        // Test get
        let result = storage.get(key).await.unwrap().unwrap();
        println!("result size: {:?}", result.len());
        assert_eq!(result.as_ref(), value.as_slice());

        // Test get nonexistent
        let result = storage.get("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    #[serial]
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
    #[serial]
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
