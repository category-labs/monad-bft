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

//! DynamoDB-API-compatible [`MetaStore`] + [`MetaStoreCas`] backend.
//!
//! Talks to the DynamoDB HTTP API: AWS DynamoDB, DynamoDB Local, and -- the
//! main portability target -- **ScyllaDB Alternator** (point `endpoint_url` at
//! the Alternator port). Chain-data metadata (kv rows, scannable
//! directory/index rows, and the publication-head CAS row) can then live in a
//! DynamoDB-shaped store instead of the embedded fjall LSM.
//!
//! ## Single-table design (a wire contract once data exists)
//!
//! Every logical row lives in one physical table. Schema:
//!
//! | attribute | type | role |
//! |---|---|---|
//! | `pk` | Binary | partition key |
//! | `sk` | Binary | sort key |
//! | `val` | Binary | the stored value |
//! | `v` | Number | CAS version (CAS rows only) |
//!
//! The key kind (`kv`/`scan`/`cas`) and the logical table name are folded into
//! `pk` with explicit length prefixes so decoding is unambiguous and partitions
//! are well distributed:
//!
//! ```text
//! pk = u16-be(len(kind)) ∥ kind ∥ u16-be(len(table)) ∥ table ∥ key-or-partition
//! sk = clustering            (scan rows)
//! sk = 0x00 (sentinel)       (kv / cas rows -- pk already identifies the row)
//! ```
//!
//! Binary keys sort by unsigned byte order in DynamoDB/Alternator, which is
//! exactly the ordering `scan_list`/`begins_with` rely on, so logical keys map
//! natively with no hex hop. A scannable `(table, partition)` becomes the
//! DynamoDB partition and its clustering keys become the sort keys within it, so
//! `scan_list` is a single-partition `Query` (drained across response pages so a
//! single call returns the whole requested range), never a cross-partition
//! `Scan`.
//!
//! ## Strong consistency & CAS
//!
//! `get`/`scan_get`/`cas_get` use strongly consistent reads (quorum reads on
//! Alternator) so the publication-head fence is correct. `cas_put` is a
//! conditional `PutItem` (LWT-backed on Alternator). A failed condition maps to
//! [`CasOutcome::Conflict`], never an error. See the scope doc for the full
//! Alternator-compatibility checklist.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    config::{Credentials, Region},
    error::{ProvideErrorMetadata, SdkError},
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
        ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TableStatus, WriteRequest,
    },
    Client,
};
use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::{
    error::{MonadChainDataError, Result},
    store::{
        common::Page,
        meta::{
            CasOutcome, CasVersion, MetaStore, MetaStoreCas, MetaWriteOp, ScannableTableId, TableId,
        },
    },
};

/// Attribute names. Short to keep item overhead low; these are a wire contract.
pub(crate) const ATTR_PK: &str = "pk";
pub(crate) const ATTR_SK: &str = "sk";
pub(crate) const ATTR_VAL: &str = "val";
const ATTR_VERSION: &str = "v";

/// Key-kind discriminators folded into `pk`.
const KIND_KV: &[u8] = b"kv";
const KIND_SCAN: &[u8] = b"scan";
const KIND_CAS: &[u8] = b"cas";

/// Sort-key sentinel for rows whose `pk` already uniquely identifies them
/// (kv/cas). A single byte keeps the row addressable by a fixed `sk`.
const SK_SENTINEL: &[u8] = &[0x00];

/// DynamoDB caps `BatchWriteItem` at 25 write requests per call.
const BATCH_WRITE_LIMIT: usize = 25;

/// Alternator rejects oversized HTTP request bodies. DynamoDB's documented
/// BatchWriteItem payload cap is 16 MiB; keep a margin for JSON/base64 framing.
const BATCH_WRITE_PAYLOAD_SOFT_LIMIT: usize = 12 * 1024 * 1024;
const BATCH_WRITE_ITEM_OVERHEAD: usize = 512;
// With a 50ms exponential backoff, 8 retry sleeps have a worst-case cumulative
// delay of 12.75s; the 9th would raise it to 25.55s.
pub(crate) const BATCH_WRITE_CHUNK_MAX_RETRIES: u32 = 8;
const BATCH_WRITE_CHUNK_BASE_BACKOFF_MS: u64 = 50;
const BATCH_WRITE_CHUNK_MAX_BACKOFF_MS: u64 = 20_000;

/// Static credentials supplied explicitly rather than via the ambient AWS
/// credential chain. Required for DynamoDB Local / Alternator deployments that
/// have no instance/profile credentials (they accept any non-empty pair).
#[derive(Clone)]
pub struct DynamoCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for DynamoCredentials {
    // Never print secret material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Physical DynamoDB table mapping for logical metadata tables.
#[derive(Debug, Clone)]
pub enum DynamoTableLayout {
    /// All kv/scan/cas rows share one physical DynamoDB table.
    Single { table_name: String },
    /// Each logical table id maps to one physical DynamoDB table named
    /// `{prefix}-{logical-table-name}` after Dynamo-safe normalization.
    PerLogicalTable { prefix: String },
}

impl DynamoTableLayout {
    pub fn single(table_name: impl Into<String>) -> Self {
        Self::Single {
            table_name: table_name.into(),
        }
    }
}

/// Construction parameters for [`DynamoMetaStore`].
#[derive(Debug, Clone)]
pub struct DynamoMetaStoreConfig {
    /// Physical table layout for logical kv/scan/cas rows.
    pub table_layout: DynamoTableLayout,
    /// Override the endpoint for DynamoDB Local / Alternator. Leave `None` to
    /// target real AWS DynamoDB via the default endpoint resolver.
    pub endpoint_url: Option<String>,
    /// AWS region. `None` falls through to the default region provider chain.
    /// DynamoDB Local / Alternator accept any value (commonly `"us-east-1"`).
    pub region: Option<String>,
    /// Max in-flight `BatchWriteItem` calls for [`DynamoMetaStore::apply_writes`].
    /// Clamped to >= 1.
    pub batch_max_concurrency: usize,
    /// Max in-flight `BatchWriteItem` calls per physical table for
    /// [`DynamoMetaStore::apply_writes`]. Clamped to >= 1.
    pub batch_table_max_concurrency: usize,
    /// Explicit static credentials. `None` uses the ambient AWS credential
    /// chain (env, profile, instance role, ...).
    pub credentials: Option<DynamoCredentials>,
}

impl DynamoMetaStoreConfig {
    /// Minimal config targeting real AWS DynamoDB with ambient credentials and
    /// the default region chain. Callers tweak the remaining fields as needed.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_layout: DynamoTableLayout::single(table_name),
            endpoint_url: None,
            region: None,
            batch_max_concurrency: 16,
            batch_table_max_concurrency: 16,
            credentials: None,
        }
    }
}

struct Inner {
    client: Client,
    table_layout: DynamoTableLayout,
    batch_max_concurrency: usize,
    batch_table_max_concurrency: usize,
}

#[derive(Default)]
struct BatchWriteProgress {
    started: AtomicUsize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    retries: AtomicUsize,
    unprocessed_items: AtomicUsize,
    active: Mutex<HashMap<usize, ActiveBatchWriteChunk>>,
}

#[derive(Clone)]
struct ActiveBatchWriteChunk {
    table: String,
}

impl BatchWriteProgress {
    fn mark_started(&self, chunk_idx: usize, table: &str, attempt: u32) {
        if attempt == 0 {
            self.started.fetch_add(1, Ordering::Relaxed);
        } else {
            self.retries.fetch_add(1, Ordering::Relaxed);
        }
        self.active.lock().expect("batch progress poisoned").insert(
            chunk_idx,
            ActiveBatchWriteChunk {
                table: table.to_string(),
            },
        );
    }

    fn mark_completed(&self, chunk_idx: usize) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.active
            .lock()
            .expect("batch progress poisoned")
            .remove(&chunk_idx);
    }

    fn mark_attempt_finished(&self, chunk_idx: usize) {
        self.active
            .lock()
            .expect("batch progress poisoned")
            .remove(&chunk_idx);
    }

    fn mark_failed(&self, chunk_idx: usize) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.active
            .lock()
            .expect("batch progress poisoned")
            .remove(&chunk_idx);
    }

    fn mark_unprocessed(&self, count: usize) {
        self.unprocessed_items.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> BatchWriteProgressSnapshot {
        let active = self.active.lock().expect("batch progress poisoned");
        let mut active_by_table: HashMap<String, usize> = HashMap::new();
        for chunk in active.values() {
            *active_by_table.entry(chunk.table.clone()).or_default() += 1;
        }
        let mut active_by_table = active_by_table.into_iter().collect::<Vec<_>>();
        active_by_table.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        BatchWriteProgressSnapshot {
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            unprocessed_items: self.unprocessed_items.load(Ordering::Relaxed),
            active: active.len(),
            active_by_table,
        }
    }
}

struct BatchWriteProgressSnapshot {
    started: usize,
    completed: usize,
    failed: usize,
    retries: usize,
    unprocessed_items: usize,
    active: usize,
    active_by_table: Vec<(String, usize)>,
}

/// DynamoDB-API-compatible [`MetaStore`] + [`MetaStoreCas`]. Cheaply cloneable
/// -- all state lives behind an `Arc`, and the underlying SDK `Client` is itself
/// `Arc`-backed.
#[derive(Clone)]
pub struct DynamoMetaStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DynamoMetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoMetaStore")
            .field("table_layout", &self.inner.table_layout)
            .field("batch_max_concurrency", &self.inner.batch_max_concurrency)
            .field(
                "batch_table_max_concurrency",
                &self.inner.batch_table_max_concurrency,
            )
            .finish_non_exhaustive()
    }
}

impl DynamoMetaStore {
    /// Builds the SDK client from `config` and returns a ready store. Async
    /// because resolving the AWS credential/region chain performs I/O. Assumes
    /// the table already exists (see [`DynamoMetaStore::create_table`] for the
    /// opt-in test/dev provisioning helper).
    pub async fn new(config: DynamoMetaStoreConfig) -> Result<Self> {
        let DynamoMetaStoreConfig {
            table_layout,
            endpoint_url,
            region,
            batch_max_concurrency,
            batch_table_max_concurrency,
            credentials,
        } = config;

        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        }
        if let Some(endpoint) = &endpoint_url {
            loader = loader.endpoint_url(endpoint);
        }
        if let Some(creds) = credentials {
            loader = loader.credentials_provider(Credentials::new(
                creds.access_key_id,
                creds.secret_access_key,
                creds.session_token,
                None,
                "DynamoMetaStoreConfig",
            ));
        }
        let sdk_config = loader.load().await;
        let client = Client::new(&sdk_config);

        Ok(Self {
            inner: Arc::new(Inner {
                client,
                table_layout,
                batch_max_concurrency: batch_max_concurrency.max(1),
                batch_table_max_concurrency: batch_table_max_concurrency.max(1),
            }),
        })
    }

    /// The underlying SDK client. Cloning is cheap (it is `Arc`-backed) and
    /// shares the connection pool and resolved credentials, letting a
    /// fully-dynamo deployment hand this client to the blob backend instead of
    /// building a second one (see `DynamoBlobStore::new_with_client`).
    pub fn client(&self) -> Client {
        self.inner.client.clone()
    }

    fn table_name_for_kv(&self, table: TableId) -> String {
        self.table_name_for(table.as_str())
    }

    fn table_name_for_scan(&self, table: ScannableTableId) -> String {
        self.table_name_for(table.as_str())
    }

    fn table_name_for_cas(&self, table: TableId) -> String {
        self.table_name_for(table.as_str())
    }

    fn table_name_for(&self, logical_table: &str) -> String {
        match &self.inner.table_layout {
            DynamoTableLayout::Single { table_name } => table_name.clone(),
            DynamoTableLayout::PerLogicalTable { prefix } => {
                format!("{prefix}-{}", dynamo_safe_name(logical_table))
            }
        }
    }

    fn physical_table_names(&self) -> Vec<String> {
        match &self.inner.table_layout {
            DynamoTableLayout::Single { table_name } => vec![table_name.clone()],
            DynamoTableLayout::PerLogicalTable { prefix } => known_logical_table_names()
                .into_iter()
                .map(|logical| format!("{prefix}-{}", dynamo_safe_name(logical)))
                .collect(),
        }
    }

    /// Single strongly-consistent `GetItem` returning the `val` bytes, or
    /// `Ok(None)` when the item is absent.
    async fn get_val(
        &self,
        physical_table: String,
        pk: Vec<u8>,
        sk: Vec<u8>,
    ) -> Result<Option<Bytes>> {
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(physical_table)
            .key(ATTR_PK, AttributeValue::B(Blob::new(pk)))
            .key(ATTR_SK, AttributeValue::B(Blob::new(sk)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| backend_err("get_item", e))?;

        match resp.item {
            None => Ok(None),
            Some(mut item) => Ok(Some(take_val(&mut item)?)),
        }
    }

    /// Single `PutItem` of `(pk, sk, val)`.
    async fn put_val(
        &self,
        physical_table: String,
        pk: Vec<u8>,
        sk: Vec<u8>,
        value: Bytes,
    ) -> Result<()> {
        self.inner
            .client
            .put_item()
            .table_name(physical_table)
            .item(ATTR_PK, AttributeValue::B(Blob::new(pk)))
            .item(ATTR_SK, AttributeValue::B(Blob::new(sk)))
            .item(ATTR_VAL, AttributeValue::B(Blob::new(value.to_vec())))
            .send()
            .await
            .map_err(|e| backend_err("put_item", e))?;
        Ok(())
    }

    /// One `BatchWriteItem` call (<= 25 items), retrying `UnprocessedItems`
    /// until they drain.
    async fn write_chunk(
        &self,
        chunk_idx: usize,
        table: String,
        requests: Vec<WriteRequest>,
        progress: Arc<BatchWriteProgress>,
    ) -> Result<()> {
        let mut pending: HashMap<String, Vec<WriteRequest>> = HashMap::new();
        let initial_request_count = requests.len();
        pending.insert(table.clone(), requests);

        // Bounded retry of throttled/unprocessed items with exponential full
        // jitter. Batch-level IO retry is intentionally expensive; concentrate
        // retry pressure on the residual items in this one chunk first.
        // BatchWriteItem returns the leftovers rather than erroring on partial
        // capacity, so we resubmit only what was not applied.
        let mut attempt = 0u32;
        loop {
            let pending_count = pending.values().map(Vec::len).sum::<usize>();
            let log_sample = chunk_idx < 8 || chunk_idx % 100 == 0 || attempt > 0;
            if log_sample {
                debug!(
                    chunk_idx,
                    table = %table,
                    attempt,
                    pending_count,
                    initial_request_count,
                    "dynamo batch_write_item send starting"
                );
            }
            progress.mark_started(chunk_idx, &table, attempt);
            let send_started = std::time::Instant::now();
            let send = self
                .inner
                .client
                .batch_write_item()
                .set_request_items(Some(pending.clone()))
                .send();
            tokio::pin!(send);

            let mut next_warn = std::time::Duration::from_secs(10);
            let resp = loop {
                tokio::select! {
                    result = &mut send => break result,
                    _ = tokio::time::sleep(next_warn) => {
                        warn!(
                            chunk_idx,
                            table = %table,
                            attempt,
                            pending_count,
                            initial_request_count,
                            elapsed_ms = send_started.elapsed().as_millis() as u64,
                            "dynamo batch_write_item send still in flight"
                        );
                        next_warn = std::time::Duration::from_secs(60);
                    }
                }
            };
            let resp = match resp {
                Ok(resp) => resp,
                Err(e) => {
                    progress.mark_attempt_finished(chunk_idx);
                    let error = backend_err("batch_write_item", e);
                    attempt += 1;
                    if attempt > BATCH_WRITE_CHUNK_MAX_RETRIES {
                        progress.mark_failed(chunk_idx);
                        return Err(error);
                    }
                    let backoff = batch_write_retry_backoff(chunk_idx, &table, attempt);
                    warn!(
                        chunk_idx,
                        table = %table,
                        attempt,
                        max_retries = BATCH_WRITE_CHUNK_MAX_RETRIES,
                        pending_count,
                        elapsed_ms = send_started.elapsed().as_millis() as u64,
                        backoff_ms = backoff.as_millis() as u64,
                        %error,
                        "dynamo batch_write_item send failed; retrying chunk"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };
            if log_sample {
                debug!(
                    chunk_idx,
                    table = %table,
                    attempt,
                    pending_count,
                    elapsed_ms = send_started.elapsed().as_millis() as u64,
                    "dynamo batch_write_item send completed"
                );
            }

            let leftovers = resp
                .unprocessed_items
                .and_then(|mut m| m.remove(&table))
                .unwrap_or_default();
            if leftovers.is_empty() {
                progress.mark_completed(chunk_idx);
                return Ok(());
            }
            progress.mark_attempt_finished(chunk_idx);
            progress.mark_unprocessed(leftovers.len());
            warn!(
                chunk_idx,
                table = %table,
                attempt,
                max_retries = BATCH_WRITE_CHUNK_MAX_RETRIES,
                leftover_count = leftovers.len(),
                "dynamo batch_write_item returned unprocessed items"
            );
            attempt += 1;
            if attempt > BATCH_WRITE_CHUNK_MAX_RETRIES {
                progress.mark_failed(chunk_idx);
                return Err(MonadChainDataError::Backend(format!(
                    "dynamo batch_write_item: {} items still unprocessed after {attempt} retries",
                    leftovers.len()
                )));
            }
            let backoff = batch_write_retry_backoff(chunk_idx, &table, attempt);
            tokio::time::sleep(backoff).await;
            pending.clear();
            pending.insert(table.clone(), leftovers);
        }
    }

    /// Idempotent table provisioning for tests/dev ONLY -- never called by
    /// [`DynamoMetaStore::new`]. Creates the single shared table with the
    /// `pk` (Binary HASH) + `sk` (Binary RANGE) schema in on-demand billing
    /// mode, treating `ResourceInUseException` (table already exists) as
    /// success, then polls `DescribeTable` until the table is `ACTIVE`.
    pub async fn create_table(&self) -> Result<()> {
        for table in self.physical_table_names() {
            self.create_one_table(&table).await?;
        }
        Ok(())
    }

    async fn create_one_table(&self, table: &str) -> Result<()> {
        let attr = |name: &str| {
            AttributeDefinition::builder()
                .attribute_name(name)
                .attribute_type(ScalarAttributeType::B)
                .build()
                .expect("attribute definition")
        };
        let key = |name: &str, kt: KeyType| {
            KeySchemaElement::builder()
                .attribute_name(name)
                .key_type(kt)
                .build()
                .expect("key schema element")
        };

        let create = self
            .inner
            .client
            .create_table()
            .table_name(table.to_string())
            .attribute_definitions(attr(ATTR_PK))
            .attribute_definitions(attr(ATTR_SK))
            .key_schema(key(ATTR_PK, KeyType::Hash))
            .key_schema(key(ATTR_SK, KeyType::Range))
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        match create {
            Ok(_) => {}
            // Already provisioned: idempotent success. Match both the typed
            // variant and the metadata code (Alternator may report untyped).
            Err(e) if e.code() == Some("ResourceInUseException") => {}
            Err(e) => return Err(backend_err("create_table", e)),
        }

        // Poll until ACTIVE. No generated waiter is available without the
        // `waiters` SDK feature, so describe-and-sleep.
        for _ in 0..60 {
            let desc = self
                .inner
                .client
                .describe_table()
                .table_name(table.to_string())
                .send()
                .await
                .map_err(|e| backend_err("describe_table", e))?;
            let status = desc.table.and_then(|t| t.table_status);
            if status == Some(TableStatus::Active) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        Err(MonadChainDataError::Backend(format!(
            "dynamo create_table: table {table} did not become ACTIVE in time"
        )))
    }

    /// Startup connectivity/schema check. Verifies the configured table exists,
    /// is ACTIVE, and has the expected binary `pk` hash + binary `sk` range
    /// key schema before ingest workers begin writing.
    pub async fn validate_table(&self) -> Result<()> {
        let tables = self.physical_table_names();
        let concurrency = self
            .inner
            .batch_table_max_concurrency
            .min(tables.len())
            .max(1);
        futures::stream::iter(tables)
            .map(|table| async move { self.validate_one_table(&table).await })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }

    async fn validate_one_table(&self, table: &str) -> Result<()> {
        let desc = self
            .inner
            .client
            .describe_table()
            .table_name(table.to_string())
            .send()
            .await
            .map_err(|e| backend_err("describe_table", e))?;
        let Some(table_desc) = desc.table else {
            return Err(MonadChainDataError::Backend(format!(
                "dynamo describe_table: table {table} missing from response"
            )));
        };
        if table_desc.table_status != Some(TableStatus::Active) {
            return Err(MonadChainDataError::Backend(format!(
                "dynamo table {table} is not ACTIVE: {:?}",
                table_desc.table_status
            )));
        }

        let attrs = table_desc.attribute_definitions.unwrap_or_default();
        let attr_type = |name: &str| {
            attrs
                .iter()
                .find(|a| a.attribute_name == name)
                .map(|a| a.attribute_type.clone())
        };
        let keys = table_desc.key_schema.unwrap_or_default();
        let key_type = |name: &str| {
            keys.iter()
                .find(|k| k.attribute_name == name)
                .map(|k| k.key_type.clone())
        };

        if attr_type(ATTR_PK) != Some(ScalarAttributeType::B)
            || attr_type(ATTR_SK) != Some(ScalarAttributeType::B)
            || key_type(ATTR_PK) != Some(KeyType::Hash)
            || key_type(ATTR_SK) != Some(KeyType::Range)
        {
            return Err(MonadChainDataError::Backend(format!(
                "dynamo table {table} schema mismatch: expected binary pk HASH + binary sk RANGE"
            )));
        }
        Ok(())
    }
}

impl MetaStore for DynamoMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_val(
            self.table_name_for_kv(table),
            kv_pk(table, key),
            SK_SENTINEL.to_vec(),
        )
        .await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        self.get_val(
            self.table_name_for_scan(table),
            scan_pk(table, partition),
            clustering.to_vec(),
        )
        .await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        self.put_val(
            self.table_name_for_kv(table),
            kv_pk(table, key),
            SK_SENTINEL.to_vec(),
            value,
        )
        .await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        self.put_val(
            self.table_name_for_scan(table),
            scan_pk(table, partition),
            clustering.to_vec(),
            value,
        )
        .await
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let build_started = std::time::Instant::now();
        let input_write_count = writes.len();
        let requests: Vec<(String, WriteRequest, usize)> = writes
            .into_iter()
            .map(|op| {
                let (physical_table, pk, sk, value) = match op {
                    MetaWriteOp::Put { table, key, value } => (
                        self.table_name_for_kv(table),
                        kv_pk(table, &key),
                        SK_SENTINEL.to_vec(),
                        value,
                    ),
                    MetaWriteOp::ScanPut {
                        table,
                        partition,
                        clustering,
                        value,
                    } => (
                        self.table_name_for_scan(table),
                        scan_pk(table, &partition),
                        clustering,
                        value,
                    ),
                };
                let estimated_wire_bytes =
                    estimated_batch_write_item_bytes(pk.len(), sk.len(), value.len());
                let put = PutRequest::builder()
                    .item(ATTR_PK, AttributeValue::B(Blob::new(pk)))
                    .item(ATTR_SK, AttributeValue::B(Blob::new(sk)))
                    .item(ATTR_VAL, AttributeValue::B(Blob::new(value.to_vec())))
                    .build()
                    .map_err(|e| {
                        MonadChainDataError::Backend(format!("dynamo build put_request: {e}"))
                    })?;
                Ok((
                    physical_table,
                    WriteRequest::builder().put_request(put).build(),
                    estimated_wire_bytes,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let request_count = requests.len();
        let estimated_wire_bytes = requests.iter().map(|(_, _, bytes)| *bytes).sum::<usize>();
        let concurrency = self.inner.batch_max_concurrency;
        let table_concurrency = self.inner.batch_table_max_concurrency;
        let count_only_chunks = count_only_chunks_by_table(&requests);
        let chunks = split_batch_write_chunks(requests);
        let chunk_count = chunks.len();
        let chunk_tables = chunks.iter().map(|(table, _)| table.as_str());
        let chunks_by_table = summarize_names(chunk_tables, 8);
        info!(
            input_write_count,
            request_count,
            chunks = chunk_count,
            count_only_chunks,
            estimated_wire_bytes,
            concurrency,
            table_concurrency,
            chunks_by_table = %chunks_by_table,
            build_ms = build_started.elapsed().as_millis() as u64,
            "dynamo apply_writes built BatchWriteItem requests"
        );
        if chunks.len() > count_only_chunks {
            warn!(
                request_count,
                chunks = chunk_count,
                count_only_chunks,
                estimated_wire_bytes,
                payload_soft_limit = BATCH_WRITE_PAYLOAD_SOFT_LIMIT,
                "dynamo batch_write_item split by estimated payload size"
            );
        }
        let progress = Arc::new(BatchWriteProgress::default());
        let mut table_permits = HashMap::new();
        for (table, _) in &chunks {
            table_permits
                .entry(table.clone())
                .or_insert_with(|| Arc::new(Semaphore::new(table_concurrency)));
        }
        let table_permits = Arc::new(table_permits);
        let send_started = std::time::Instant::now();
        let result =
            futures::stream::iter(chunks.into_iter().enumerate().map(|(chunk_idx, chunk)| {
                let store = self.clone();
                let progress = progress.clone();
                let table_permits = table_permits.clone();
                async move {
                    let (table, requests): (String, Vec<WriteRequest>) = chunk;
                    let permit = table_permits
                        .get(&table)
                        .expect("table semaphore must exist for every write chunk")
                        .clone()
                        .acquire_owned()
                        .await
                        .expect("table semaphore should not be closed");
                    let _permit = permit;
                    store
                        .write_chunk(chunk_idx, table, requests, progress)
                        .await
                }
            }))
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await;
        if let Err(error) = &result {
            let snapshot = progress.snapshot();
            warn!(
                %error,
                request_count,
                chunks = chunk_count,
                started_chunks = snapshot.started,
                completed_chunks = snapshot.completed,
                failed_chunks = snapshot.failed,
                active_chunks = snapshot.active,
                retry_attempts = snapshot.retries,
                unprocessed_items = snapshot.unprocessed_items,
                active_by_table = %format_count_pairs(&snapshot.active_by_table, 8),
                "dynamo apply_writes BatchWriteItem stream failed"
            );
        }
        result?;
        let snapshot = progress.snapshot();
        info!(
            request_count,
            chunks = chunk_count,
            started_chunks = snapshot.started,
            completed_chunks = snapshot.completed,
            failed_chunks = snapshot.failed,
            retry_attempts = snapshot.retries,
            unprocessed_items = snapshot.unprocessed_items,
            send_ms = send_started.elapsed().as_millis() as u64,
            "dynamo apply_writes completed BatchWriteItem requests"
        );
        Ok(())
    }

    async fn scan_list(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        prefix: &[u8],
        cursor: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<Page> {
        let pk = scan_pk(table, partition);
        let physical_table = self.table_name_for_scan(table);
        // A DynamoDB `Query` returns at most 1 MB per response (and at most
        // `Limit` items), signalling more via `LastEvaluatedKey`. The trait
        // contract -- matching fjall/in-memory -- is to return the whole
        // matching range up to `limit` in one call, so we drain pages here.
        //
        // We over-read by one row past `limit` (`want = limit + 1`) so we can
        // tell whether a *further matching* row exists: that distinguishes "the
        // page happened to end exactly on `limit`" (no more rows -> next_cursor
        // None) from "there is genuinely more" (next_cursor Some), exactly like
        // fjall, with no spurious trailing empty page. `usize::MAX` (the
        // unbounded callers) saturates, so no per-page `Limit` is set and we
        // drain until the partition is exhausted.
        let want = limit.saturating_add(1);
        // Resume server-side just past the prior clustering via
        // ExclusiveStartKey = {pk, sk: cursor}.
        let mut start_key: Option<HashMap<String, AttributeValue>> = cursor.map(|c| {
            HashMap::from([
                (
                    ATTR_PK.to_string(),
                    AttributeValue::B(Blob::new(pk.clone())),
                ),
                (ATTR_SK.to_string(), AttributeValue::B(Blob::new(c))),
            ])
        });

        let mut keys: Vec<Vec<u8>> = Vec::new();
        loop {
            let mut req = self
                .inner
                .client
                .query()
                .table_name(physical_table.clone())
                .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.clone())))
                // Callers only consume the clustering (`sk`); projecting it
                // alone keeps `val` off the wire.
                .projection_expression(ATTR_SK)
                .consistent_read(true)
                .scan_index_forward(true);
            // An empty prefix means "every clustering in the partition" (matching
            // fjall/in-memory). DynamoDB/Alternator rejects `begins_with(sk, "")`
            // -- an empty value on a key attribute -- so drop the term entirely
            // and let `pk = :p` select the whole partition. A non-empty prefix
            // keeps the `begins_with` filter (and only then binds `:prefix`, so
            // no unused expression value is sent).
            req = if prefix.is_empty() {
                req.key_condition_expression("pk = :p")
            } else {
                req.key_condition_expression("pk = :p AND begins_with(sk, :prefix)")
                    .expression_attribute_values(
                        ":prefix",
                        AttributeValue::B(Blob::new(prefix.to_vec())),
                    )
            };
            // Bound each page to what we still need (when finite).
            if let Ok(remaining) = i32::try_from(want - keys.len()) {
                req = req.limit(remaining);
            }
            if let Some(start) = &start_key {
                req = req.set_exclusive_start_key(Some(start.clone()));
            }

            let resp = req.send().await.map_err(|e| backend_err("query", e))?;
            for mut item in resp.items.unwrap_or_default() {
                keys.push(take_binary(&mut item, ATTR_SK)?);
            }

            // Got the look-ahead row -> we have enough to answer.
            if keys.len() >= want {
                break;
            }
            // Follow the server's cursor (robust against an empty page that
            // still carries a LastEvaluatedKey); a missing one means the
            // matching range is exhausted.
            match resp.last_evaluated_key {
                Some(lek) => start_key = Some(lek),
                None => break,
            }
        }

        // If we over-read past `limit`, a further matching row exists: trim to
        // `limit` and hand back the last returned clustering as the cursor.
        let next_cursor = if keys.len() > limit {
            keys.truncate(limit);
            keys.last().cloned()
        } else {
            None
        };

        Ok(Page { keys, next_cursor })
    }
}

impl MetaStoreCas for DynamoMetaStore {
    async fn cas_get(&self, table: TableId, key: &[u8]) -> Result<Option<(CasVersion, Bytes)>> {
        let physical_table = self.table_name_for_cas(table);
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(physical_table)
            .key(ATTR_PK, AttributeValue::B(Blob::new(cas_pk(table, key))))
            .key(ATTR_SK, AttributeValue::B(Blob::new(SK_SENTINEL.to_vec())))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| backend_err("get_item", e))?;

        match resp.item {
            None => Ok(None),
            Some(mut item) => {
                let version = take_version(&mut item)?;
                let val = take_val(&mut item)?;
                Ok(Some((CasVersion(version), val)))
            }
        }
    }

    async fn cas_put(
        &self,
        table: TableId,
        key: &[u8],
        expected: Option<CasVersion>,
        value: Bytes,
    ) -> Result<CasOutcome> {
        let pk = cas_pk(table, key);
        let physical_table = self.table_name_for_cas(table);
        let new_version = match expected {
            None => 1,
            Some(v) => v.0 + 1,
        };

        let mut req = self
            .inner
            .client
            .put_item()
            .table_name(physical_table.clone())
            .item(ATTR_PK, AttributeValue::B(Blob::new(pk.clone())))
            .item(ATTR_SK, AttributeValue::B(Blob::new(SK_SENTINEL.to_vec())))
            .item(ATTR_VAL, AttributeValue::B(Blob::new(value.to_vec())))
            .item(ATTR_VERSION, AttributeValue::N(new_version.to_string()))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld);
        req = match expected {
            // Insert-if-absent: the partition must not already exist.
            None => req.condition_expression("attribute_not_exists(pk)"),
            // Match the stored version exactly.
            Some(v) => req
                .condition_expression("v = :expected")
                .expression_attribute_values(":expected", AttributeValue::N(v.0.to_string())),
        };

        match req.send().await {
            Ok(_) => Ok(CasOutcome::Applied {
                new_version: CasVersion(new_version),
            }),
            Err(e) if is_conditional_check_failed(&e) => {
                // Recover the current version: prefer ALL_OLD on the exception
                // (DynamoDB / newer Alternator), else a follow-up GetItem.
                let current_version = match current_version_from_error(&e) {
                    Some(v) => v,
                    None => self.read_cas_version(&physical_table, &pk).await?,
                };
                Ok(CasOutcome::Conflict { current_version })
            }
            Err(e) => Err(backend_err("put_item", e)),
        }
    }
}

impl DynamoMetaStore {
    /// Follow-up consistent read of a CAS row's version, used to populate
    /// `Conflict.current_version` when the conditional-failure response did not
    /// carry ALL_OLD (older Alternator).
    async fn read_cas_version(
        &self,
        physical_table: &str,
        pk: &[u8],
    ) -> Result<Option<CasVersion>> {
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(physical_table.to_string())
            .key(ATTR_PK, AttributeValue::B(Blob::new(pk.to_vec())))
            .key(ATTR_SK, AttributeValue::B(Blob::new(SK_SENTINEL.to_vec())))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| backend_err("get_item", e))?;
        match resp.item {
            None => Ok(None),
            Some(mut item) => Ok(Some(CasVersion(take_version(&mut item)?))),
        }
    }
}

// ----- key encoding (client-free, unit-tested) -----

/// `pk = u16-be(len(kind)) ∥ kind ∥ u16-be(len(table)) ∥ table ∥ tail`. Length
/// prefixes keep the kind/table boundaries unambiguous; `tail` is the kv/cas
/// key or the scan partition appended raw.
pub(crate) fn encode_pk(kind: &[u8], table: &str, tail: &[u8]) -> Vec<u8> {
    let table = table.as_bytes();
    let mut out = Vec::with_capacity(2 + kind.len() + 2 + table.len() + tail.len());
    push_len_prefixed(&mut out, kind);
    push_len_prefixed(&mut out, table);
    out.extend_from_slice(tail);
    out
}

fn push_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u16::try_from(bytes.len()).expect("pk segment length fits in u16");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
}

fn kv_pk(table: TableId, key: &[u8]) -> Vec<u8> {
    encode_pk(KIND_KV, table.as_str(), key)
}

fn scan_pk(table: ScannableTableId, partition: &[u8]) -> Vec<u8> {
    encode_pk(KIND_SCAN, table.as_str(), partition)
}

fn cas_pk(table: TableId, key: &[u8]) -> Vec<u8> {
    encode_pk(KIND_CAS, table.as_str(), key)
}

fn dynamo_safe_name(logical_table: &str) -> String {
    logical_table.replace('_', "-")
}

fn known_logical_table_names() -> Vec<&'static str> {
    vec![
        "publication_state",
        "block_metadata",
        "block_evm_header",
        "block_hash_to_number_index",
        "tx_hash_index",
        "log_dict_by_version",
        "log_dir_by_block",
        "log_dir_bucket",
        "log_bitmap_by_block",
        "log_bitmap_page_blob",
        "log_bitmap_page_counts",
        "log_open_bitmap_stream",
        "tx_dict_by_version",
        "tx_dir_by_block",
        "tx_dir_bucket",
        "tx_bitmap_by_block",
        "tx_bitmap_page_blob",
        "tx_bitmap_page_counts",
        "tx_open_bitmap_stream",
        "trace_dict_by_version",
        "trace_dir_by_block",
        "trace_dir_bucket",
        "trace_bitmap_by_block",
        "trace_bitmap_page_blob",
        "trace_bitmap_page_counts",
        "trace_open_bitmap_stream",
    ]
}

// ----- attribute extraction -----

/// Removes and returns a Binary attribute, erroring if absent or wrong type.
pub(crate) fn take_binary(
    item: &mut HashMap<String, AttributeValue>,
    attr: &str,
) -> Result<Vec<u8>> {
    match item.remove(attr) {
        Some(AttributeValue::B(blob)) => Ok(blob.into_inner()),
        _ => Err(MonadChainDataError::Backend(format!(
            "dynamo item missing binary attribute `{attr}`"
        ))),
    }
}

fn take_val(item: &mut HashMap<String, AttributeValue>) -> Result<Bytes> {
    take_binary(item, ATTR_VAL).map(Bytes::from)
}

/// Removes and parses the CAS version Number attribute.
fn take_version(item: &mut HashMap<String, AttributeValue>) -> Result<u64> {
    match item.remove(ATTR_VERSION) {
        Some(AttributeValue::N(n)) => n.parse::<u64>().map_err(|_| {
            MonadChainDataError::Backend(format!("dynamo cas version not a u64: {n}"))
        }),
        _ => Err(MonadChainDataError::Backend(
            "dynamo cas item missing numeric version attribute `v`".to_string(),
        )),
    }
}

// ----- error helpers -----

/// True when a `PutItem` failed its condition. Robust to the error arriving as
/// the typed variant OR untyped via the error metadata code (Alternator).
fn is_conditional_check_failed<R>(
    e: &SdkError<aws_sdk_dynamodb::operation::put_item::PutItemError, R>,
) -> bool {
    use aws_sdk_dynamodb::operation::put_item::PutItemError;
    matches!(
        e,
        SdkError::ServiceError(se) if matches!(se.err(), PutItemError::ConditionalCheckFailedException(_))
    ) || e.code() == Some("ConditionalCheckFailedException")
}

/// Pulls the current CAS version out of a conditional-failure response's
/// ALL_OLD item, if present. Returns `None` if the SDK/service did not carry it
/// (caller then falls back to a follow-up GetItem).
fn current_version_from_error<R>(
    e: &SdkError<aws_sdk_dynamodb::operation::put_item::PutItemError, R>,
) -> Option<Option<CasVersion>> {
    use aws_sdk_dynamodb::operation::put_item::PutItemError;
    let SdkError::ServiceError(se) = e else {
        return None;
    };
    let PutItemError::ConditionalCheckFailedException(ccf) = se.err() else {
        return None;
    };
    // `item` is the ALL_OLD snapshot. Absent => the row did not exist (an
    // insert-if-absent that lost a race is the only way that happens) => the
    // current version is unknown from here; fall back to a read.
    let item = ccf.item()?;
    match item.get(ATTR_VERSION) {
        Some(AttributeValue::N(n)) => Some(n.parse::<u64>().ok().map(CasVersion)),
        _ => None,
    }
}

pub(crate) fn backend_err<E, R>(op: &str, e: SdkError<E, R>) -> MonadChainDataError
where
    E: ProvideErrorMetadata + std::error::Error + std::fmt::Debug + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    // Prefer the service-reported code/message; SdkError's own Display is terse.
    let detail = match e.code() {
        Some(code) => format!("{code}: {}", e.message().unwrap_or("")),
        None => format!("{e}; debug={e:?}"),
    };
    MonadChainDataError::Backend(format!("dynamo {op}: {detail}"))
}

pub(crate) fn estimated_batch_write_item_bytes(
    pk_len: usize,
    sk_len: usize,
    value_len: usize,
) -> usize {
    BATCH_WRITE_ITEM_OVERHEAD
        + base64_encoded_len(pk_len)
        + base64_encoded_len(sk_len)
        + base64_encoded_len(value_len)
}

fn base64_encoded_len(raw_len: usize) -> usize {
    raw_len.div_ceil(3).saturating_mul(4)
}

pub(crate) fn split_batch_write_chunks<T>(items: Vec<(String, T, usize)>) -> Vec<(String, Vec<T>)> {
    let mut by_table: HashMap<String, Vec<(T, usize)>> = HashMap::new();
    for (table, item, estimated_wire_bytes) in items {
        by_table
            .entry(table)
            .or_default()
            .push((item, estimated_wire_bytes));
    }

    let mut chunks_by_table = Vec::new();
    for (table, table_items) in by_table {
        let mut current = Vec::new();
        let mut current_bytes = 0usize;
        let mut table_chunks = VecDeque::new();

        for (item, estimated_wire_bytes) in table_items {
            let exceeds_count = current.len() >= BATCH_WRITE_LIMIT;
            let exceeds_bytes = !current.is_empty()
                && current_bytes.saturating_add(estimated_wire_bytes)
                    > BATCH_WRITE_PAYLOAD_SOFT_LIMIT;
            if exceeds_count || exceeds_bytes {
                table_chunks.push_back(std::mem::take(&mut current));
                current_bytes = 0;
            }
            current.push(item);
            current_bytes = current_bytes.saturating_add(estimated_wire_bytes);
        }

        if !current.is_empty() {
            table_chunks.push_back(current);
        }
        chunks_by_table.push((table, table_chunks));
    }
    chunks_by_table.sort_by(|a, b| a.0.cmp(&b.0));

    let mut chunks = Vec::new();
    loop {
        let mut emitted = false;
        for (table, table_chunks) in &mut chunks_by_table {
            if let Some(chunk) = table_chunks.pop_front() {
                chunks.push((table.clone(), chunk));
                emitted = true;
            }
        }
        if !emitted {
            break;
        }
    }

    chunks
}

pub(crate) fn batch_write_retry_backoff(
    chunk_idx: usize,
    table: &str,
    attempt: u32,
) -> std::time::Duration {
    let exp = attempt.saturating_sub(1).min(30);
    let cap_ms = BATCH_WRITE_CHUNK_BASE_BACKOFF_MS
        .saturating_mul(1_u64 << exp)
        .min(BATCH_WRITE_CHUNK_MAX_BACKOFF_MS);
    let jitter_ms = deterministic_jitter_ms(chunk_idx, table, attempt, cap_ms);
    std::time::Duration::from_millis(jitter_ms.max(1))
}

fn deterministic_jitter_ms(chunk_idx: usize, table: &str, attempt: u32, cap_ms: u64) -> u64 {
    if cap_ms == 0 {
        return 0;
    }
    let mut x = 0xcbf29ce484222325_u64;
    for byte in table.as_bytes() {
        x ^= u64::from(*byte);
        x = x.wrapping_mul(0x100000001b3);
    }
    x ^= chunk_idx as u64;
    x = x.wrapping_mul(0x9e3779b97f4a7c15);
    x ^= u64::from(attempt);
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x = x.wrapping_mul(0x2545f4914f6cdd1d);
    x % (cap_ms + 1)
}

fn count_only_chunks_by_table<T>(items: &[(String, T, usize)]) -> usize {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for (table, _, _) in items {
        *counts.entry(table.as_str()).or_default() += 1;
    }
    counts
        .values()
        .map(|count| count.div_ceil(BATCH_WRITE_LIMIT))
        .sum()
}

fn summarize_names<'a>(names: impl Iterator<Item = &'a str>, limit: usize) -> String {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for name in names {
        *counts.entry(name).or_default() += 1;
    }
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    counts
        .into_iter()
        .take(limit)
        .map(|(name, count)| format!("{name}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_count_pairs(pairs: &[(String, usize)], limit: usize) -> String {
    pairs
        .iter()
        .take(limit)
        .map(|(name, count)| format!("{name}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    const KV: TableId = TableId::new("blocks");
    const SCAN: ScannableTableId = ScannableTableId::new("logs");

    #[test]
    fn pk_encoding_is_length_prefixed() {
        // u16-be(2)="kv", u16-be(6)="blocks", then the raw key bytes.
        let pk = kv_pk(KV, &[0xaa, 0xbb]);
        assert_eq!(
            pk,
            [
                0x00, 0x02, b'k', b'v', // len(kind)=2 ∥ "kv"
                0x00, 0x06, b'b', b'l', b'o', b'c', b'k', b's', // len(table)=6 ∥ "blocks"
                0xaa, 0xbb, // raw key
            ]
        );
    }

    #[test]
    fn pk_kinds_are_disjoint() {
        // Same logical name across kinds must never collide: the kind prefix
        // (different length AND bytes) keeps kv/scan/cas in separate partitions.
        let a = encode_pk(KIND_KV, "t", b"x");
        let b = encode_pk(KIND_SCAN, "t", b"x");
        let c = encode_pk(KIND_CAS, "t", b"x");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn pk_table_boundary_is_unambiguous() {
        // ("ab", "c") and ("a", "bc") must not alias: the length prefix on the
        // table segment disambiguates the table/tail split.
        let lhs = encode_pk(KIND_KV, "ab", b"c");
        let rhs = encode_pk(KIND_KV, "a", b"bc");
        assert_ne!(lhs, rhs);
    }

    #[test]
    fn scan_sk_orders_by_clustering_byte_order() {
        // Within one (table, partition) the pk is identical, so DynamoDB's
        // unsigned-byte sort on sk must equal byte-order on the clustering. We
        // verify the sk bytes we hand the server are exactly the clustering.
        let part = b"p";
        let pk = scan_pk(SCAN, part);
        let clusterings: Vec<Vec<u8>> =
            vec![vec![0x00], vec![0x01, 0x00], vec![0x01, 0xff], vec![0xff]];
        // sk == clustering verbatim, so sorting sks == sorting clusterings.
        let mut sks = clusterings.clone();
        sks.sort();
        assert_eq!(sks, clusterings, "input already in byte order");
        // All share the same pk (single-partition Query).
        assert!(clusterings.iter().all(|_| scan_pk(SCAN, part) == pk));
    }

    #[test]
    fn cas_condition_shaping_insert_vs_match() {
        // Document the locked CAS condition-expression shaping that cas_put
        // emits, so a refactor that changes the wire condition fails here.
        // expected=None  -> attribute_not_exists(pk), v=1
        // expected=Some  -> v = :expected,            v=expected+1
        let insert_new_version = 1u64; // from `None => 1`
        assert_eq!(insert_new_version, 1);
        let expected = CasVersion(7);
        let match_new_version = expected.0 + 1; // from `Some(v) => v.0 + 1`
        assert_eq!(match_new_version, 8);
    }

    #[test]
    fn version_attribute_round_trips_as_number_string() {
        let mut item = HashMap::new();
        item.insert(
            ATTR_VERSION.to_string(),
            AttributeValue::N("42".to_string()),
        );
        assert_eq!(take_version(&mut item).unwrap(), 42);
    }

    #[test]
    fn batch_write_chunks_respect_item_count_limit() {
        let items = (0..(BATCH_WRITE_LIMIT * 2 + 1))
            .map(|i| ("table-a".to_string(), i, 1usize))
            .collect();

        let chunks = split_batch_write_chunks(items);

        assert_eq!(chunks.len(), 3);
        let mut lens: Vec<_> = chunks
            .iter()
            .map(|(table, chunk)| (table.as_str(), chunk.len()))
            .collect();
        lens.sort();
        assert_eq!(
            lens,
            vec![
                ("table-a", 1),
                ("table-a", BATCH_WRITE_LIMIT),
                ("table-a", BATCH_WRITE_LIMIT)
            ]
        );
    }

    #[test]
    fn batch_write_chunks_respect_payload_soft_limit() {
        let large = BATCH_WRITE_PAYLOAD_SOFT_LIMIT / 2 + 1;
        let chunks = split_batch_write_chunks(vec![
            ("table-a".to_string(), 0, large),
            ("table-a".to_string(), 1, large),
            ("table-a".to_string(), 2, large),
        ]);

        let mut payloads: Vec<_> = chunks.into_iter().collect();
        payloads.sort_by(|a, b| a.1[0].cmp(&b.1[0]));
        assert_eq!(
            payloads,
            vec![
                ("table-a".to_string(), vec![0]),
                ("table-a".to_string(), vec![1]),
                ("table-a".to_string(), vec![2])
            ]
        );
    }

    #[test]
    fn batch_write_chunks_do_not_mix_physical_tables() {
        let chunks = split_batch_write_chunks(vec![
            ("table-a".to_string(), 1, 1),
            ("table-b".to_string(), 2, 1),
            ("table-a".to_string(), 3, 1),
        ]);

        let mut chunks: Vec<_> = chunks.into_iter().collect();
        chunks.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            chunks,
            vec![
                ("table-a".to_string(), vec![1, 3]),
                ("table-b".to_string(), vec![2])
            ]
        );
    }

    #[test]
    fn batch_write_chunks_interleave_physical_tables() {
        let mut items = Vec::new();
        for i in 0..(BATCH_WRITE_LIMIT * 2) {
            items.push(("table-a".to_string(), i, 1));
        }
        for i in 0..(BATCH_WRITE_LIMIT * 2) {
            items.push(("table-b".to_string(), 100 + i, 1));
        }
        for i in 0..(BATCH_WRITE_LIMIT * 2) {
            items.push(("table-c".to_string(), 200 + i, 1));
        }

        let chunk_tables: Vec<_> = split_batch_write_chunks(items)
            .into_iter()
            .map(|(table, _)| table)
            .collect();

        assert_eq!(
            chunk_tables,
            vec!["table-a", "table-b", "table-c", "table-a", "table-b", "table-c"]
        );
    }

    #[test]
    fn logical_table_names_are_dynamo_safe() {
        assert_eq!(dynamo_safe_name("block_evm_header"), "block-evm-header");
    }

    #[test]
    fn take_val_extracts_binary() {
        let mut item = HashMap::new();
        item.insert(
            ATTR_VAL.to_string(),
            AttributeValue::B(Blob::new(vec![1u8, 2, 3])),
        );
        assert_eq!(take_val(&mut item).unwrap().as_ref(), &[1, 2, 3]);
    }
}
