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

//! DynamoDB-API-compatible [`BlobStore`] backend that stores each blob as a row
//! of fixed-size chunks instead of one large object.
//!
//! Talks to the DynamoDB HTTP API -- AWS DynamoDB, DynamoDB Local, and the main
//! portability target, **ScyllaDB Alternator** (point `endpoint_url` at the
//! Alternator port). It shares the metastore's client setup, key encoding, and
//! BatchWriteItem chunking machinery (see [`crate::store::meta`]'s dynamo
//! backend); this module adds only the blob-specific chunking.
//!
//! ## Why chunk
//!
//! The [`S3BlobStore`](super::S3BlobStore) keeps a whole per-block blob as one
//! object and serves [`BlobStore::read_range`] with an HTTP `Range` request.
//! DynamoDB/Alternator has no server-side byte-range read and caps an item at
//! 400 KiB, so a 16 MiB block blob cannot live in one item anyway. We instead
//! split each logical blob into `chunk_size`-byte pieces, one DynamoDB item per
//! chunk, sharing a partition. A `read_range` then becomes a single-partition
//! `Query` over just the clustering (chunk-index) range that covers the request
//! -- the same "fetch only the bytes you need" property, expressed in the
//! DynamoDB data model. Everything above the [`BlobStore`] trait (the engine's
//! per-block coalescing and `BlockBlobHeader` offsets) is untouched: chunking is
//! an internal detail of this backend.
//!
//! ## Single-table design (a wire contract once data exists)
//!
//! Every chunk of every blob lives in one physical table. Schema mirrors the
//! metastore's (`pk` Binary HASH + `sk` Binary RANGE + `val` Binary):
//!
//! ```text
//! pk  = u16-be(len("blob")) ∥ "blob" ∥ u16-be(len(table)) ∥ table ∥ blob-key
//! sk  = u32-be(chunk_index)           -- fixed width so byte order == chunk order
//! val = the chunk's bytes
//! len = total blob length (Number)    -- on chunk 0 only
//! ```
//!
//! The `"blob"` kind prefix keeps blob partitions disjoint from the metastore's
//! `kv`/`scan`/`cas` rows even if they ever share a physical table. `len` on the
//! first chunk lets `read_range` validate bounds and tell a missing object
//! (`None`) from an out-of-range request (`Decode` error) with one small read,
//! exactly matching the [`BlobStore`] trait contract and the S3 backend.
//!
//! ## apply_writes is not atomic
//!
//! Like the S3 backend, a multi-chunk blob is several independent PutItems; a
//! partial write leaves orphan chunks, never a torn read. Chain-data blobs are
//! write-once and idempotent, and the publication-head CAS in the `MetaStore`
//! gates *visibility* only after all chunk writes have been acked, so a
//! partially written (then retried) blob is never observed by readers.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::{
    config::{Credentials, Region},
    error::ProvideErrorMetadata,
    primitives::Blob,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType, PutRequest,
        ScalarAttributeType, TableStatus, WriteRequest,
    },
    Client,
};
use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use tracing::{info, warn};

pub use crate::store::meta::dynamo::DynamoCredentials;
use crate::{
    error::{MonadChainDataError, Result},
    store::{
        blob::{BlobStore, BlobTableId, BlobWriteOp},
        meta::dynamo::{
            backend_err, batch_write_retry_backoff, encode_pk, estimated_batch_write_item_bytes,
            split_batch_write_chunks, take_binary, ATTR_PK, ATTR_SK, ATTR_VAL,
            BATCH_WRITE_CHUNK_MAX_RETRIES, BATCH_WRITE_LIMIT,
        },
    },
};

/// Key-kind discriminator folded into `pk`, disjoint from the metastore kinds.
const KIND_BLOB: &[u8] = b"blob";

/// Number attribute on chunk 0 carrying the total blob length.
const ATTR_LEN: &str = "len";

/// Default chunk size: 64 KiB.
///
/// Most chain-data blobs are tens of KiB (a single chunk -- so a small blob is
/// one item, just like one S3 object), while the rare large block blob (up to
/// ~16 MiB) splits into <=256 chunks. 64 KiB stays well under DynamoDB's 400 KiB
/// per-item cap, bounds `read_range` over-read to <=64 KiB on each end, and
/// keeps the item count (hence BatchWriteItem call count) modest.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Hard ceiling: DynamoDB rejects items larger than 400 KiB. Stay under it with
/// margin for `pk`/`sk`/`len`/framing overhead.
const MAX_CHUNK_SIZE: usize = 350 * 1024;

/// Construction parameters for [`DynamoBlobStore`].
#[derive(Debug, Clone)]
pub struct DynamoBlobStoreConfig {
    /// Physical DynamoDB table holding every blob chunk.
    pub table_name: String,
    /// Override the endpoint for DynamoDB Local / Alternator. `None` targets
    /// real AWS DynamoDB via the default endpoint resolver.
    pub endpoint_url: Option<String>,
    /// AWS region. `None` falls through to the default region provider chain.
    pub region: Option<String>,
    /// AWS profile name. `None` uses the SDK default profile/environment chain.
    pub profile: Option<String>,
    /// Max in-flight `BatchWriteItem` calls. Clamped to >= 1.
    pub batch_max_concurrency: usize,
    /// Bytes per chunk. Clamped to `1..=MAX_CHUNK_SIZE`. This is a wire contract:
    /// it sets the byte->chunk-index mapping, so it must not change for a table
    /// that already holds data.
    pub chunk_size: usize,
    /// Explicit static credentials. `None` uses the ambient AWS credential chain.
    pub credentials: Option<DynamoCredentials>,
}

impl DynamoBlobStoreConfig {
    /// Minimal config targeting real AWS DynamoDB with ambient credentials, the
    /// default region chain, and the default chunk size.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            endpoint_url: None,
            region: None,
            profile: None,
            batch_max_concurrency: 16,
            chunk_size: DEFAULT_CHUNK_SIZE,
            credentials: None,
        }
    }
}

struct Inner {
    client: Client,
    table_name: String,
    batch_max_concurrency: usize,
    chunk_size: usize,
}

#[derive(Default)]
struct BatchWriteProgress {
    started: AtomicUsize,
    completed: AtomicUsize,
    failed: AtomicUsize,
    retries: AtomicUsize,
    unprocessed_items: AtomicUsize,
}

impl BatchWriteProgress {
    fn mark_started(&self, attempt: u32) {
        if attempt == 0 {
            self.started.fetch_add(1, Ordering::Relaxed);
        } else {
            self.retries.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn mark_completed(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_unprocessed(&self, count: usize) {
        self.unprocessed_items.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> BatchWriteProgressSnapshot {
        BatchWriteProgressSnapshot {
            started: self.started.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            unprocessed_items: self.unprocessed_items.load(Ordering::Relaxed),
        }
    }
}

struct BatchWriteProgressSnapshot {
    started: usize,
    completed: usize,
    failed: usize,
    retries: usize,
    unprocessed_items: usize,
}

/// DynamoDB-API-compatible chunked [`BlobStore`]. Cheaply cloneable -- all state
/// lives behind an `Arc`, and the SDK `Client` is itself `Arc`-backed.
#[derive(Clone)]
pub struct DynamoBlobStore {
    inner: std::sync::Arc<Inner>,
}

impl std::fmt::Debug for DynamoBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoBlobStore")
            .field("table_name", &self.inner.table_name)
            .field("batch_max_concurrency", &self.inner.batch_max_concurrency)
            .field("chunk_size", &self.inner.chunk_size)
            .finish_non_exhaustive()
    }
}

impl DynamoBlobStore {
    /// Builds the SDK client from `config` and returns a ready store. Async
    /// because resolving the AWS credential/region chain performs I/O. Assumes
    /// the table already exists (see [`DynamoBlobStore::create_table`] for the
    /// opt-in test/dev provisioning helper).
    pub async fn new(config: DynamoBlobStoreConfig) -> Result<Self> {
        let DynamoBlobStoreConfig {
            table_name,
            endpoint_url,
            region,
            profile,
            batch_max_concurrency,
            chunk_size,
            credentials,
        } = config;

        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        }
        if let Some(profile) = profile {
            loader = loader.profile_name(profile);
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
                "DynamoBlobStoreConfig",
            ));
        }
        let sdk_config = loader.load().await;
        let client = Client::new(&sdk_config);

        Ok(Self::new_with_client(
            client,
            table_name,
            batch_max_concurrency,
            chunk_size,
        ))
    }

    /// Builds a store from an already-constructed DynamoDB client instead of
    /// resolving the AWS config chain and opening a new one. Sharing a client
    /// (e.g. from a [`DynamoMetaStore`](crate::store::DynamoMetaStore) via its
    /// `client()` accessor) lets a deployment running both the dynamo meta and
    /// blob backends use a single connection pool and credential provider.
    pub fn new_with_client(
        client: Client,
        table_name: impl Into<String>,
        batch_max_concurrency: usize,
        chunk_size: usize,
    ) -> Self {
        Self {
            inner: std::sync::Arc::new(Inner {
                client,
                table_name: table_name.into(),
                batch_max_concurrency: batch_max_concurrency.max(1),
                chunk_size: chunk_size.clamp(1, MAX_CHUNK_SIZE),
            }),
        }
    }

    fn table(&self) -> &str {
        &self.inner.table_name
    }

    /// Strongly-consistent read of chunk 0's `len` attribute -- the blob's total
    /// length. `Ok(None)` means the blob does not exist. Projects only `len` so
    /// chunk 0's bytes never come over the wire.
    async fn blob_len(&self, pk: &[u8]) -> Result<Option<usize>> {
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(self.table())
            .key(ATTR_PK, AttributeValue::B(Blob::new(pk.to_vec())))
            .key(ATTR_SK, AttributeValue::B(Blob::new(chunk_sk(0))))
            .consistent_read(true)
            .projection_expression("#l")
            .expression_attribute_names("#l", ATTR_LEN)
            .send()
            .await
            .map_err(|e| backend_err("get_item", e))?;
        match resp.item {
            None => Ok(None),
            Some(item) => match item.get(ATTR_LEN) {
                Some(AttributeValue::N(n)) => n.parse::<usize>().map(Some).map_err(|_| {
                    MonadChainDataError::Backend(format!("dynamo blob len not a usize: {n}"))
                }),
                _ => Err(MonadChainDataError::Backend(
                    "dynamo blob chunk 0 missing numeric `len` attribute".to_string(),
                )),
            },
        }
    }

    /// Drains a single-partition `Query` over the chunk-index range
    /// `[first, last]` (inclusive) and concatenates the chunk bytes in order.
    /// The returned buffer begins at byte `first * chunk_size`.
    async fn read_chunk_range(&self, pk: &[u8], first: u32, last: u32) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = self
                .inner
                .client
                .query()
                .table_name(self.table())
                .key_condition_expression("pk = :p AND sk BETWEEN :a AND :b")
                .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.to_vec())))
                .expression_attribute_values(":a", AttributeValue::B(Blob::new(chunk_sk(first))))
                .expression_attribute_values(":b", AttributeValue::B(Blob::new(chunk_sk(last))))
                .consistent_read(true)
                .scan_index_forward(true);
            if let Some(start) = &start_key {
                req = req.set_exclusive_start_key(Some(start.clone()));
            }
            let resp = req.send().await.map_err(|e| backend_err("query", e))?;
            for mut item in resp.items.unwrap_or_default() {
                buf.extend_from_slice(&take_binary(&mut item, ATTR_VAL)?);
            }
            match resp.last_evaluated_key {
                Some(lek) => start_key = Some(lek),
                None => break,
            }
        }
        Ok(buf)
    }

    /// Splits `value` into `chunk_size` pieces and builds one BatchWriteItem put
    /// request per chunk (`pk` shared, `sk` = chunk index, `val` = the bytes;
    /// `len` on chunk 0). An empty value still writes a single empty chunk 0 so
    /// the blob reads back as present-and-empty rather than missing.
    fn chunk_puts(&self, pk: Vec<u8>, value: &[u8]) -> Result<Vec<(String, WriteRequest, usize)>> {
        let table = self.table().to_string();
        let total = value.len();
        if value.is_empty() {
            return Ok(vec![self.build_chunk_put(
                &table,
                &pk,
                0,
                &[],
                Some(total),
            )?]);
        }
        value
            .chunks(self.inner.chunk_size)
            .enumerate()
            .map(|(i, chunk)| {
                let idx = u32::try_from(i).map_err(|_| {
                    MonadChainDataError::Backend(
                        "dynamo blob chunk index overflows u32".to_string(),
                    )
                })?;
                let len = if idx == 0 { Some(total) } else { None };
                self.build_chunk_put(&table, &pk, idx, chunk, len)
            })
            .collect()
    }

    fn build_chunk_put(
        &self,
        table: &str,
        pk: &[u8],
        idx: u32,
        chunk: &[u8],
        len: Option<usize>,
    ) -> Result<(String, WriteRequest, usize)> {
        let sk = chunk_sk(idx);
        let estimated = estimated_batch_write_item_bytes(pk.len(), sk.len(), chunk.len());
        let mut put = PutRequest::builder()
            .item(ATTR_PK, AttributeValue::B(Blob::new(pk.to_vec())))
            .item(ATTR_SK, AttributeValue::B(Blob::new(sk)))
            .item(ATTR_VAL, AttributeValue::B(Blob::new(chunk.to_vec())));
        if let Some(len) = len {
            put = put.item(ATTR_LEN, AttributeValue::N(len.to_string()));
        }
        let put = put
            .build()
            .map_err(|e| MonadChainDataError::Backend(format!("dynamo build put_request: {e}")))?;
        Ok((
            table.to_string(),
            WriteRequest::builder().put_request(put).build(),
            estimated,
        ))
    }

    /// Splits put requests into BatchWriteItem-legal chunks (<=25 items / payload
    /// soft limit, shared with the metastore) and runs them concurrently, each
    /// chunk retrying throttled/unprocessed items with bounded jittered backoff.
    async fn run_batch_writes(&self, requests: Vec<(String, WriteRequest, usize)>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let build_started = std::time::Instant::now();
        let request_count = requests.len();
        let estimated_wire_bytes = requests.iter().map(|(_, _, bytes)| *bytes).sum::<usize>();
        let chunks = split_batch_write_chunks(requests, BATCH_WRITE_LIMIT);
        let chunk_count = chunks.len();
        let concurrency = self.inner.batch_max_concurrency;
        info!(
            request_count,
            chunks = chunk_count,
            estimated_wire_bytes,
            concurrency,
            build_ms = build_started.elapsed().as_millis() as u64,
            "dynamo blob built BatchWriteItem requests"
        );
        let progress = Arc::new(BatchWriteProgress::default());
        let send_started = std::time::Instant::now();
        let result =
            futures::stream::iter(chunks.into_iter().enumerate().map(|(chunk_idx, chunk)| {
                let client = self.inner.client.clone();
                let progress = progress.clone();
                async move {
                    let (table, requests) = chunk;
                    write_one_chunk(&client, chunk_idx, table, requests, progress).await
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
                retry_attempts = snapshot.retries,
                unprocessed_items = snapshot.unprocessed_items,
                "dynamo blob BatchWriteItem stream failed"
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
            "dynamo blob completed BatchWriteItem requests"
        );
        Ok(())
    }

    /// Idempotent table provisioning for tests/dev ONLY -- never called by
    /// [`DynamoBlobStore::new`]. Creates the table with the `pk` (Binary HASH) +
    /// `sk` (Binary RANGE) schema in on-demand billing mode, treating
    /// `ResourceInUseException` as success, then polls until `ACTIVE`.
    pub async fn create_table(&self) -> Result<()> {
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
            .table_name(self.table())
            .attribute_definitions(attr(ATTR_PK))
            .attribute_definitions(attr(ATTR_SK))
            .key_schema(key(ATTR_PK, KeyType::Hash))
            .key_schema(key(ATTR_SK, KeyType::Range))
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;
        match create {
            Ok(_) => {}
            Err(e) if e.code() == Some("ResourceInUseException") => {}
            Err(e) => return Err(backend_err("create_table", e)),
        }
        for _ in 0..60 {
            let desc = self
                .inner
                .client
                .describe_table()
                .table_name(self.table())
                .send()
                .await
                .map_err(|e| backend_err("describe_table", e))?;
            if desc.table.and_then(|t| t.table_status) == Some(TableStatus::Active) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        Err(MonadChainDataError::Backend(format!(
            "dynamo create_table: table {} did not become ACTIVE in time",
            self.table()
        )))
    }

    /// Startup connectivity/schema check: the configured table exists, is
    /// `ACTIVE`, and has the expected binary `pk` HASH + binary `sk` RANGE keys.
    pub async fn validate_table(&self) -> Result<()> {
        let table = self.table();
        let desc = self
            .inner
            .client
            .describe_table()
            .table_name(table)
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
                "dynamo blob table {table} is not ACTIVE: {:?}",
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
                "dynamo blob table {table} schema mismatch: expected binary pk HASH + binary sk RANGE"
            )));
        }
        Ok(())
    }
}

impl BlobStore for DynamoBlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let requests = self.chunk_puts(blob_pk(table, key), &value)?;
        self.run_batch_writes(requests).await
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let mut requests = Vec::new();
        for BlobWriteOp { table, key, value } in writes {
            requests.extend(self.chunk_puts(blob_pk(table, &key), &value)?);
        }
        self.run_batch_writes(requests).await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let pk = blob_pk(table, key);
        let mut buf = Vec::new();
        let mut present = false;
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let mut req = self
                .inner
                .client
                .query()
                .table_name(self.table())
                .key_condition_expression("pk = :p")
                .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.clone())))
                .consistent_read(true)
                .scan_index_forward(true);
            if let Some(start) = &start_key {
                req = req.set_exclusive_start_key(Some(start.clone()));
            }
            let resp = req.send().await.map_err(|e| backend_err("query", e))?;
            for mut item in resp.items.unwrap_or_default() {
                present = true;
                buf.extend_from_slice(&take_binary(&mut item, ATTR_VAL)?);
            }
            match resp.last_evaluated_key {
                Some(lek) => start_key = Some(lek),
                None => break,
            }
        }
        if present {
            Ok(Some(Bytes::from(buf)))
        } else {
            Ok(None)
        }
    }

    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        if start > end_exclusive {
            return Err(MonadChainDataError::Decode("invalid blob range"));
        }
        let pk = blob_pk(table, key);
        let Some(len) = self.blob_len(&pk).await? else {
            return Ok(None);
        };
        if start > len {
            return Err(MonadChainDataError::Decode("invalid blob range"));
        }
        // Clamp the end past EOF, matching the trait default and S3 backend.
        let end = end_exclusive.min(len);
        if start == end {
            return Ok(Some(Bytes::new()));
        }
        let (first, last, offset) = chunk_span(start, end, self.inner.chunk_size);
        let buf = self.read_chunk_range(&pk, first, last).await?;
        let want = end - start;
        if offset + want > buf.len() {
            // `len` claimed more bytes than the chunks hold: a partial/torn
            // write that should never be visible (publication CAS gates reads).
            return Err(MonadChainDataError::Decode(
                "blob range exceeds stored chunks",
            ));
        }
        Ok(Some(Bytes::from(buf).slice(offset..offset + want)))
    }
}

/// One BatchWriteItem call (<=25 items, single table), retrying throttled or
/// `UnprocessedItems` leftovers with the shared bounded jittered backoff.
async fn write_one_chunk(
    client: &Client,
    chunk_idx: usize,
    table: String,
    requests: Vec<WriteRequest>,
    progress: Arc<BatchWriteProgress>,
) -> Result<()> {
    let mut pending: HashMap<String, Vec<WriteRequest>> =
        HashMap::from([(table.clone(), requests)]);
    let mut attempt = 0u32;
    loop {
        progress.mark_started(attempt);
        let resp = match client
            .batch_write_item()
            .set_request_items(Some(pending.clone()))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                let error = backend_err("batch_write_item", e);
                attempt += 1;
                if attempt > BATCH_WRITE_CHUNK_MAX_RETRIES {
                    progress.mark_failed();
                    return Err(error);
                }
                warn!(
                    chunk_idx,
                    table = %table,
                    attempt,
                    %error,
                    "dynamo blob batch_write_item failed; retrying chunk"
                );
                tokio::time::sleep(batch_write_retry_backoff(chunk_idx, &table, attempt)).await;
                continue;
            }
        };
        let leftovers = resp
            .unprocessed_items
            .and_then(|mut m| m.remove(&table))
            .unwrap_or_default();
        if leftovers.is_empty() {
            progress.mark_completed();
            return Ok(());
        }
        progress.mark_unprocessed(leftovers.len());
        attempt += 1;
        if attempt > BATCH_WRITE_CHUNK_MAX_RETRIES {
            progress.mark_failed();
            return Err(MonadChainDataError::Backend(format!(
                "dynamo blob batch_write_item: {} items still unprocessed after {attempt} retries",
                leftovers.len()
            )));
        }
        tokio::time::sleep(batch_write_retry_backoff(chunk_idx, &table, attempt)).await;
        pending = HashMap::from([(table.clone(), leftovers)]);
    }
}

// ----- key encoding (client-free, unit-tested) -----

/// `pk` for a blob: the metastore's length-prefixed `(kind, table, tail)`
/// encoding with the `"blob"` kind and the blob key as the tail.
fn blob_pk(table: BlobTableId, key: &[u8]) -> Vec<u8> {
    encode_pk(KIND_BLOB, table.as_str(), key)
}

/// Sort key for a chunk: the index as fixed-width big-endian so DynamoDB's
/// unsigned-byte sort on `sk` equals chunk order.
fn chunk_sk(idx: u32) -> Vec<u8> {
    idx.to_be_bytes().to_vec()
}

/// For a byte range `[start, end)` (with `end > start`) returns the inclusive
/// chunk-index span `(first, last)` covering it and `start`'s offset within the
/// first chunk. The chunks `[first, last]` concatenated start at byte
/// `first * chunk_size`, so the requested bytes are `buf[offset..offset+(end-start)]`.
fn chunk_span(start: usize, end: usize, chunk_size: usize) -> (u32, u32, usize) {
    let first = start / chunk_size;
    let last = (end - 1) / chunk_size;
    (first as u32, last as u32, start - first * chunk_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: BlobTableId = BlobTableId::new("block_blob");

    #[test]
    fn blob_pk_uses_blob_kind_and_is_length_prefixed() {
        // u16-be(4)="blob", u16-be(10)="block_blob", then the raw key bytes.
        let pk = blob_pk(TABLE, &[0xaa, 0xbb]);
        assert_eq!(
            pk,
            [
                0x00, 0x04, b'b', b'l', b'o', b'b', // len(kind)=4 ∥ "blob"
                0x00, 0x0a, b'b', b'l', b'o', b'c', b'k', b'_', b'b', b'l', b'o',
                b'b', // len(table)=10 ∥ "block_blob"
                0xaa, 0xbb, // raw key
            ]
        );
    }

    #[test]
    fn chunk_sk_is_fixed_width_byte_ordered() {
        // Byte order of the sk must equal numeric chunk order.
        let mut sks: Vec<Vec<u8>> = vec![chunk_sk(0), chunk_sk(255), chunk_sk(256), chunk_sk(1)];
        sks.sort();
        assert_eq!(
            sks,
            vec![chunk_sk(0), chunk_sk(1), chunk_sk(255), chunk_sk(256)]
        );
        assert_eq!(chunk_sk(256), vec![0x00, 0x00, 0x01, 0x00]);
    }

    #[test]
    fn chunk_span_single_chunk() {
        // [2, 6) within a 4-byte chunking spans only chunk 0, offset 2.
        assert_eq!(chunk_span(2, 6, 4), (0, 1, 2));
        assert_eq!(chunk_span(0, 4, 4), (0, 0, 0));
        assert_eq!(chunk_span(4, 8, 4), (1, 1, 0));
    }

    #[test]
    fn chunk_span_crosses_chunks() {
        // [7, 9) with chunk size 4: chunk 1 (bytes 4..8) and chunk 2 (8..12),
        // offset 3 into the first chunk.
        assert_eq!(chunk_span(7, 9, 4), (1, 2, 3));
        // A range fully inside a later chunk.
        assert_eq!(chunk_span(10, 12, 4), (2, 2, 2));
    }

    #[test]
    fn chunk_span_reassembles_to_request_window() {
        // Property: for the assembled buffer starting at first*chunk_size, the
        // window [offset, offset+(end-start)) recovers the original [start, end).
        let chunk_size = 64 * 1024;
        for &(start, end) in &[(0usize, 1usize), (100, 200_000), (64 * 1024, 64 * 1024 + 1)] {
            let (first, _last, offset) = chunk_span(start, end, chunk_size);
            assert_eq!(first as usize * chunk_size + offset, start);
        }
    }
}
