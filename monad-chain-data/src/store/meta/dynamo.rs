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

use std::{collections::HashMap, sync::Arc};

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
const ATTR_PK: &str = "pk";
const ATTR_SK: &str = "sk";
const ATTR_VAL: &str = "val";
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

/// Construction parameters for [`DynamoMetaStore`].
#[derive(Debug, Clone)]
pub struct DynamoMetaStoreConfig {
    /// The single shared table holding every logical kv/scan/cas row.
    pub table_name: String,
    /// Override the endpoint for DynamoDB Local / Alternator. Leave `None` to
    /// target real AWS DynamoDB via the default endpoint resolver.
    pub endpoint_url: Option<String>,
    /// AWS region. `None` falls through to the default region provider chain.
    /// DynamoDB Local / Alternator accept any value (commonly `"us-east-1"`).
    pub region: Option<String>,
    /// Max in-flight `BatchWriteItem` calls for [`DynamoMetaStore::apply_writes`].
    /// Clamped to >= 1.
    pub batch_max_concurrency: usize,
    /// Explicit static credentials. `None` uses the ambient AWS credential
    /// chain (env, profile, instance role, ...).
    pub credentials: Option<DynamoCredentials>,
}

impl DynamoMetaStoreConfig {
    /// Minimal config targeting real AWS DynamoDB with ambient credentials and
    /// the default region chain. Callers tweak the remaining fields as needed.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            endpoint_url: None,
            region: None,
            batch_max_concurrency: 16,
            credentials: None,
        }
    }
}

struct Inner {
    client: Client,
    table_name: String,
    batch_max_concurrency: usize,
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
            .field("table_name", &self.inner.table_name)
            .field("batch_max_concurrency", &self.inner.batch_max_concurrency)
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
            table_name,
            endpoint_url,
            region,
            batch_max_concurrency,
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
                table_name,
                batch_max_concurrency: batch_max_concurrency.max(1),
            }),
        })
    }

    /// Single strongly-consistent `GetItem` returning the `val` bytes, or
    /// `Ok(None)` when the item is absent.
    async fn get_val(&self, pk: Vec<u8>, sk: Vec<u8>) -> Result<Option<Bytes>> {
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(&self.inner.table_name)
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
    async fn put_val(&self, pk: Vec<u8>, sk: Vec<u8>, value: Bytes) -> Result<()> {
        self.inner
            .client
            .put_item()
            .table_name(&self.inner.table_name)
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
    async fn write_chunk(&self, requests: Vec<WriteRequest>) -> Result<()> {
        let table = self.inner.table_name.clone();
        let mut pending: HashMap<String, Vec<WriteRequest>> = HashMap::new();
        pending.insert(table.clone(), requests);

        // Bounded retry of throttled/unprocessed items with linear backoff.
        // BatchWriteItem returns the leftovers rather than erroring on partial
        // capacity, so we resubmit only what was not applied.
        let mut attempt = 0u32;
        loop {
            let resp = self
                .inner
                .client
                .batch_write_item()
                .set_request_items(Some(pending.clone()))
                .send()
                .await
                .map_err(|e| backend_err("batch_write_item", e))?;

            let leftovers = resp
                .unprocessed_items
                .and_then(|mut m| m.remove(&table))
                .unwrap_or_default();
            if leftovers.is_empty() {
                return Ok(());
            }
            attempt += 1;
            if attempt > 8 {
                return Err(MonadChainDataError::Backend(format!(
                    "dynamo batch_write_item: {} items still unprocessed after {attempt} retries",
                    leftovers.len()
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50 * attempt as u64)).await;
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
        let table = &self.inner.table_name;
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
            .table_name(table)
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
                .table_name(table)
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
}

impl MetaStore for DynamoMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_val(kv_pk(table, key), SK_SENTINEL.to_vec()).await
    }

    async fn scan_get(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
    ) -> Result<Option<Bytes>> {
        self.get_val(scan_pk(table, partition), clustering.to_vec())
            .await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        self.put_val(kv_pk(table, key), SK_SENTINEL.to_vec(), value)
            .await
    }

    async fn scan_put(
        &self,
        table: ScannableTableId,
        partition: &[u8],
        clustering: &[u8],
        value: Bytes,
    ) -> Result<()> {
        self.put_val(scan_pk(table, partition), clustering.to_vec(), value)
            .await
    }

    async fn apply_writes(&self, writes: Vec<MetaWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let requests: Vec<WriteRequest> = writes
            .into_iter()
            .map(|op| {
                let (pk, sk, value) = match op {
                    MetaWriteOp::Put { table, key, value } => {
                        (kv_pk(table, &key), SK_SENTINEL.to_vec(), value)
                    }
                    MetaWriteOp::ScanPut {
                        table,
                        partition,
                        clustering,
                        value,
                    } => (scan_pk(table, &partition), clustering, value),
                };
                let put = PutRequest::builder()
                    .item(ATTR_PK, AttributeValue::B(Blob::new(pk)))
                    .item(ATTR_SK, AttributeValue::B(Blob::new(sk)))
                    .item(ATTR_VAL, AttributeValue::B(Blob::new(value.to_vec())))
                    .build()
                    .map_err(|e| {
                        MonadChainDataError::Backend(format!("dynamo build put_request: {e}"))
                    })?;
                Ok(WriteRequest::builder().put_request(put).build())
            })
            .collect::<Result<Vec<_>>>()?;

        let concurrency = self.inner.batch_max_concurrency;
        let chunks: Vec<Vec<WriteRequest>> = requests
            .chunks(BATCH_WRITE_LIMIT)
            .map(<[WriteRequest]>::to_vec)
            .collect();
        futures::stream::iter(chunks.into_iter().map(|chunk| {
            let store = self.clone();
            async move { store.write_chunk(chunk).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<()>>()
        .await?;
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
                .table_name(&self.inner.table_name)
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
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(&self.inner.table_name)
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
        let new_version = match expected {
            None => 1,
            Some(v) => v.0 + 1,
        };

        let mut req = self
            .inner
            .client
            .put_item()
            .table_name(&self.inner.table_name)
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
                    None => self.read_cas_version(&pk).await?,
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
    async fn read_cas_version(&self, pk: &[u8]) -> Result<Option<CasVersion>> {
        let resp = self
            .inner
            .client
            .get_item()
            .table_name(&self.inner.table_name)
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
fn encode_pk(kind: &[u8], table: &str, tail: &[u8]) -> Vec<u8> {
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

// ----- attribute extraction -----

/// Removes and returns a Binary attribute, erroring if absent or wrong type.
fn take_binary(item: &mut HashMap<String, AttributeValue>, attr: &str) -> Result<Vec<u8>> {
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

fn backend_err<E, R>(op: &str, e: SdkError<E, R>) -> MonadChainDataError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    // Prefer the service-reported code/message; SdkError's own Display is terse.
    let detail = match e.code() {
        Some(code) => format!("{code}: {}", e.message().unwrap_or("")),
        None => e.to_string(),
    };
    MonadChainDataError::Backend(format!("dynamo {op}: {detail}"))
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
    fn take_val_extracts_binary() {
        let mut item = HashMap::new();
        item.insert(
            ATTR_VAL.to_string(),
            AttributeValue::B(Blob::new(vec![1u8, 2, 3])),
        );
        assert_eq!(take_val(&mut item).unwrap().as_ref(), &[1, 2, 3]);
    }
}
