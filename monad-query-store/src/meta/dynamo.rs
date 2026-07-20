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

//! DynamoDB-API-compatible [`MetaStore`] backend: AWS DynamoDB, DynamoDB
//! Local, and ScyllaDB Alternator (point `endpoint_url` at the Alternator port).
//!
//! Wire contract: rows are `(pk: Binary, sk: Binary, val: Binary)`, with the
//! key kind (`kv`/`scan`) and logical table name length-prefixed into `pk`
//! (see [`encode_pk`]); `sk` is the scan clustering, or a 0x00 sentinel for kv
//! rows. Binary keys sort by unsigned byte order, matching what `scan_keys`
//! relies on, so a scan is a single-partition `Query`.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use aws_sdk_dynamodb::{
    primitives::Blob,
    types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest},
    Client,
};
use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use monad_query_errors::{QueryError, Result};
use tracing::{debug, info, warn};

use crate::{
    aws::load_sdk_config,
    dynamo_common::{
        backend_err, create_pk_sk_table, encode_pk, estimated_batch_write_item_bytes,
        run_batch_write_chunks, split_batch_write_chunks, summarize_names, take_binary,
        validate_pk_sk_table, ClientRing, SharedDynamoConnection, ATTR_PK, ATTR_SK, ATTR_VAL,
        BATCH_WRITE_LIMIT, BATCH_WRITE_PAYLOAD_SOFT_LIMIT,
    },
    meta::{MetaStore, MetaWriteOp, ScannableTableId, TableId},
};

/// Explicit static credentials, for DynamoDB Local / Alternator deployments
/// with no ambient AWS credential chain (they accept any non-empty pair).
pub type DynamoCredentials = crate::aws::StaticCredentials;

/// Key-kind discriminators folded into `pk`.
const KIND_KV: &[u8] = b"kv";
const KIND_SCAN: &[u8] = b"scan";

/// Sort-key sentinel for kv rows, whose `pk` alone identifies them.
const SK_SENTINEL: &[u8] = &[0x00];

/// Physical DynamoDB table mapping for logical metadata tables.
#[derive(Debug, Clone)]
pub enum DynamoTableLayout {
    /// All kv/scan/cas rows share one physical DynamoDB table.
    Single { table_name: String },
    /// Each logical table id maps to one physical DynamoDB table named
    /// `{prefix}-{logical-table-name}` after Dynamo-safe normalization.
    /// `logical_names` is the engine-supplied catalog of logical table names to
    /// provision; this crate stays generic and never hard-codes the set.
    PerLogicalTable {
        prefix: String,
        logical_names: Vec<String>,
    },
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
    /// Endpoints for DynamoDB Local / Alternator; empty targets real AWS.
    /// Multiple endpoints get one client each, round-robined to spread
    /// coordinator load (Alternator nodes are peers).
    pub endpoint_urls: Vec<String>,
    /// AWS region. `None` falls through to the default region provider chain.
    /// DynamoDB Local / Alternator accept any value (commonly `"us-east-1"`).
    pub region: Option<String>,
    /// AWS profile name. `None` uses the SDK default profile/environment chain.
    pub profile: Option<String>,
    /// Max in-flight `BatchWriteItem` calls for [`DynamoMetaStore::apply_writes`].
    /// Clamped to >= 1.
    pub batch_max_concurrency: usize,
    /// Max in-flight `BatchWriteItem` calls per physical table for
    /// [`DynamoMetaStore::apply_writes`]. Clamped to >= 1.
    pub batch_table_max_concurrency: usize,
    /// Max write requests per `BatchWriteItem`. DynamoDB caps this at 25
    /// ([`BATCH_WRITE_LIMIT`]); Alternator defaults to 100 (tunable via
    /// `alternator_max_items_in_batch_write`). Verify with
    /// [`DynamoMetaStore::discover_batch_write_limit`] before exceeding 25 —
    /// an over-limit batch fails non-retryably. Clamped to >= 1.
    pub batch_write_max_items: usize,
    /// Explicit static credentials. `None` uses the ambient AWS credential
    /// chain (env, profile, instance role, ...).
    pub credentials: Option<DynamoCredentials>,
}

impl DynamoMetaStoreConfig {
    /// Minimal config targeting real AWS DynamoDB with ambient credentials
    /// and the default region chain.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_layout: DynamoTableLayout::single(table_name),
            endpoint_urls: Vec::new(),
            region: None,
            profile: None,
            batch_max_concurrency: 16,
            batch_table_max_concurrency: 16,
            batch_write_max_items: BATCH_WRITE_LIMIT,
            credentials: None,
        }
    }
}

struct Inner {
    /// One client per configured endpoint (>=1), round-robined.
    ring: ClientRing,
    table_layout: DynamoTableLayout,
    batch_max_concurrency: usize,
    batch_table_max_concurrency: usize,
    /// Effective max write requests per `BatchWriteItem`; atomic so
    /// [`DynamoMetaStore::discover_batch_write_limit`] can raise it. `Arc`d so
    /// a co-deployed dynamo blob store shares the probed value.
    batch_write_max_items: Arc<AtomicUsize>,
    /// Configured endpoint URLs, retained so a co-deployed dynamo blob store can
    /// recognize when it shares this store's endpoint(s) and skip its warn.
    endpoint_urls: Arc<Vec<String>>,
}

/// DynamoDB-API-compatible [`MetaStore`]. Cheaply cloneable (`Arc`-backed).
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
    /// Builds the SDK clients (async: resolving the AWS credential/region
    /// chain performs I/O). Assumes the tables already exist; see
    /// [`DynamoMetaStore::create_table`] for the test/dev provisioning helper.
    pub async fn new(config: DynamoMetaStoreConfig) -> Result<Self> {
        let DynamoMetaStoreConfig {
            table_layout,
            endpoint_urls,
            region,
            profile,
            batch_max_concurrency,
            batch_table_max_concurrency,
            batch_write_max_items,
            credentials,
        } = config;

        // Resolve the credential/region chain once; derive one client per
        // endpoint by overriding only the endpoint.
        let sdk_config =
            load_sdk_config(region, profile, credentials, "DynamoMetaStoreConfig").await;

        let clients: Vec<Client> = if endpoint_urls.is_empty() {
            vec![Client::new(&sdk_config)]
        } else {
            endpoint_urls
                .iter()
                .map(|endpoint| {
                    let conf = aws_sdk_dynamodb::config::Builder::from(&sdk_config)
                        .endpoint_url(endpoint)
                        .build();
                    Client::from_conf(conf)
                })
                .collect()
        };

        Ok(Self {
            inner: Arc::new(Inner {
                ring: ClientRing::new(clients),
                table_layout,
                batch_max_concurrency: batch_max_concurrency.max(1),
                batch_table_max_concurrency: batch_table_max_concurrency.max(1),
                batch_write_max_items: Arc::new(AtomicUsize::new(batch_write_max_items.max(1))),
                endpoint_urls: Arc::new(endpoint_urls),
            }),
        })
    }

    /// Round-robin one of the per-endpoint clients.
    fn round_robin_client(&self) -> &Client {
        self.inner.ring.next_client()
    }

    /// Connection state (client ring + probed batch-write limit) for sharing
    /// with a co-deployed dynamo blob backend.
    pub fn shared_connection(&self) -> SharedDynamoConnection {
        SharedDynamoConnection {
            ring: self.inner.ring.clone(),
            batch_write_max_items: self.inner.batch_write_max_items.clone(),
            endpoint_urls: self.inner.endpoint_urls.clone(),
        }
    }

    fn table_name_for_kv(&self, table: TableId) -> String {
        self.table_name_for(table.as_str())
    }

    fn table_name_for_scan(&self, table: ScannableTableId) -> String {
        self.table_name_for(table.as_str())
    }

    fn table_name_for(&self, logical_table: &str) -> String {
        match &self.inner.table_layout {
            DynamoTableLayout::Single { table_name } => table_name.clone(),
            DynamoTableLayout::PerLogicalTable { prefix, .. } => {
                format!("{prefix}-{}", dynamo_safe_name(logical_table))
            }
        }
    }

    fn physical_table_names(&self) -> Vec<String> {
        match &self.inner.table_layout {
            DynamoTableLayout::Single { table_name } => vec![table_name.clone()],
            DynamoTableLayout::PerLogicalTable {
                prefix,
                logical_names,
            } => logical_names
                .iter()
                .map(|logical| format!("{prefix}-{}", dynamo_safe_name(logical)))
                .collect(),
        }
    }

    /// Single strongly-consistent `GetItem` returning the `val` bytes, or
    /// `Ok(None)` when the item is absent.
    async fn get_row_data(
        &self,
        physical_table: String,
        partition_key: Vec<u8>,
        sort_key: Vec<u8>,
    ) -> Result<Option<Bytes>> {
        let resp = self
            .round_robin_client()
            .get_item()
            .table_name(physical_table)
            .key(ATTR_PK, AttributeValue::B(Blob::new(partition_key)))
            .key(ATTR_SK, AttributeValue::B(Blob::new(sort_key)))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| backend_err("get_item", e))?;

        match resp.item {
            None => Ok(None),
            Some(mut item) => take_binary(&mut item, ATTR_VAL).map(Bytes::from).map(Some),
        }
    }

    /// Single `PutItem` of `(pk, sk, val)`.
    async fn put_row_data(
        &self,
        physical_table: String,
        partition_key: Vec<u8>,
        sort_key: Vec<u8>,
        row_data: Bytes,
    ) -> Result<()> {
        self.round_robin_client()
            .put_item()
            .table_name(physical_table)
            .item(ATTR_PK, AttributeValue::B(Blob::new(partition_key)))
            .item(ATTR_SK, AttributeValue::B(Blob::new(sort_key)))
            // `Blob::new(row_data)` reclaims the backing Vec when uniquely owned.
            .item(ATTR_VAL, AttributeValue::B(Blob::new(row_data)))
            .send()
            .await
            .map_err(|e| backend_err("put_item", e))?;
        Ok(())
    }

    /// Idempotent table provisioning for tests/dev ONLY -- never called by
    /// [`DynamoMetaStore::new`]. Creates the binary `pk` HASH + binary `sk`
    /// RANGE schema in on-demand billing mode and polls until `ACTIVE`.
    pub async fn create_table(&self) -> Result<()> {
        for table in self.physical_table_names() {
            create_pk_sk_table(&self.inner.ring, &table).await?;
        }
        Ok(())
    }

    /// Startup connectivity/schema check. Verifies the configured table exists,
    /// is ACTIVE, and has the expected binary `pk` hash + binary `sk` range
    /// key schema before ingest workers begin writing.
    pub async fn validate_table(&self) -> Result<()> {
        let tables = self.physical_table_names();
        // batch_table_max_concurrency is clamped >= 1 at construction and the
        // layout always yields at least one table, so this stays >= 1.
        let concurrency = self.inner.batch_table_max_concurrency.min(tables.len());
        futures::stream::iter(tables)
            .map(|table| async move { validate_pk_sk_table(&self.inner.ring, &table).await })
            .buffer_unordered(concurrency)
            .try_collect::<()>()
            .await
    }

    /// Probe and set the effective `BatchWriteItem` item limit by sending
    /// `candidate` deletes of non-existent keys (idempotent no-ops). If the
    /// backend rejects the batch, falls back to [`BATCH_WRITE_LIMIT`]; a
    /// transport-class failure (timeout, dispatch) is an error — pinning the
    /// fallback for the process lifetime would silently shrink every batch
    /// over a transient outage. Call once at startup after the table is
    /// ACTIVE.
    pub async fn discover_batch_write_limit(&self, candidate: usize) -> Result<usize> {
        let candidate = candidate.max(1);
        if candidate <= BATCH_WRITE_LIMIT {
            self.inner
                .batch_write_max_items
                .store(candidate, Ordering::Relaxed);
            return Ok(candidate);
        }
        let table = self
            .physical_table_names()
            .into_iter()
            .next()
            .expect("layout yields at least one physical table");
        let probe: Vec<WriteRequest> = (0..candidate)
            .map(|i| {
                let pk = format!("__alternator_batch_probe__#{i}").into_bytes();
                let delete = DeleteRequest::builder()
                    .key(ATTR_PK, AttributeValue::B(Blob::new(pk)))
                    .key(ATTR_SK, AttributeValue::B(Blob::new(SK_SENTINEL.to_vec())))
                    .build()
                    .expect("probe delete_request has both keys set");
                WriteRequest::builder().delete_request(delete).build()
            })
            .collect();
        let effective = match self
            .round_robin_client()
            .batch_write_item()
            .request_items(table.clone(), probe)
            .send()
            .await
        {
            Ok(_) => {
                info!(
                    candidate,
                    table, "alternator accepts larger BatchWriteItem; using probed limit"
                );
                candidate
            }
            // Only a service-level rejection means "the backend caps batches
            // below the candidate". Timeouts and dispatch failures say nothing
            // about the limit, so they surface as startup errors instead.
            Err(e) if e.as_service_error().is_some() => {
                warn!(
                    candidate,
                    fallback = BATCH_WRITE_LIMIT,
                    error = ?e,
                    "BatchWriteItem limit probe rejected; falling back to 25"
                );
                BATCH_WRITE_LIMIT
            }
            Err(e) => return Err(backend_err("batch_write_item limit probe", e)),
        };
        self.inner
            .batch_write_max_items
            .store(effective, Ordering::Relaxed);
        Ok(effective)
    }
}

impl MetaStore for DynamoMetaStore {
    async fn get(&self, table: TableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_row_data(
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
        self.get_row_data(
            self.table_name_for_scan(table),
            scan_pk(table, partition),
            clustering.to_vec(),
        )
        .await
    }

    async fn put(&self, table: TableId, key: &[u8], value: Bytes) -> Result<()> {
        self.put_row_data(
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
        self.put_row_data(
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
                let (physical_table, partition_key, sort_key, row_data) = match op {
                    MetaWriteOp::Put {
                        table,
                        row_key,
                        row_data,
                    } => (
                        self.table_name_for_kv(table),
                        kv_pk(table, &row_key),
                        SK_SENTINEL.to_vec(),
                        row_data,
                    ),
                    MetaWriteOp::ScanPut {
                        table,
                        partition,
                        clustering_key,
                        row_data,
                    } => (
                        self.table_name_for_scan(table),
                        scan_pk(table, &partition),
                        clustering_key,
                        row_data,
                    ),
                };
                let estimated_wire_bytes = estimated_batch_write_item_bytes(
                    partition_key.len(),
                    sort_key.len(),
                    row_data.len(),
                );
                let put = PutRequest::builder()
                    .item(ATTR_PK, AttributeValue::B(Blob::new(partition_key)))
                    .item(ATTR_SK, AttributeValue::B(Blob::new(sort_key)))
                    // `Blob::new(row_data)` reclaims the Vec when uniquely owned.
                    .item(ATTR_VAL, AttributeValue::B(Blob::new(row_data)))
                    .build()
                    .map_err(|e| QueryError::Backend(format!("dynamo build put_request: {e}")))?;
                Ok((
                    physical_table,
                    WriteRequest::builder().put_request(put).build(),
                    estimated_wire_bytes,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let request_count = requests.len();
        let estimated_wire_bytes = requests.iter().map(|(_, _, bytes)| *bytes).sum::<usize>();
        let batch_write_max_items = self.inner.batch_write_max_items.load(Ordering::Relaxed);
        let chunks = split_batch_write_chunks(requests, batch_write_max_items);
        let chunk_count = chunks.len();
        // Any table whose chunk count exceeds the count-only minimum was split
        // by the payload soft limit (derived from `chunks`, O(#chunks)).
        let mut by_table: HashMap<&str, (usize, usize)> = HashMap::new();
        for (table, chunk) in &chunks {
            let (items, chunk_count) = by_table.entry(table.as_str()).or_default();
            *items += chunk.len();
            *chunk_count += 1;
        }
        let payload_split = by_table
            .values()
            .any(|(items, chunk_count)| *chunk_count > items.div_ceil(batch_write_max_items));
        let chunk_tables = chunks.iter().map(|(table, _)| table.as_str());
        let chunks_by_table = summarize_names(chunk_tables, 8);
        debug!(
            input_write_count,
            request_count,
            chunks = chunk_count,
            estimated_wire_bytes,
            concurrency = self.inner.batch_max_concurrency,
            table_concurrency = self.inner.batch_table_max_concurrency,
            batch_write_max_items,
            chunks_by_table = %chunks_by_table,
            build_ms = build_started.elapsed().as_millis() as u64,
            "dynamo apply_writes built BatchWriteItem requests"
        );
        if payload_split {
            warn!(
                request_count,
                chunks = chunk_count,
                estimated_wire_bytes,
                payload_soft_limit = BATCH_WRITE_PAYLOAD_SOFT_LIMIT,
                "dynamo batch_write_item split by estimated payload size"
            );
        }
        run_batch_write_chunks(
            "apply_writes",
            &self.inner.ring,
            chunks,
            self.inner.batch_max_concurrency,
            self.inner.batch_table_max_concurrency,
        )
        .await
    }

    async fn scan_keys(&self, table: ScannableTableId, partition: &[u8]) -> Result<Vec<Vec<u8>>> {
        let pk = scan_pk(table, partition);
        let physical_table = self.table_name_for_scan(table);
        // A `Query` response is capped at 1 MB, but the trait contract is the
        // whole partition in one call, so follow `LastEvaluatedKey` until the
        // server stops handing one back.
        let mut pagination_key: Option<HashMap<String, AttributeValue>> = None;
        let mut clustering_keys: Vec<Vec<u8>> = Vec::new();
        loop {
            let req = self
                .round_robin_client()
                .query()
                .table_name(physical_table.clone())
                .key_condition_expression("pk = :p")
                .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.clone())))
                // Callers only consume the clustering; keeps `val` off the wire.
                .projection_expression(ATTR_SK)
                .consistent_read(true)
                .scan_index_forward(true)
                .set_exclusive_start_key(pagination_key.take());

            let resp = req.send().await.map_err(|e| backend_err("query", e))?;
            for mut item in resp.items.unwrap_or_default() {
                clustering_keys.push(take_binary(&mut item, ATTR_SK)?);
            }

            // Follow the server's cursor (an empty page can still carry a
            // LastEvaluatedKey); absence means the range is exhausted.
            pagination_key = resp.last_evaluated_key;
            if pagination_key.is_none() {
                break;
            }
        }

        Ok(clustering_keys)
    }
}

fn kv_pk(table: TableId, key: &[u8]) -> Vec<u8> {
    encode_pk(KIND_KV, table.as_str(), key)
}

fn scan_pk(table: ScannableTableId, partition: &[u8]) -> Vec<u8> {
    encode_pk(KIND_SCAN, table.as_str(), partition)
}

fn dynamo_safe_name(logical_table: &str) -> String {
    logical_table.replace('_', "-")
}

#[cfg(test)]
mod tests {
    use super::*;

    const KV: TableId = TableId::new("blocks");

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
        let a = encode_pk(KIND_KV, "t", b"x");
        let b = encode_pk(KIND_SCAN, "t", b"x");
        assert_ne!(a, b);
    }

    #[test]
    fn pk_table_boundary_is_unambiguous() {
        // ("ab", "c") vs ("a", "bc"): the table length prefix must disambiguate.
        let lhs = encode_pk(KIND_KV, "ab", b"c");
        let rhs = encode_pk(KIND_KV, "a", b"bc");
        assert_ne!(lhs, rhs);
    }
}
