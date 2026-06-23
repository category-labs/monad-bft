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

//! DynamoDB-API-compatible [`BlobStore`] backend (AWS DynamoDB, DynamoDB Local,
//! ScyllaDB Alternator). DynamoDB caps items at 400 KiB and has no server-side
//! byte-range read, so each blob is split into `chunk_size`-byte items sharing
//! a partition; `read_range` is a single-partition `Query` over the covering
//! chunk-index range. Reuses the metastore's client setup, key encoding, and
//! BatchWriteItem machinery (`store::dynamo_common`).
//!
//! Single-table layout (a wire contract once data exists):
//!
//! ```text
//! pk  = u16-be(len("blob")) ∥ "blob" ∥ u16-be(len(table)) ∥ table ∥ blob-key
//! sk  = u32-be(chunk_index)           -- fixed width so byte order == chunk order
//! val = the chunk's bytes
//! len = total blob length (Number)    -- on chunk 0 only
//! ```
//!
//! One marker item per physical table (`pk` kind `"blob_chunk_size"`, `sk` =
//! u32-be(0), `val`: Number) records the chunk size the data is cut with;
//! [`DynamoBlobStore::create_table`] writes it and
//! [`DynamoBlobStore::validate_table`] rejects a store configured with a
//! different size (absent marker = pre-marker table, tolerated).
//!
//! `apply_writes` is not atomic: a multi-chunk blob is independent PutItems and
//! a partial write leaves orphan chunks, but the `MetaStore` head publication
//! gates visibility, so readers never observe a torn blob.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use aws_sdk_dynamodb::{
    error::{ProvideErrorMetadata, SdkError},
    operation::{put_item::PutItemError, query::builders::QueryFluentBuilder},
    primitives::Blob,
    types::{AttributeValue, DeleteRequest, PutRequest, WriteRequest},
    Client,
};
use bytes::Bytes;
use tracing::debug;

pub use crate::meta::DynamoCredentials;
use crate::{
    aws::load_sdk_config,
    blob::{BlobStore, BlobTableId, BlobWriteOp},
    dynamo_common::{
        backend_err, create_pk_sk_table, encode_pk, estimated_batch_write_item_bytes,
        run_batch_write_chunks, split_batch_write_chunks, take_binary, validate_pk_sk_table,
        ClientRing, SharedDynamoConnection, ATTR_PK, ATTR_SK, ATTR_VAL, BATCH_WRITE_LIMIT,
    },
    error::{MonadChainDataError, Result},
};

/// Key-kind discriminator folded into `pk`, disjoint from the metastore kinds.
const KIND_BLOB: &[u8] = b"blob";

/// Key-kind discriminator of the per-table chunk-size marker item, disjoint
/// from `KIND_BLOB` (and the metastore kinds) via the length-prefixed pk
/// encoding.
const KIND_CHUNK_SIZE: &[u8] = b"blob_chunk_size";

/// Number attribute on chunk 0 carrying the total blob length.
const ATTR_LEN: &str = "len";

/// Default chunk size: 64 KiB. Most blobs fit one chunk; a ~16 MiB block blob
/// splits into <=256 items, and `read_range` over-reads <=64 KiB on each end.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Hard ceiling: DynamoDB rejects items larger than 400 KiB. Stay under it with
/// margin for `pk`/`sk`/`len`/framing overhead.
pub const MAX_CHUNK_SIZE: usize = 350 * 1024;

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
    /// that already holds data (a mismatched reader silently returns wrong
    /// bytes). [`DynamoBlobStore::create_table`] persists the size as a marker
    /// item that [`DynamoBlobStore::validate_table`] checks at startup.
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
    ring: ClientRing,
    table_name: String,
    batch_max_concurrency: usize,
    chunk_size: usize,
    /// Effective max write requests per `BatchWriteItem`. Shared with the
    /// metastore's probed limit when the deployment shares clients; a
    /// standalone store stays at the DynamoDB-safe [`BATCH_WRITE_LIMIT`].
    batch_write_max_items: Arc<AtomicUsize>,
}

/// DynamoDB-API-compatible chunked [`BlobStore`]. Cheaply cloneable -- all state
/// lives behind an `Arc`, and the SDK `Client` is itself `Arc`-backed.
#[derive(Clone)]
pub struct DynamoBlobStore {
    inner: Arc<Inner>,
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
    /// Builds the SDK client from `config`; async because resolving the AWS
    /// credential/region chain performs I/O. Assumes the table already exists
    /// (see [`DynamoBlobStore::create_table`] for test/dev provisioning).
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

        let sdk_config =
            load_sdk_config(region, profile, credentials, "DynamoBlobStoreConfig").await;
        let client = match &endpoint_url {
            Some(endpoint) => {
                let conf = aws_sdk_dynamodb::config::Builder::from(&sdk_config)
                    .endpoint_url(endpoint)
                    .build();
                Client::from_conf(conf)
            }
            None => Client::new(&sdk_config),
        };

        Ok(Self::with_connection(
            SharedDynamoConnection {
                ring: ClientRing::new(vec![client]),
                batch_write_max_items: Arc::new(AtomicUsize::new(BATCH_WRITE_LIMIT)),
                endpoint_urls: Arc::new(endpoint_url.into_iter().collect()),
            },
            table_name,
            batch_max_concurrency,
            chunk_size,
        ))
    }

    /// Builds a store from an existing connection (client ring + effective
    /// batch-write limit), so a deployment running both the dynamo meta and
    /// blob backends shares one connection pool and one probed limit.
    pub fn with_connection(
        connection: SharedDynamoConnection,
        table_name: impl Into<String>,
        batch_max_concurrency: usize,
        chunk_size: usize,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                ring: connection.ring,
                table_name: table_name.into(),
                batch_max_concurrency: batch_max_concurrency.max(1),
                chunk_size: chunk_size.clamp(1, MAX_CHUNK_SIZE),
                batch_write_max_items: connection.batch_write_max_items,
            }),
        }
    }

    fn table(&self) -> &str {
        &self.inner.table_name
    }

    /// Round-robin one of the per-endpoint clients.
    fn rr_client(&self) -> &Client {
        self.inner.ring.get()
    }

    /// Strongly-consistent read of chunk 0's `len` attribute (the blob's total
    /// length), projecting only `len`. `Ok(None)` means the blob does not exist.
    async fn blob_len(&self, pk: &[u8]) -> Result<Option<usize>> {
        let resp = self
            .rr_client()
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
        let Some(item) = resp.item else {
            return Ok(None);
        };
        match item.get(ATTR_LEN) {
            Some(AttributeValue::N(n)) => n.parse::<usize>().map(Some).map_err(|_| {
                MonadChainDataError::Backend(format!("dynamo blob len not a usize: {n}"))
            }),
            _ => Err(MonadChainDataError::Backend(
                "dynamo blob chunk 0 missing numeric `len` attribute".to_string(),
            )),
        }
    }

    /// Runs one strongly-consistent, ascending-sk `Query` shaped by `configure`
    /// (key condition / projection), following `last_evaluated_key` pagination
    /// to exhaustion. Takes a fresh round-robin client per page.
    async fn query_all_items(
        &self,
        configure: impl Fn(QueryFluentBuilder) -> QueryFluentBuilder,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let mut items = Vec::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;
        loop {
            let base = self
                .rr_client()
                .query()
                .table_name(self.table())
                .consistent_read(true)
                .scan_index_forward(true)
                .set_exclusive_start_key(start_key.take());
            let resp = configure(base)
                .send()
                .await
                .map_err(|e| backend_err("query", e))?;
            items.extend(resp.items.unwrap_or_default());
            match resp.last_evaluated_key {
                Some(lek) => start_key = Some(lek),
                None => return Ok(items),
            }
        }
    }

    /// Queries chunks `[first, last]` (inclusive) of one partition and
    /// concatenates them; the returned buffer begins at byte `first * chunk_size`.
    async fn read_chunk_range(&self, pk: &[u8], first: u32, last: u32) -> Result<Vec<u8>> {
        let items = self
            .query_all_items(|q| {
                q.key_condition_expression("pk = :p AND sk BETWEEN :a AND :b")
                    .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.to_vec())))
                    .expression_attribute_values(
                        ":a",
                        AttributeValue::B(Blob::new(chunk_sk(first))),
                    )
                    .expression_attribute_values(":b", AttributeValue::B(Blob::new(chunk_sk(last))))
            })
            .await?;
        concat_chunk_values(items)
    }

    /// Builds one put request per `chunk_size` piece (`len` on chunk 0). An empty
    /// value still writes an empty chunk 0 so the blob reads back as present.
    fn chunk_puts(&self, pk: Vec<u8>, value: &[u8]) -> Result<Vec<(String, WriteRequest, usize)>> {
        let total = value.len();
        if value.is_empty() {
            return Ok(vec![self.build_chunk_put(&pk, 0, &[], Some(total))?]);
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
                self.build_chunk_put(&pk, idx, chunk, len)
            })
            .collect()
    }

    fn build_chunk_put(
        &self,
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
            self.table().to_string(),
            WriteRequest::builder().put_request(put).build(),
            estimated,
        ))
    }

    /// Splits put requests into BatchWriteItem-legal chunks (effective item /
    /// payload limits) and runs them via the shared bounded-retry executor.
    async fn run_batch_writes(&self, requests: Vec<(String, WriteRequest, usize)>) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let build_started = std::time::Instant::now();
        let request_count = requests.len();
        let estimated_wire_bytes = requests.iter().map(|(_, _, bytes)| *bytes).sum::<usize>();
        let batch_write_max_items = self.inner.batch_write_max_items.load(Ordering::Relaxed);
        let chunks = split_batch_write_chunks(requests, batch_write_max_items);
        let concurrency = self.inner.batch_max_concurrency;
        debug!(
            request_count,
            chunks = chunks.len(),
            estimated_wire_bytes,
            concurrency,
            batch_write_max_items,
            build_ms = build_started.elapsed().as_millis() as u64,
            "dynamo blob built BatchWriteItem requests"
        );
        // Single physical table: the per-table cap equals the global one.
        run_batch_write_chunks("blob", &self.inner.ring, chunks, concurrency, concurrency).await
    }

    /// Idempotent table provisioning for tests/dev ONLY -- never called by
    /// [`DynamoBlobStore::new`]. Treats `ResourceInUseException` as success,
    /// then polls until `ACTIVE` and records the chunk size the table's data
    /// will be cut with. Never overwrites an existing marker: re-provisioning
    /// with a different chunk size must surface the mismatch, not mask it
    /// (the put is conditional on absence, so a racing provisioner cannot
    /// mask one either). Adopting a pre-marker table that already holds data
    /// stamps the configured size as authoritative — it cannot be verified
    /// against the data itself.
    pub async fn create_table(&self) -> Result<()> {
        create_pk_sk_table(&self.inner.ring, self.table()).await?;
        match self.stored_chunk_size().await? {
            Some(stored) => self.check_chunk_size(stored),
            None => self.write_chunk_size_marker().await,
        }
    }

    /// Startup connectivity/schema check: the configured table exists, is
    /// `ACTIVE`, has the expected binary `pk` HASH + binary `sk` RANGE keys,
    /// and (when the marker exists) was provisioned with this store's chunk
    /// size. An absent marker (pre-marker table) is tolerated, and read-only
    /// stores never write one.
    pub async fn validate_table(&self) -> Result<()> {
        validate_pk_sk_table(&self.inner.ring, self.table()).await?;
        match self.stored_chunk_size().await? {
            Some(stored) => self.check_chunk_size(stored),
            None => Ok(()),
        }
    }

    /// Reads the table's chunk-size marker; `Ok(None)` for tables provisioned
    /// before the marker existed.
    async fn stored_chunk_size(&self) -> Result<Option<usize>> {
        let resp = self
            .rr_client()
            .get_item()
            .table_name(self.table())
            .key(
                ATTR_PK,
                AttributeValue::B(Blob::new(chunk_size_marker_pk())),
            )
            .key(ATTR_SK, AttributeValue::B(Blob::new(chunk_sk(0))))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| backend_err("get_item", e))?;
        let Some(item) = resp.item else {
            return Ok(None);
        };
        match item.get(ATTR_VAL) {
            Some(AttributeValue::N(n)) => n.parse::<usize>().map(Some).map_err(|_| {
                MonadChainDataError::Backend(format!(
                    "dynamo blob chunk-size marker not a usize: {n}"
                ))
            }),
            _ => Err(MonadChainDataError::Backend(
                "dynamo blob chunk-size marker missing numeric `val` attribute".to_string(),
            )),
        }
    }

    /// Stamps the marker, conditional on absence so racing provisioners
    /// cannot overwrite each other: the loser re-reads and validates against
    /// the value that won.
    async fn write_chunk_size_marker(&self) -> Result<()> {
        let result = self
            .rr_client()
            .put_item()
            .table_name(self.table())
            .item(
                ATTR_PK,
                AttributeValue::B(Blob::new(chunk_size_marker_pk())),
            )
            .item(ATTR_SK, AttributeValue::B(Blob::new(chunk_sk(0))))
            .item(
                ATTR_VAL,
                AttributeValue::N(self.inner.chunk_size.to_string()),
            )
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // Lost a provisioning race (or the SDK retried a put whose first
            // response was lost): validate against whatever got stored.
            Err(e) if is_conditional_check_failed(&e) => match self.stored_chunk_size().await? {
                Some(stored) => self.check_chunk_size(stored),
                None => Err(MonadChainDataError::Backend(
                    "dynamo blob chunk-size marker absent after conditional-put conflict"
                        .to_string(),
                )),
            },
            Err(e) => Err(backend_err("put_item", e)),
        }
    }

    fn check_chunk_size(&self, stored: usize) -> Result<()> {
        if stored == self.inner.chunk_size {
            return Ok(());
        }
        Err(MonadChainDataError::Backend(format!(
            "dynamo blob table {} chunk_size mismatch: configured {}, data written with {stored} \
             (the byte->chunk-index mapping is a wire contract; a mismatched size mis-slices \
             range reads)",
            self.table(),
            self.inner.chunk_size
        )))
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
        let items = self
            .query_all_items(|q| {
                q.key_condition_expression("pk = :p")
                    .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.clone())))
            })
            .await?;
        if items.is_empty() {
            return Ok(None);
        }
        Ok(Some(Bytes::from(concat_chunk_values(items)?)))
    }

    /// Deletes every chunk item of the blob's partition. The chunk set is
    /// enumerated by a keys-only `Query` rather than derived from the `len`
    /// attribute, so orphan tail chunks left by a shorter overwrite are removed
    /// too. A missing blob has no items, so this is an idempotent no-op.
    async fn delete_blob(&self, table: BlobTableId, key: &[u8]) -> Result<()> {
        let pk = blob_pk(table, key);
        let items = self
            .query_all_items(|q| {
                q.key_condition_expression("pk = :p")
                    .expression_attribute_values(":p", AttributeValue::B(Blob::new(pk.clone())))
                    .projection_expression("#s")
                    .expression_attribute_names("#s", ATTR_SK)
            })
            .await?;
        let table_name = self.table().to_string();
        let requests = items
            .into_iter()
            .map(|mut item| {
                let sk = take_binary(&mut item, ATTR_SK)?;
                let estimated = estimated_batch_write_item_bytes(pk.len(), sk.len(), 0);
                let delete = DeleteRequest::builder()
                    .key(ATTR_PK, AttributeValue::B(Blob::new(pk.clone())))
                    .key(ATTR_SK, AttributeValue::B(Blob::new(sk)))
                    .build()
                    .map_err(|e| {
                        MonadChainDataError::Backend(format!("dynamo build delete_request: {e}"))
                    })?;
                Ok((
                    table_name.clone(),
                    WriteRequest::builder().delete_request(delete).build(),
                    estimated,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        self.run_batch_writes(requests).await
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
            // `len` claims more bytes than the chunks hold: a torn write that
            // head publication should have kept invisible.
            return Err(MonadChainDataError::Decode(
                "blob range exceeds stored chunks",
            ));
        }
        Ok(Some(Bytes::from(buf).slice(offset..offset + want)))
    }
}

/// Concatenates the `ATTR_VAL` bytes of `items` (already in ascending-sk chunk
/// order) into one buffer; the read path's shared chunk-reassembly step.
fn concat_chunk_values(items: Vec<HashMap<String, AttributeValue>>) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    for mut item in items {
        buf.extend_from_slice(&take_binary(&mut item, ATTR_VAL)?);
    }
    Ok(buf)
}

/// `pk` for a blob: the metastore's length-prefixed `(kind, table, tail)`
/// encoding with the `"blob"` kind and the blob key as the tail.
fn blob_pk(table: BlobTableId, key: &[u8]) -> Vec<u8> {
    encode_pk(KIND_BLOB, table.as_str(), key)
}

/// `pk` of the physical table's single chunk-size marker item (the logical
/// table and tail are empty: the chunk size is a per-physical-table property).
fn chunk_size_marker_pk() -> Vec<u8> {
    encode_pk(KIND_CHUNK_SIZE, "", &[])
}

/// Sort key for a chunk: the index as fixed-width big-endian so DynamoDB's
/// unsigned-byte sort on `sk` equals chunk order.
fn chunk_sk(idx: u32) -> [u8; 4] {
    idx.to_be_bytes()
}

/// Matches both the modeled variant and a bare error code (a compatible
/// backend may not model the exception).
fn is_conditional_check_failed<R>(e: &SdkError<PutItemError, R>) -> bool {
    matches!(
        e,
        SdkError::ServiceError(se)
            if matches!(se.err(), PutItemError::ConditionalCheckFailedException(_))
    ) || e.code() == Some("ConditionalCheckFailedException")
}

/// For a byte range `[start, end)` (`end > start`) returns the inclusive chunk
/// span `(first, last)` covering it and `start`'s offset within the first
/// chunk, so the requested bytes are `buf[offset..offset+(end-start)]`.
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
        let mut sks: Vec<[u8; 4]> = vec![chunk_sk(0), chunk_sk(255), chunk_sk(256), chunk_sk(1)];
        sks.sort();
        assert_eq!(
            sks,
            vec![chunk_sk(0), chunk_sk(1), chunk_sk(255), chunk_sk(256)]
        );
        assert_eq!(chunk_sk(256), [0x00, 0x00, 0x01, 0x00]);
    }

    #[test]
    fn chunk_span_single_chunk() {
        assert_eq!(chunk_span(2, 6, 4), (0, 1, 2));
        assert_eq!(chunk_span(0, 4, 4), (0, 0, 0));
        assert_eq!(chunk_span(4, 8, 4), (1, 1, 0));
    }

    #[test]
    fn chunk_span_crosses_chunks() {
        assert_eq!(chunk_span(7, 9, 4), (1, 2, 3));
        assert_eq!(chunk_span(10, 12, 4), (2, 2, 2));
    }

    #[test]
    fn chunk_span_reassembles_to_request_window() {
        let chunk_size = 64 * 1024;
        for &(start, end) in &[(0usize, 1usize), (100, 200_000), (64 * 1024, 64 * 1024 + 1)] {
            let (first, _last, offset) = chunk_span(start, end, chunk_size);
            assert_eq!(first as usize * chunk_size + offset, start);
        }
    }
}
