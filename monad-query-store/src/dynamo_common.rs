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

//! Wire-format and `BatchWriteItem` machinery shared by the dynamo meta and
//! blob backends: pk encoding, chunk splitting, the chunk-retry executor with
//! progress tracking, and pk/sk table provisioning/validation.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use aws_sdk_dynamodb::{
    error::{ProvideErrorMetadata, SdkError},
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, TableStatus, WriteRequest,
    },
    Client,
};
use futures::stream::{StreamExt, TryStreamExt};
use monad_query_errors::{QueryError, Result};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

/// Attribute names. Short to keep item overhead low; these are a wire contract.
pub(crate) const ATTR_PK: &str = "pk";
pub(crate) const ATTR_SK: &str = "sk";
pub(crate) const ATTR_VAL: &str = "val";

/// DynamoDB caps `BatchWriteItem` at 25 write requests per call.
pub(crate) const BATCH_WRITE_LIMIT: usize = 25;

/// Alternator rejects oversized HTTP request bodies. DynamoDB's documented
/// BatchWriteItem payload cap is 16 MiB; keep a margin for JSON/base64 framing.
pub(crate) const BATCH_WRITE_PAYLOAD_SOFT_LIMIT: usize = 12 * 1024 * 1024;
const BATCH_WRITE_ITEM_OVERHEAD: usize = 512;
// 8 retries at 50ms exponential backoff = 12.75s worst-case cumulative delay.
const BATCH_WRITE_CHUNK_MAX_RETRIES: u32 = 8;
const BATCH_WRITE_CHUNK_BASE_BACKOFF_MS: u64 = 50;
const BATCH_WRITE_CHUNK_MAX_BACKOFF_MS: u64 = 20_000;

/// One SDK client per configured endpoint, round-robined per request to
/// spread coordinator load (Alternator nodes are peers). Cheaply cloneable;
/// clones share the rotation cursor.
#[derive(Clone)]
pub(crate) struct ClientRing {
    clients: Arc<[Client]>,
    next: Arc<AtomicUsize>,
}

impl ClientRing {
    pub(crate) fn new(clients: Vec<Client>) -> Self {
        assert!(!clients.is_empty(), "client ring requires >= 1 client");
        Self {
            clients: clients.into(),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Round-robin one of the per-endpoint clients.
    pub(crate) fn next_client(&self) -> &Client {
        if self.clients.len() == 1 {
            return &self.clients[0];
        }
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.clients.len();
        &self.clients[i]
    }
}

/// The meta store's connection state handed to a co-deployed dynamo blob
/// store, so both backends share one client ring (and its endpoint spread)
/// and the probed `BatchWriteItem` item limit.
#[derive(Clone)]
pub struct SharedDynamoConnection {
    pub(crate) ring: ClientRing,
    /// Live handle onto the meta store's effective max items per
    /// `BatchWriteItem` (raised by its startup probe).
    pub(crate) batch_write_max_items: Arc<AtomicUsize>,
    /// The meta store's configured endpoint URLs, so a co-deployed blob store
    /// can suppress its "ignored settings" warn when it points at the same
    /// endpoint(s) (the common DynamoDynamo case).
    pub endpoint_urls: Arc<Vec<String>>,
}

/// `pk = u16-be(len(kind)) ∥ kind ∥ u16-be(len(table)) ∥ table ∥ tail`. Length
/// prefixes keep the kind/table boundaries unambiguous; `tail` is the kv key
/// or the scan partition appended raw.
pub(crate) fn encode_pk(kind: &[u8], table: &str, tail: &[u8]) -> Vec<u8> {
    let table = table.as_bytes();
    let mut out = Vec::with_capacity(4 + kind.len() + table.len() + tail.len());
    for segment in [kind, table] {
        let segment_length = u16::try_from(segment.len()).expect("pk segment length fits in u16");
        out.extend_from_slice(&segment_length.to_be_bytes());
        out.extend_from_slice(segment);
    }
    out.extend_from_slice(tail);
    out
}

/// Removes and returns a Binary attribute, erroring if absent or wrong type.
pub(crate) fn take_binary(
    item: &mut HashMap<String, AttributeValue>,
    attr: &str,
) -> Result<Vec<u8>> {
    match item.remove(attr) {
        Some(AttributeValue::B(blob)) => Ok(blob.into_inner()),
        _ => Err(QueryError::Backend(format!(
            "dynamo item missing binary attribute `{attr}`"
        ))),
    }
}

pub(crate) fn backend_err<E, R>(op: &str, e: SdkError<E, R>) -> QueryError
where
    E: ProvideErrorMetadata + std::error::Error + std::fmt::Debug + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    // Prefer the service-reported code/message; SdkError's own Display is terse.
    let detail = match e.code() {
        Some(code) => format!("{code}: {}", e.message().unwrap_or("")),
        None => format!("{e}; debug={e:?}"),
    };
    QueryError::Backend(format!("dynamo {op}: {detail}"))
}

pub(crate) fn estimated_batch_write_item_bytes(
    pk_len: usize,
    sk_len: usize,
    value_len: usize,
) -> usize {
    // Each binary field is base64-encoded in the JSON payload
    let base64_len = |raw_len: usize| raw_len.div_ceil(3).saturating_mul(4);
    BATCH_WRITE_ITEM_OVERHEAD + base64_len(pk_len) + base64_len(sk_len) + base64_len(value_len)
}

pub(crate) fn split_batch_write_chunks<T>(
    items: Vec<(String, T, usize)>,
    max_items: usize,
) -> Vec<(String, Vec<T>)> {
    let max_items = max_items.max(1);
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
            let exceeds_count = current.len() >= max_items;
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

/// Counts duplicates in `names` and formats the top `limit` as
/// `name:count,...`, most frequent first (ties by name). Log-friendly summary
/// of e.g. the per-table chunk distribution and the in-flight chunk tables.
pub(crate) fn summarize_names<'a>(names: impl Iterator<Item = &'a str>, limit: usize) -> String {
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

#[derive(Default)]
struct BatchWriteProgress {
    chunks_started: AtomicUsize,
    chunks_completed: AtomicUsize,
    chunks_failed: AtomicUsize,
    total_retries: AtomicUsize,
    total_unprocessed_items: AtomicUsize,
    /// Maps chunk index to the table being processed for this chunk
    in_flight_chunks: Mutex<HashMap<usize, String>>,
}

impl BatchWriteProgress {
    fn mark_started(&self, chunk_idx: usize, table: &str, attempt: u32) {
        if attempt == 0 {
            self.chunks_started.fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_retries.fetch_add(1, Ordering::Relaxed);
        }
        self.in_flight_chunks
            .lock()
            .expect("batch progress poisoned")
            .insert(chunk_idx, table.to_string());
    }

    fn mark_completed(&self, chunk_idx: usize) {
        self.chunks_completed.fetch_add(1, Ordering::Relaxed);
        self.in_flight_chunks
            .lock()
            .expect("batch progress poisoned")
            .remove(&chunk_idx);
    }

    fn mark_attempt_finished(&self, chunk_idx: usize) {
        self.in_flight_chunks
            .lock()
            .expect("batch progress poisoned")
            .remove(&chunk_idx);
    }

    fn mark_failed(&self, chunk_idx: usize) {
        self.chunks_failed.fetch_add(1, Ordering::Relaxed);
        self.in_flight_chunks
            .lock()
            .expect("batch progress poisoned")
            .remove(&chunk_idx);
    }

    fn mark_unprocessed(&self, count: usize) {
        self.total_unprocessed_items
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Calculates exponential backoff with deterministic jitter for retry delays.
    fn retry_backoff(chunk_idx: usize, table: &str, attempt: u32) -> std::time::Duration {
        let exponent = attempt.saturating_sub(1).min(30);
        let cap_ms = BATCH_WRITE_CHUNK_BASE_BACKOFF_MS
            .saturating_mul(1_u64 << exponent)
            .min(BATCH_WRITE_CHUNK_MAX_BACKOFF_MS);

        // Deterministic hash-based jitter using chunk index and table name
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in table.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= chunk_idx as u64;
        hash = hash.wrapping_mul(0x9e3779b97f4a7c15);
        hash ^= u64::from(attempt);
        hash ^= hash >> 12;
        hash ^= hash << 25;
        hash ^= hash >> 27;
        hash = hash.wrapping_mul(0x2545f4914f6cdd1d);

        let jitter_ms = if cap_ms == 0 { 0 } else { hash % (cap_ms + 1) };
        std::time::Duration::from_millis(jitter_ms.max(1))
    }

    fn snapshot(&self) -> BatchWriteProgressSnapshot {
        let in_flight = self
            .in_flight_chunks
            .lock()
            .expect("batch progress poisoned");
        BatchWriteProgressSnapshot {
            chunks_started: self.chunks_started.load(Ordering::Relaxed),
            chunks_completed: self.chunks_completed.load(Ordering::Relaxed),
            chunks_failed: self.chunks_failed.load(Ordering::Relaxed),
            total_retries: self.total_retries.load(Ordering::Relaxed),
            total_unprocessed_items: self.total_unprocessed_items.load(Ordering::Relaxed),
            in_flight_chunk_count: in_flight.len(),
            in_flight_table_summary: summarize_names(in_flight.values().map(String::as_str), 8),
        }
    }
}

struct BatchWriteProgressSnapshot {
    chunks_started: usize,
    chunks_completed: usize,
    chunks_failed: usize,
    total_retries: usize,
    total_unprocessed_items: usize,
    in_flight_chunk_count: usize,
    /// Top tables by in-flight chunk count, formatted `name:count,...`.
    in_flight_table_summary: String,
}

/// Runs pre-split `BatchWriteItem` chunks concurrently (bounded globally by
/// `concurrency` and per physical table by `table_concurrency`), retrying each
/// chunk via [`write_batch_chunk`] and logging progress on failure.
pub(crate) async fn run_batch_write_chunks(
    backend: &'static str,
    ring: &ClientRing,
    chunks: Vec<(String, Vec<WriteRequest>)>,
    concurrency: usize,
    table_concurrency: usize,
) -> Result<()> {
    let request_count = chunks.iter().map(|(_, c)| c.len()).sum::<usize>();
    let chunk_count = chunks.len();
    let progress = Arc::new(BatchWriteProgress::default());
    let mut table_permits = HashMap::new();
    for (table, _) in &chunks {
        table_permits
            .entry(table.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(table_concurrency)));
    }
    let table_permits = Arc::new(table_permits);
    let send_started = std::time::Instant::now();
    let result = futures::stream::iter(chunks.into_iter().enumerate().map(
        |(chunk_idx, (table, requests))| {
            let progress = progress.clone();
            let table_permits = table_permits.clone();
            async move {
                let _permit = table_permits
                    .get(&table)
                    .expect("table semaphore must exist for every write chunk")
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("table semaphore should not be closed");
                write_batch_chunk(ring, chunk_idx, table, requests, &progress).await
            }
        },
    ))
    .buffer_unordered(concurrency)
    .try_collect::<()>()
    .await;
    if let Err(error) = &result {
        let snapshot = progress.snapshot();
        warn!(
            %error,
            request_count,
            chunks = chunk_count,
            chunks_started = snapshot.chunks_started,
            chunks_completed = snapshot.chunks_completed,
            chunks_failed = snapshot.chunks_failed,
            in_flight_chunks = snapshot.in_flight_chunk_count,
            total_retries = snapshot.total_retries,
            total_unprocessed_items = snapshot.total_unprocessed_items,
            in_flight_tables = %snapshot.in_flight_table_summary,
            "dynamo {backend} BatchWriteItem stream failed"
        );
    }
    result?;
    let snapshot = progress.snapshot();
    debug!(
        request_count,
        chunks = chunk_count,
        chunks_started = snapshot.chunks_started,
        chunks_completed = snapshot.chunks_completed,
        chunks_failed = snapshot.chunks_failed,
        total_retries = snapshot.total_retries,
        total_unprocessed_items = snapshot.total_unprocessed_items,
        send_ms = send_started.elapsed().as_millis() as u64,
        "dynamo {backend} completed BatchWriteItem requests"
    );
    Ok(())
}

/// One `BatchWriteItem` call (single physical table), retrying send errors and
/// `UnprocessedItems` leftovers with bounded jittered backoff until they drain
/// or [`BATCH_WRITE_CHUNK_MAX_RETRIES`] is exhausted. A slow-send watchdog
/// warns while a send stays in flight; the SDK-level operation timeouts
/// (`store::aws`) bound the send itself, so a wedged connection resolves to an
/// `Err` here and re-enters the retry loop (each retry takes a fresh
/// round-robin client, rotating endpoints).
async fn write_batch_chunk(
    ring: &ClientRing,
    chunk_idx: usize,
    table: String,
    requests: Vec<WriteRequest>,
    progress: &BatchWriteProgress,
) -> Result<()> {
    let initial_request_count = requests.len();
    let mut pending = requests;

    // BatchWriteItem returns leftovers rather than erroring on partial
    // capacity; resubmit only those, with bounded exponential backoff and
    // full jitter.
    let mut attempt = 0u32;
    loop {
        let pending_count = pending.len();
        let log_sample = chunk_idx < 8 || chunk_idx.is_multiple_of(100) || attempt > 0;
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
        // The SDK consumes the requests, and a retry may need them again.
        let send = ring
            .next_client()
            .batch_write_item()
            .set_request_items(Some(HashMap::from([(table.clone(), pending.clone())])))
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
                let backoff = BatchWriteProgress::retry_backoff(chunk_idx, &table, attempt);
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
            return Err(QueryError::Backend(format!(
                "dynamo batch_write_item: {} items still unprocessed after {attempt} retries",
                leftovers.len()
            )));
        }
        let backoff = BatchWriteProgress::retry_backoff(chunk_idx, &table, attempt);
        tokio::time::sleep(backoff).await;
        pending = leftovers;
    }
}

/// Idempotent provisioning of the shared binary `pk` HASH + binary `sk` RANGE
/// schema in on-demand billing mode, polling until `ACTIVE`. Tests/dev only.
pub(crate) async fn create_pk_sk_table(ring: &ClientRing, table: &str) -> Result<()> {
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

    let create = ring
        .next_client()
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
        // Already provisioned: idempotent success. Match by metadata code
        // since Alternator may report it untyped.
        Err(e) if e.code() == Some("ResourceInUseException") => {}
        Err(e) => return Err(backend_err("create_table", e)),
    }

    // No generated waiter without the `waiters` SDK feature, so
    // describe-and-sleep until ACTIVE.
    for _ in 0..60 {
        let desc = ring
            .next_client()
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
    Err(QueryError::Backend(format!(
        "dynamo create_table: table {table} did not become ACTIVE in time"
    )))
}

/// Startup connectivity/schema check: the table exists, is `ACTIVE`, and has
/// the expected binary `pk` HASH + binary `sk` RANGE key schema.
pub(crate) async fn validate_pk_sk_table(ring: &ClientRing, table: &str) -> Result<()> {
    let desc = ring
        .next_client()
        .describe_table()
        .table_name(table)
        .send()
        .await
        .map_err(|e| backend_err("describe_table", e))?;
    let Some(table_desc) = desc.table else {
        return Err(QueryError::Backend(format!(
            "dynamo describe_table: table {table} missing from response"
        )));
    };
    if table_desc.table_status != Some(TableStatus::Active) {
        return Err(QueryError::Backend(format!(
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
        return Err(QueryError::Backend(format!(
            "dynamo table {table} schema mismatch: expected binary pk HASH + binary sk RANGE"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_write_chunks_respect_item_count_limit() {
        let items = (0..(BATCH_WRITE_LIMIT * 2 + 1))
            .map(|i| ("table-a".to_string(), i, 1usize))
            .collect();

        let chunks = split_batch_write_chunks(items, BATCH_WRITE_LIMIT);

        assert_eq!(chunks.len(), 3);
        // Larger max_items (Alternator's 100) yields proportionally fewer chunks.
        let items100: Vec<_> = (0..250)
            .map(|i| ("table-a".to_string(), i, 1usize))
            .collect();
        let chunks100 = split_batch_write_chunks(items100, 100);
        let mut lens100: Vec<_> = chunks100.iter().map(|(_, c)| c.len()).collect();
        lens100.sort_unstable();
        assert_eq!(lens100, vec![50, 100, 100]);
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
        let chunks = split_batch_write_chunks(
            vec![
                ("table-a".to_string(), 0, large),
                ("table-a".to_string(), 1, large),
                ("table-a".to_string(), 2, large),
            ],
            BATCH_WRITE_LIMIT,
        );

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
        let chunks = split_batch_write_chunks(
            vec![
                ("table-a".to_string(), 1, 1),
                ("table-b".to_string(), 2, 1),
                ("table-a".to_string(), 3, 1),
            ],
            BATCH_WRITE_LIMIT,
        );

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

        let chunk_tables: Vec<_> = split_batch_write_chunks(items, BATCH_WRITE_LIMIT)
            .into_iter()
            .map(|(table, _)| table)
            .collect();

        assert_eq!(
            chunk_tables,
            vec!["table-a", "table-b", "table-c", "table-a", "table-b", "table-c"]
        );
    }
}
