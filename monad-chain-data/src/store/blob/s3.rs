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

//! S3-API-compatible [`BlobStore`] backend.
//!
//! Talks to any S3-compatible object store -- AWS S3, MinIO, Cloudflare R2,
//! Ceph RGW, the GCS XML API -- so chain-data blobs (block bodies, receipts,
//! compacted bitmap artifacts; write-once, never deleted, 10s of KB up to
//! ~16 MiB) can live in object storage.
//!
//! ## Object-key layout (a wire contract once data exists)
//!
//! [`BlobTableId`] is opaque and the backend owns all namespacing. We map a
//! logical `(table, key)` onto a single bucket as:
//!
//! ```text
//! {root_prefix}/{table.as_str()}/{lowercase-hex(key)}
//! ```
//!
//! `table.as_str()` is already a safe ASCII identifier. Blob keys are arbitrary
//! binary, so they are lowercase-hex encoded -- a total, collision-free mapping
//! into the UTF-8 object-key space. Blob keys here are short composite ids, so
//! the 2x size of hex is irrelevant and keeps keys greppable in bucket tooling.
//!
//! ## apply_writes is not atomic
//!
//! S3 has no multi-object batch/transaction. [`S3BlobStore::apply_writes`] fans
//! the PUTs out concurrently and fails on the first error. This is acceptable
//! here: chain-data blobs are write-once and idempotent, and the publication
//! CAS that gates *visibility* lives in the `MetaStore`, not the blob store. A
//! partially-applied blob batch followed by a failed/retried meta CAS leaves
//! only unreferenced orphan objects, never torn reads.
//!
//! ## read_range pushes the byte range to the server
//!
//! [`S3BlobStore::read_range`] issues an HTTP `Range` request rather than
//! inheriting the trait default (which fetches the whole blob then slices),
//! turning a multi-MiB transfer into the requested few bytes. Note that when a
//! `BlobCompressionStore` wraps this backend it re-overrides `read_range` to
//! fetch-full-then-slice (it must -- the stored bytes are a zstd envelope), so
//! server-side range pushdown only pays off on the raw/uncompressed path.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region},
    error::{ProvideErrorMetadata, SdkError},
    operation::get_object::GetObjectError,
    primitives::ByteStream,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
    Client,
};
use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use tracing::{debug, info, warn};

use crate::{
    error::{MonadChainDataError, Result},
    store::blob::{BlobStore, BlobTableId, BlobWriteOp},
};

/// Static credentials supplied explicitly rather than via the ambient AWS
/// credential chain. Required for most non-AWS compatibles (MinIO, Ceph) where
/// no instance/profile credentials exist.
#[derive(Clone)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for S3Credentials {
    // Never print secret material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Construction parameters for [`S3BlobStore`].
#[derive(Debug, Clone)]
pub struct S3BlobStoreConfig {
    /// Bucket holding every logical blob table (namespaced by key prefix).
    pub bucket: String,
    /// Key prefix prepended to every object, e.g. `"chain-data"`. May be empty.
    /// Leading/trailing slashes are normalized away.
    pub root_prefix: String,
    /// Override S3 endpoints for a compatible service (MinIO/R2/Ceph). Leave
    /// empty to target real AWS S3 via the default endpoint resolver. Multiple
    /// endpoints are client-sharded by object key.
    pub endpoint_urls: Vec<String>,
    /// AWS region. `None` falls through to the default region provider chain.
    /// Most S3 compatibles accept any value (commonly `"us-east-1"`).
    pub region: Option<String>,
    /// AWS profile name. `None` uses the SDK default profile/environment chain.
    pub profile: Option<String>,
    /// Path-style addressing (`endpoint/bucket/key`) instead of virtual-host
    /// style (`bucket.endpoint/key`). Required by MinIO/Ceph; real S3 and R2
    /// use virtual-host style (`false`).
    pub force_path_style: bool,
    /// Max in-flight PUTs for [`S3BlobStore::apply_writes`]. Clamped to >= 1.
    pub max_concurrency: usize,
    /// Create the bucket before returning the store. Intended for real AWS
    /// bootstrap/dev flows; existing buckets owned by the caller are accepted.
    pub create_bucket: bool,
    /// Explicit static credentials. `None` uses the ambient AWS credential
    /// chain (env, profile, instance role, ...).
    pub credentials: Option<S3Credentials>,
}

impl S3BlobStoreConfig {
    /// Minimal config targeting real AWS S3 with ambient credentials and the
    /// default region chain. Callers tweak the remaining fields as needed.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            root_prefix: String::new(),
            endpoint_urls: Vec::new(),
            region: None,
            profile: None,
            force_path_style: false,
            max_concurrency: 32,
            create_bucket: false,
            credentials: None,
        }
    }
}

struct Inner {
    clients: Vec<Client>,
    bucket: String,
    /// Normalized: no leading/trailing slashes (may be empty).
    root_prefix: String,
    max_concurrency: usize,
    endpoint_urls: Vec<String>,
    read_stats: S3ReadStats,
}

#[derive(Debug, Default)]
struct S3ReadStats {
    started: AtomicU64,
    completed: AtomicU64,
    errors: AtomicU64,
    canceled: AtomicU64,
    range_gets: AtomicU64,
    full_gets: AtomicU64,
    bytes: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct S3ReadStatsSnapshot {
    pub started: u64,
    pub completed: u64,
    pub errors: u64,
    pub canceled: u64,
    pub range_gets: u64,
    pub full_gets: u64,
    pub bytes: u64,
    pub in_flight: u64,
    pub max_in_flight: u64,
}

/// S3-API-compatible [`BlobStore`]. Cheaply cloneable -- all state lives behind
/// an `Arc`, and the underlying SDK `Client` is itself `Arc`-backed.
#[derive(Clone)]
pub struct S3BlobStore {
    inner: std::sync::Arc<Inner>,
}

impl std::fmt::Debug for S3BlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3BlobStore")
            .field("bucket", &self.inner.bucket)
            .field("root_prefix", &self.inner.root_prefix)
            .field("max_concurrency", &self.inner.max_concurrency)
            .field("endpoint_count", &self.inner.clients.len())
            .finish_non_exhaustive()
    }
}

impl S3BlobStore {
    /// Builds the SDK client from `config` and returns a ready store. Async
    /// because resolving the AWS credential/region chain performs I/O.
    pub async fn new(config: S3BlobStoreConfig) -> Result<Self> {
        let S3BlobStoreConfig {
            bucket,
            root_prefix,
            endpoint_urls,
            region,
            profile,
            force_path_style,
            max_concurrency,
            create_bucket,
            credentials,
        } = config;
        let endpoint_is_aws = endpoint_urls.is_empty();

        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = region {
            loader = loader.region(Region::new(region));
        }
        if let Some(profile) = profile {
            loader = loader.profile_name(profile);
        }
        if let Some(creds) = credentials {
            loader = loader.credentials_provider(Credentials::new(
                creds.access_key_id,
                creds.secret_access_key,
                creds.session_token,
                None,
                "S3BlobStoreConfig",
            ));
        }
        let sdk_config = loader.load().await;
        let region_for_create = sdk_config.region().map(|r| r.as_ref().to_string());

        let clients = if endpoint_urls.is_empty() {
            vec![build_client(&sdk_config, None, force_path_style)]
        } else {
            endpoint_urls
                .iter()
                .map(|endpoint| {
                    build_client(&sdk_config, Some(endpoint.as_str()), force_path_style)
                })
                .collect::<Vec<_>>()
        };

        if create_bucket {
            create_bucket_if_needed(
                &clients[0],
                &bucket,
                region_for_create.as_deref(),
                endpoint_is_aws,
            )
            .await?;
        }
        futures::stream::iter(clients.clone())
            .map(|client| {
                let bucket = bucket.clone();
                async move { validate_bucket_access(&client, &bucket).await }
            })
            .buffer_unordered(clients.len().max(1))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(Self {
            inner: std::sync::Arc::new(Inner {
                clients,
                bucket,
                root_prefix: normalize_prefix(&root_prefix),
                max_concurrency: max_concurrency.max(1),
                endpoint_urls,
                read_stats: S3ReadStats::default(),
            }),
        })
    }

    fn object_key(&self, table: BlobTableId, key: &[u8]) -> String {
        object_key(&self.inner.root_prefix, table, key)
    }

    fn client_for_object_key(&self, object_key: &str) -> (&Client, Option<&str>) {
        let idx = if self.inner.clients.len() == 1 {
            0
        } else {
            let mut hasher = DefaultHasher::new();
            object_key.hash(&mut hasher);
            (hasher.finish() as usize) % self.inner.clients.len()
        };
        (
            &self.inner.clients[idx],
            self.inner.endpoint_urls.get(idx).map(String::as_str),
        )
    }

    /// Single GET, optionally with a `Range` header. Returns `Ok(None)` when the
    /// object does not exist.
    async fn get_object(
        &self,
        table: BlobTableId,
        key: &[u8],
        range: Option<String>,
    ) -> Result<Option<Bytes>> {
        let object_key = self.object_key(table, key);
        let (client, _) = self.client_for_object_key(&object_key);
        let is_range = range.is_some();
        let mut req = client
            .get_object()
            .bucket(&self.inner.bucket)
            .key(&object_key);
        if let Some(range) = range {
            req = req.range(range);
        }

        let read_guard = self.read_started(is_range);
        let resp = match req.send().await {
            Ok(resp) => resp,
            Err(e) if is_no_such_key(&e) => {
                read_guard.finish(false, 0);
                return Ok(None);
            }
            Err(e) if is_invalid_range(&e) => {
                read_guard.finish(true, 0);
                return Err(MonadChainDataError::Decode("invalid blob range"));
            }
            Err(e) => {
                read_guard.finish(true, 0);
                return Err(backend_err("get_object", &object_key, e));
            }
        };

        let collected = match resp.body.collect().await {
            Ok(collected) => collected,
            Err(e) => {
                read_guard.finish(true, 0);
                return Err(MonadChainDataError::Backend(format!(
                    "s3 get_object body {object_key}: {e}"
                )));
            }
        };
        let bytes = collected.into_bytes();
        read_guard.finish(false, bytes.len() as u64);
        Ok(Some(bytes))
    }

    fn read_started(&self, is_range: bool) -> S3ReadGuard {
        let stats = &self.inner.read_stats;
        stats.started.fetch_add(1, Ordering::Relaxed);
        if is_range {
            stats.range_gets.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.full_gets.fetch_add(1, Ordering::Relaxed);
        }
        let in_flight = stats.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        let mut prev = stats.max_in_flight.load(Ordering::Relaxed);
        while in_flight > prev {
            match stats.max_in_flight.compare_exchange_weak(
                prev,
                in_flight,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(next) => prev = next,
            }
        }
        S3ReadGuard {
            inner: self.inner.clone(),
            finished: false,
        }
    }

    pub fn take_read_stats(&self) -> S3ReadStatsSnapshot {
        let stats = &self.inner.read_stats;
        S3ReadStatsSnapshot {
            started: stats.started.swap(0, Ordering::Relaxed),
            completed: stats.completed.swap(0, Ordering::Relaxed),
            errors: stats.errors.swap(0, Ordering::Relaxed),
            canceled: stats.canceled.swap(0, Ordering::Relaxed),
            range_gets: stats.range_gets.swap(0, Ordering::Relaxed),
            full_gets: stats.full_gets.swap(0, Ordering::Relaxed),
            bytes: stats.bytes.swap(0, Ordering::Relaxed),
            in_flight: stats.in_flight.load(Ordering::Relaxed),
            max_in_flight: stats.max_in_flight.swap(0, Ordering::Relaxed),
        }
    }
}

struct S3ReadGuard {
    inner: std::sync::Arc<Inner>,
    finished: bool,
}

impl S3ReadGuard {
    fn finish(mut self, error: bool, bytes: u64) {
        self.finished = true;
        let stats = &self.inner.read_stats;
        if error {
            stats.errors.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.completed.fetch_add(1, Ordering::Relaxed);
            stats.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for S3ReadGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let stats = &self.inner.read_stats;
        stats.canceled.fetch_add(1, Ordering::Relaxed);
        stats.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

fn build_client(
    sdk_config: &aws_config::SdkConfig,
    endpoint_url: Option<&str>,
    force_path_style: bool,
) -> Client {
    // force_path_style and endpoint_url live on the S3-specific config, not SdkConfig.
    let mut s3_builder = aws_sdk_s3::config::Builder::from(sdk_config);
    if let Some(endpoint_url) = endpoint_url {
        s3_builder = s3_builder.endpoint_url(endpoint_url);
    }
    if force_path_style {
        s3_builder = s3_builder.force_path_style(true);
    }
    Client::from_conf(s3_builder.build())
}

/// Idempotent-ish bucket provisioning for startup bootstrap. AWS S3 requires a
/// location constraint outside us-east-1; most compatible endpoints either
/// ignore it or reject it, so only set it when targeting real AWS.
async fn create_bucket_if_needed(
    client: &Client,
    bucket: &str,
    region: Option<&str>,
    endpoint_is_aws: bool,
) -> Result<()> {
    let mut req = client.create_bucket().bucket(bucket);
    if endpoint_is_aws {
        let region = region.unwrap_or("us-east-1");
        if region != "us-east-1" {
            let cfg = CreateBucketConfiguration::builder()
                .location_constraint(BucketLocationConstraint::from(region))
                .build();
            req = req.create_bucket_configuration(cfg);
        }
    }

    match req.send().await {
        Ok(_) => Ok(()),
        // Re-running bootstrap against a bucket we own should be harmless.
        Err(e) if e.code() == Some("BucketAlreadyOwnedByYou") => Ok(()),
        Err(e) => Err(backend_err("create_bucket", bucket, e)),
    }
}

async fn validate_bucket_access(client: &Client, bucket: &str) -> Result<()> {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| backend_err("head_bucket", bucket, e))?;
    Ok(())
}

impl BlobStore for S3BlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let object_key = self.object_key(table, key);
        let (client, endpoint_url) = self.client_for_object_key(&object_key);
        let value_len = value.len();
        let started = std::time::Instant::now();
        let put = client
            .put_object()
            .bucket(&self.inner.bucket)
            .key(&object_key)
            .body(ByteStream::from(value))
            .send();
        tokio::pin!(put);

        let mut next_warn = std::time::Duration::from_secs(30);
        let resp = loop {
            tokio::select! {
                result = &mut put => break result,
                _ = tokio::time::sleep(next_warn) => {
                    warn!(
                        table = %table.as_str(),
                        object_key = %object_key,
                        endpoint_url,
                        value_len,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "s3 put_object still in flight"
                    );
                    next_warn = std::time::Duration::from_secs(120);
                }
            }
        };

        resp.map_err(|e| backend_err("put_object", &object_key, e))?;
        if started.elapsed() >= std::time::Duration::from_secs(10) {
            debug!(
                table = %table.as_str(),
                object_key = %object_key,
                endpoint_url,
                value_len,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "s3 put_object completed slowly"
            );
        }
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_object(table, key, None).await
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let write_count = writes.len();
        let total_bytes = writes.iter().map(|op| op.value.len()).sum::<usize>();
        let concurrency = self.inner.max_concurrency;
        info!(
            write_count,
            total_bytes, concurrency, "s3 apply_writes starting PUT batch"
        );
        futures::stream::iter(writes.into_iter().map(|op| {
            let store = self.clone();
            async move { store.put_blob(op.table, &op.key, op.value).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<()>>()
        .await?;
        info!(
            write_count,
            total_bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "s3 apply_writes completed PUT batch"
        );
        Ok(())
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
        // Empty range: we still must distinguish a missing object (None) from a
        // present one (Some(empty)), and S3 cannot express a zero-length range,
        // so fall back to an existence-revealing point read.
        if start == end_exclusive {
            return Ok(self.get_blob(table, key).await?.map(|_| Bytes::new()));
        }
        // HTTP byte ranges are inclusive on both ends; our end is exclusive.
        // S3 clamps an end past EOF to the last byte, matching the trait
        // default's `end_exclusive.min(blob.len())`. A start at/after EOF yields
        // 416, which `get_object` maps to the same `Decode` the default returns.
        let range = format!("bytes={}-{}", start, end_exclusive - 1);
        self.get_object(table, key, Some(range)).await
    }
}

/// Normalizes a configured root prefix to have no leading/trailing slashes.
fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

/// Builds the S3 object key for a logical `(table, key)`. See the module docs
/// for the layout contract.
fn object_key(root_prefix: &str, table: BlobTableId, key: &[u8]) -> String {
    let table = table.as_str();
    let mut out =
        String::with_capacity(root_prefix.len() + 1 + table.len() + 1 + key.len() * 2 + 1);
    if !root_prefix.is_empty() {
        out.push_str(root_prefix);
        out.push('/');
    }
    out.push_str(table);
    out.push('/');
    push_hex(&mut out, key);
    out
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
}

fn backend_err<E, R>(op: &str, object_key: &str, e: SdkError<E, R>) -> MonadChainDataError
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug,
{
    // Prefer the service-reported code/message; SdkError's own Display is terse.
    let detail = match e.code() {
        Some(code) => format!("{code}: {}", e.message().unwrap_or("")),
        None => e.to_string(),
    };
    MonadChainDataError::Backend(format!("s3 {op} {object_key}: {detail}"))
}

fn is_no_such_key<R>(e: &SdkError<GetObjectError, R>) -> bool {
    matches!(e, SdkError::ServiceError(se) if matches!(se.err(), GetObjectError::NoSuchKey(_)))
        || e.code() == Some("NoSuchKey")
}

fn is_invalid_range<R>(e: &SdkError<GetObjectError, R>) -> bool {
    e.code() == Some("InvalidRange")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: BlobTableId = BlobTableId::new("blocks");

    #[test]
    fn object_key_layout_with_prefix() {
        assert_eq!(
            object_key("chain-data", TABLE, &[0x00, 0xab, 0xff]),
            "chain-data/blocks/00abff"
        );
    }

    #[test]
    fn object_key_layout_without_prefix() {
        assert_eq!(object_key("", TABLE, &[0x12, 0x34]), "blocks/1234");
    }

    #[test]
    fn prefix_normalization_strips_slashes() {
        assert_eq!(normalize_prefix("/a/b/"), "a/b");
        assert_eq!(normalize_prefix(""), "");
        assert_eq!(normalize_prefix("///"), "");
        // A normalized prefix produces no double slashes in the key.
        assert_eq!(
            object_key(&normalize_prefix("/p/"), TABLE, &[0x01]),
            "p/blocks/01"
        );
    }

    #[test]
    fn hex_encodes_full_byte_range() {
        let mut s = String::new();
        push_hex(&mut s, &[0x00, 0x0f, 0xf0, 0xff, 0x7e]);
        assert_eq!(s, "000ff0ff7e");
    }

    #[test]
    fn empty_key_encodes_to_empty_hex() {
        assert_eq!(object_key("", TABLE, &[]), "blocks/");
    }
}
