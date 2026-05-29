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
//! ~16 MiB) can live in object storage instead of the embedded fjall LSM.
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

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region},
    error::{ProvideErrorMetadata, SdkError},
    operation::get_object::GetObjectError,
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};

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
    /// Override the S3 endpoint for a compatible service (MinIO/R2/Ceph). Leave
    /// `None` to target real AWS S3 via the default endpoint resolver.
    pub endpoint_url: Option<String>,
    /// AWS region. `None` falls through to the default region provider chain.
    /// Most S3 compatibles accept any value (commonly `"us-east-1"`).
    pub region: Option<String>,
    /// Path-style addressing (`endpoint/bucket/key`) instead of virtual-host
    /// style (`bucket.endpoint/key`). Required by MinIO/Ceph; real S3 and R2
    /// use virtual-host style (`false`).
    pub force_path_style: bool,
    /// Max in-flight PUTs for [`S3BlobStore::apply_writes`]. Clamped to >= 1.
    pub max_concurrency: usize,
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
            endpoint_url: None,
            region: None,
            force_path_style: false,
            max_concurrency: 32,
            credentials: None,
        }
    }
}

struct Inner {
    client: Client,
    bucket: String,
    /// Normalized: no leading/trailing slashes (may be empty).
    root_prefix: String,
    max_concurrency: usize,
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
            endpoint_url,
            region,
            force_path_style,
            max_concurrency,
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
                "S3BlobStoreConfig",
            ));
        }
        let sdk_config = loader.load().await;

        // force_path_style lives on the S3-specific config, not SdkConfig.
        let mut s3_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if force_path_style {
            s3_builder = s3_builder.force_path_style(true);
        }
        let client = Client::from_conf(s3_builder.build());

        Ok(Self {
            inner: std::sync::Arc::new(Inner {
                client,
                bucket,
                root_prefix: normalize_prefix(&root_prefix),
                max_concurrency: max_concurrency.max(1),
            }),
        })
    }

    fn object_key(&self, table: BlobTableId, key: &[u8]) -> String {
        object_key(&self.inner.root_prefix, table, key)
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
        let mut req = self
            .inner
            .client
            .get_object()
            .bucket(&self.inner.bucket)
            .key(&object_key);
        if let Some(range) = range {
            req = req.range(range);
        }

        let resp = match req.send().await {
            Ok(resp) => resp,
            Err(e) if is_no_such_key(&e) => return Ok(None),
            Err(e) if is_invalid_range(&e) => {
                return Err(MonadChainDataError::Decode("invalid blob range"))
            }
            Err(e) => return Err(backend_err("get_object", &object_key, e)),
        };

        let collected = resp.body.collect().await.map_err(|e| {
            MonadChainDataError::Backend(format!("s3 get_object body {object_key}: {e}"))
        })?;
        Ok(Some(collected.into_bytes()))
    }
}

impl BlobStore for S3BlobStore {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        let object_key = self.object_key(table, key);
        self.inner
            .client
            .put_object()
            .bucket(&self.inner.bucket)
            .key(&object_key)
            .body(ByteStream::from(value))
            .send()
            .await
            .map_err(|e| backend_err("put_object", &object_key, e))?;
        Ok(())
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_object(table, key, None).await
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let concurrency = self.inner.max_concurrency;
        futures::stream::iter(writes.into_iter().map(|op| {
            let store = self.clone();
            async move { store.put_blob(op.table, &op.key, op.value).await }
        }))
        .buffer_unordered(concurrency)
        .try_collect::<Vec<()>>()
        .await?;
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
