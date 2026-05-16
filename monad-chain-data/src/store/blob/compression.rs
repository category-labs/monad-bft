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

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use bytes::Bytes;
use rayon::prelude::*;

use crate::{
    error::{MonadChainDataError, Result},
    store::blob::{BlobStore, BlobTableId, BlobWriteOp},
};

const MAGIC: &[u8; 8] = b"MCDBLB1\0";
const CODEC_ZSTD: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 1 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobCompressionConfig {
    pub enabled: bool,
    pub zstd_level: i32,
    pub min_bytes: usize,
}

impl BlobCompressionConfig {
    pub const fn none() -> Self {
        Self {
            enabled: false,
            zstd_level: 0,
            min_bytes: usize::MAX,
        }
    }

    pub const fn zstd(zstd_level: i32, min_bytes: usize) -> Self {
        Self {
            enabled: true,
            zstd_level,
            min_bytes,
        }
    }

    pub const fn is_enabled(self) -> bool {
        self.enabled
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BlobCompressionSnapshot {
    pub raw_input_bytes: u64,
    pub stored_output_bytes: u64,
    pub compressed_count: u64,
    pub raw_count: u64,
    pub decode_count: u64,
    pub encode_wall_us: u64,
    pub encode_work_us: u64,
    pub decode_work_us: u64,
}

impl BlobCompressionSnapshot {
    pub fn saturating_sub(&self, previous: &Self) -> Self {
        Self {
            raw_input_bytes: self
                .raw_input_bytes
                .saturating_sub(previous.raw_input_bytes),
            stored_output_bytes: self
                .stored_output_bytes
                .saturating_sub(previous.stored_output_bytes),
            compressed_count: self
                .compressed_count
                .saturating_sub(previous.compressed_count),
            raw_count: self.raw_count.saturating_sub(previous.raw_count),
            decode_count: self.decode_count.saturating_sub(previous.decode_count),
            encode_wall_us: self.encode_wall_us.saturating_sub(previous.encode_wall_us),
            encode_work_us: self.encode_work_us.saturating_sub(previous.encode_work_us),
            decode_work_us: self.decode_work_us.saturating_sub(previous.decode_work_us),
        }
    }

    pub fn ratio(&self) -> f64 {
        if self.raw_input_bytes == 0 {
            1.0
        } else {
            self.stored_output_bytes as f64 / self.raw_input_bytes as f64
        }
    }
}

#[derive(Debug, Default)]
struct BlobCompressionCounters {
    raw_input_bytes: AtomicU64,
    stored_output_bytes: AtomicU64,
    compressed_count: AtomicU64,
    raw_count: AtomicU64,
    decode_count: AtomicU64,
    encode_wall_us: AtomicU64,
    encode_work_us: AtomicU64,
    decode_work_us: AtomicU64,
}

#[derive(Debug, Clone, Default)]
pub struct BlobCompressionStats {
    counters: Arc<BlobCompressionCounters>,
}

impl BlobCompressionStats {
    pub fn snapshot(&self) -> BlobCompressionSnapshot {
        BlobCompressionSnapshot {
            raw_input_bytes: self.counters.raw_input_bytes.load(Ordering::Relaxed),
            stored_output_bytes: self.counters.stored_output_bytes.load(Ordering::Relaxed),
            compressed_count: self.counters.compressed_count.load(Ordering::Relaxed),
            raw_count: self.counters.raw_count.load(Ordering::Relaxed),
            decode_count: self.counters.decode_count.load(Ordering::Relaxed),
            encode_wall_us: self.counters.encode_wall_us.load(Ordering::Relaxed),
            encode_work_us: self.counters.encode_work_us.load(Ordering::Relaxed),
            decode_work_us: self.counters.decode_work_us.load(Ordering::Relaxed),
        }
    }

    fn record_encode(&self, raw_len: usize, stored_len: usize, compressed: bool, work_us: u64) {
        self.counters
            .raw_input_bytes
            .fetch_add(raw_len as u64, Ordering::Relaxed);
        self.counters
            .stored_output_bytes
            .fetch_add(stored_len as u64, Ordering::Relaxed);
        if compressed {
            self.counters
                .compressed_count
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.raw_count.fetch_add(1, Ordering::Relaxed);
        }
        self.counters
            .encode_work_us
            .fetch_add(work_us, Ordering::Relaxed);
    }

    fn record_encode_wall(&self, wall_us: u64) {
        self.counters
            .encode_wall_us
            .fetch_add(wall_us, Ordering::Relaxed);
    }

    fn record_decode(&self, work_us: u64) {
        self.counters.decode_count.fetch_add(1, Ordering::Relaxed);
        self.counters
            .decode_work_us
            .fetch_add(work_us, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub struct BlobCompressionStore<B> {
    inner: B,
    config: BlobCompressionConfig,
    stats: BlobCompressionStats,
}

impl<B> BlobCompressionStore<B> {
    pub fn new(inner: B, config: BlobCompressionConfig, stats: BlobCompressionStats) -> Self {
        Self {
            inner,
            config,
            stats,
        }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    pub fn stats(&self) -> &BlobCompressionStats {
        &self.stats
    }
}

impl<B: BlobStore> BlobStore for BlobCompressionStore<B> {
    async fn put_blob(&self, table: BlobTableId, key: &[u8], value: Bytes) -> Result<()> {
        if !self.config.is_enabled() {
            return self.inner.put_blob(table, key, value).await;
        }
        let encoded = encode_value(value, self.config, &self.stats)?;
        self.inner.put_blob(table, key, encoded).await
    }

    async fn get_blob(&self, table: BlobTableId, key: &[u8]) -> Result<Option<Bytes>> {
        let Some(value) = self.inner.get_blob(table, key).await? else {
            return Ok(None);
        };
        decode_value(value, &self.stats).map(Some)
    }

    async fn apply_writes(&self, writes: Vec<BlobWriteOp>) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        if !self.config.is_enabled() {
            return self.inner.apply_writes(writes).await;
        }
        let config = self.config;
        let stats = self.stats.clone();
        let wall = Instant::now();
        let encoded = writes
            .into_par_iter()
            .map(|op| {
                let value = encode_value(op.value, config, &stats)?;
                Ok(BlobWriteOp { value, ..op })
            })
            .collect::<Result<Vec<_>>>()?;
        stats.record_encode_wall(wall.elapsed().as_micros() as u64);
        self.inner.apply_writes(encoded).await
    }

    async fn read_range(
        &self,
        table: BlobTableId,
        key: &[u8],
        start: usize,
        end_exclusive: usize,
    ) -> Result<Option<Bytes>> {
        let Some(blob) = self.get_blob(table, key).await? else {
            return Ok(None);
        };
        if start > end_exclusive || start > blob.len() {
            return Err(MonadChainDataError::Decode("invalid blob range"));
        }
        Ok(Some(blob.slice(start..end_exclusive.min(blob.len()))))
    }
}

fn encode_value(
    value: Bytes,
    config: BlobCompressionConfig,
    stats: &BlobCompressionStats,
) -> Result<Bytes> {
    if !config.is_enabled() {
        return Ok(value);
    }
    if value.len() < config.min_bytes || value.starts_with(MAGIC) {
        stats.record_encode(value.len(), value.len(), false, 0);
        return Ok(value);
    }

    let raw_len = value.len();
    let started = Instant::now();
    let compressed = zstd::bulk::compress(value.as_ref(), config.zstd_level)
        .map_err(|e| MonadChainDataError::Backend(format!("zstd compress blob: {e}")))?;
    let work_us = started.elapsed().as_micros() as u64;

    if compressed.len() + HEADER_LEN >= raw_len {
        stats.record_encode(raw_len, raw_len, false, work_us);
        return Ok(value);
    }

    let mut out = Vec::with_capacity(HEADER_LEN + compressed.len());
    out.extend_from_slice(MAGIC);
    out.push(CODEC_ZSTD);
    out.extend_from_slice(&(raw_len as u64).to_be_bytes());
    out.extend_from_slice(&compressed);
    stats.record_encode(raw_len, out.len(), true, work_us);
    Ok(Bytes::from(out))
}

fn decode_value(value: Bytes, stats: &BlobCompressionStats) -> Result<Bytes> {
    if !value.starts_with(MAGIC) {
        return Ok(value);
    }
    let header = value.get(..HEADER_LEN).ok_or(MonadChainDataError::Decode(
        "compressed blob header too short",
    ))?;
    let codec = header[MAGIC.len()];
    if codec != CODEC_ZSTD {
        return Err(MonadChainDataError::Decode(
            "unsupported compressed blob codec",
        ));
    }
    let raw_len = u64::from_be_bytes(
        header[MAGIC.len() + 1..HEADER_LEN]
            .try_into()
            .map_err(|_| MonadChainDataError::Decode("compressed blob raw length"))?,
    ) as usize;
    let started = Instant::now();
    let decoded = zstd::bulk::decompress(&value[HEADER_LEN..], raw_len)
        .map_err(|e| MonadChainDataError::Backend(format!("zstd decompress blob: {e}")))?;
    stats.record_decode(started.elapsed().as_micros() as u64);
    Ok(Bytes::from(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::blob::InMemoryBlobStore;

    #[test]
    fn raw_legacy_value_decodes_without_envelope() {
        let stats = BlobCompressionStats::default();
        let value = Bytes::from_static(b"legacy bytes");
        assert_eq!(decode_value(value.clone(), &stats).unwrap(), value);
        assert_eq!(stats.snapshot().decode_count, 0);
    }

    #[test]
    fn compression_round_trip_uses_envelope_when_smaller() {
        let stats = BlobCompressionStats::default();
        let value = Bytes::from(vec![7_u8; 16 * 1024]);
        let encoded =
            encode_value(value.clone(), BlobCompressionConfig::zstd(1, 1), &stats).expect("encode");
        assert!(encoded.starts_with(MAGIC));
        assert!(encoded.len() < value.len());
        assert_eq!(decode_value(encoded, &stats).expect("decode"), value);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.compressed_count, 1);
        assert_eq!(snapshot.decode_count, 1);
    }

    #[tokio::test]
    async fn store_reads_raw_and_compressed_values() {
        let inner = InMemoryBlobStore::default();
        let stats = BlobCompressionStats::default();
        let store = BlobCompressionStore::new(
            inner.clone(),
            BlobCompressionConfig::zstd(1, 1),
            stats.clone(),
        );
        let table = BlobTableId::new("blob_compression_test");
        inner
            .put_blob(table, b"raw", Bytes::from_static(b"old raw"))
            .await
            .unwrap();
        store
            .put_blob(table, b"compressed", Bytes::from(vec![3_u8; 4096]))
            .await
            .unwrap();

        assert_eq!(
            store.get_blob(table, b"raw").await.unwrap().unwrap(),
            Bytes::from_static(b"old raw")
        );
        assert_eq!(
            store.get_blob(table, b"compressed").await.unwrap().unwrap(),
            Bytes::from(vec![3_u8; 4096])
        );
        assert_eq!(stats.snapshot().compressed_count, 1);
    }
}
