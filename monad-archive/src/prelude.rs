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

pub use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    ops::RangeInclusive,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

pub use alloy_consensus::{BlockBody, Header, ReceiptEnvelope, ReceiptWithBloom};
pub use alloy_primitives::{U128, U256, U64};
pub use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
pub use bytes::Bytes;
pub use eyre::{bail, eyre, Context, ContextCompat, OptionExt, Report, Result};
pub use futures::{try_join, StreamExt, TryStream, TryStreamExt};
pub use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
pub use tokio::time::sleep;
pub use tracing::{debug, error, info, warn, Level};

pub use crate::{
    archive_reader::{ArchiveReader, LatestKind},
    kvstore::{
        dynamodb::DynamoDBArchive, fs::FsStorage, s3::Bucket, triedb_reader::TriedbReader,
        KVReader, KVReaderErased, KVStore, KVStoreErased,
    },
    metrics::{MetricNames, Metrics},
    model::{
        block_data_archive::*, tx_index_archive::*, BlockDataReader, BlockDataReaderErased,
        BlockDataWithOffsets, HeaderSubset, TxByteOffsets, TxIndexedData,
    },
};

/// Spawn a rayon task and wait for it to complete.
pub async fn spawn_rayon_async<F, R>(func: F) -> Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(|| {
        let _ = tx.send(func());
    });
    rx.await.map_err(Into::into)
}

pub async fn retry_forever<T, F, Fut>(op: F, context: &str, retry_delay: Duration) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    retry_forever_with_observer(op, context, retry_delay, |_attempt, _delay| {}).await
}

pub async fn retry_forever_with_observer<T, F, Fut, Obs>(
    mut op: F,
    context: &str,
    retry_delay: Duration,
    mut on_retry: Obs,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
    Obs: FnMut(u64, Duration),
{
    const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(10);
    const LOG_EVERY: Duration = Duration::from_secs(30);
    const INITIAL_VERBOSE_RETRY_ATTEMPTS: u64 = 3;

    let mut attempts = 0_u64;
    let mut first_retry = None::<Instant>;
    let mut last_log_at = Instant::now()
        .checked_sub(LOG_EVERY)
        .unwrap_or_else(Instant::now);

    loop {
        match op().await {
            Ok(result) => {
                if let Some(first_retry) = first_retry {
                    info!(
                        context,
                        attempts,
                        recovered_after_secs = first_retry.elapsed().as_secs(),
                        "operation recovered after retries"
                    );
                }
                return result;
            }
            Err(e) => {
                attempts = attempts.saturating_add(1);
                let delay = jittered_backoff_delay(retry_delay, MAX_BACKOFF_DELAY, attempts);
                on_retry(attempts, delay);

                let should_log = attempts <= INITIAL_VERBOSE_RETRY_ATTEMPTS
                    || attempts.is_power_of_two()
                    || last_log_at.elapsed() >= LOG_EVERY;

                if should_log {
                    error!(
                        ?e,
                        context,
                        attempts,
                        delay_ms = delay.as_millis(),
                        "retrying operation"
                    );
                    if first_retry.is_none() {
                        first_retry = Some(Instant::now());
                    }
                    last_log_at = Instant::now();
                }

                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn jittered_backoff_delay(base_delay: Duration, max_delay: Duration, attempts: u64) -> Duration {
    let base_ms = base_delay.as_millis().max(1) as u64;
    let max_ms = max_delay.as_millis().max(base_ms as u128) as u64;
    let exponent = attempts.saturating_sub(1).min(16);
    let exp_ms = base_ms.saturating_mul(1_u64 << exponent).min(max_ms);

    // Keep jitter deterministic enough to avoid synchronized retries without adding RNG deps.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter_pct = (nanos % 41) as i32 - 20; // [-20%, +20%]
    let jittered_ms = if jitter_pct >= 0 {
        exp_ms.saturating_add(exp_ms.saturating_mul(jitter_pct as u64) / 100)
    } else {
        exp_ms.saturating_sub(exp_ms.saturating_mul((-jitter_pct) as u64) / 100)
    };

    Duration::from_millis(jittered_ms.max(1))
}
