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
pub use bytes::Bytes;
pub use eyre::{bail, eyre, Context, ContextCompat, OptionExt, Report};
pub use futures::{try_join, StreamExt, TryStream, TryStreamExt};
pub use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
pub use tokio::time::sleep;
pub use tracing::{debug, error, info, warn, Level};

pub use crate::{
    archive_reader::{ArchiveReader, LatestKind},
    error::{Error as ArchiveError, ErrorKind, Result, ResultExt, WrapErr},
    kvstore::{
        dynamodb::DynamoDBArchive, fs::FsStorage, s3::Bucket, triedb_reader::TriedbReader,
        KVReader, KVReaderErased, KVStore, KVStoreErased,
    },
    metrics::{MetricNames, Metrics},
    model::{
        block_data_archive::*, tx_index_archive::*, BlockDataReader, BlockDataReaderErased,
        BlockDataWithOffsets, HeaderSubset, TxByteOffsets, TxIndexedData,
    },
    worker_loop::{run_worker_loop, BlockRangeWorker, WorkerLoopConfig},
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
    Ok(rx.await.map_err(|e| eyre::eyre!("{e}"))?)
}
