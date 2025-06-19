use std::future::Future;
pub use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub use alloy_consensus::{BlockBody, Header, ReceiptEnvelope, ReceiptWithBloom};
pub use alloy_primitives::{U128, U256, U64};
pub use eyre::{bail, eyre, Context, ContextCompat, Result};
pub use futures::{try_join, StreamExt, TryStream, TryStreamExt};
pub use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
pub use tokio::time::sleep;
pub use tracing::{debug, error, info, warn, Level};

pub use crate::{
    archive_reader::{ArchiveReader, LatestKind},
    kvstore::{
        dynamodb::DynamoDBArchive, s3::S3Bucket, triedb_reader::TriedbReader, KVReader,
        KVReaderErased, KVStore, KVStoreErased,
    },
    metrics::{MetricNames, Metrics},
    model::{
        block_data_archive::*, tx_index_archive::*, BlockArchiver, BlockDataReader,
        BlockDataReaderErased, HeaderSubset, TxByteOffsets, TxIndexedData,
    },
    model_v2::ModelV2,
};

pub async fn spawn_eyre<T: Future<Output = Result<O>> + Send + 'static, O: Send + 'static>(
    req: T,
    msg: &'static str,
) -> Result<O> {
    let x = tokio::spawn(req).await.wrap_err(msg)?;
    x.wrap_err(msg)
}
