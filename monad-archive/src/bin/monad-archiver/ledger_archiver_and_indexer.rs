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

use std::path::{Path, PathBuf};

use futures::StreamExt;
use monad_archive::{
    model::bft_ledger::{BftBlockHeader, BftBlockModel},
    prelude::*,
};
use monad_types::BlockId;
use tokio::{
    fs,
    sync::mpsc::{self, Receiver},
};

const UPLOAD_CONCURRENCY: usize = 10;
const BFT_BLOCK_BODY_FILE_PATH: &str = "bodies/";
/// Threshold for keeping header bytes in memory during scan.
/// Above this, we discard bytes to bound memory and re-read during upload.
const HEADER_BYTES_MEMORY_THRESHOLD: usize = 10_000;

struct BftUploadItem {
    seq_num: u64,
    header_id: BlockId,
    header_path: PathBuf,
    /// Header bytes, if kept in memory. None during large catch-up batches.
    header_bytes: Option<Vec<u8>>,
    body_id: monad_types::Hash,
    body_path: PathBuf,
}

pub async fn bft_archive_worker(
    model: BftBlockModel,
    ledger_dir: PathBuf,
    poll_frequency: Duration,
    _metrics: Metrics,
) -> Result<()> {
    let (tx, rx) = mpsc::channel(100);
    tokio::spawn({
        let model = model.clone();
        upload_bft_blocks(model, rx)
    });

    let mut interval = tokio::time::interval(poll_frequency);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let latest_uploaded = model.get_latest_uploaded().await?;
        let items = match index_ledger_dir(&ledger_dir, latest_uploaded).await {
            Ok(items) => items,
            Err(e) => {
                warn!("Failed to index ledger dir: {e:?}");
                continue;
            }
        };

        for item in items {
            tx.send(item).await?;
        }
    }
}

/// Walk the bft headers back from the finalized head to the earliest header
/// that either 1) has already been uploaded or 2) is the earliest header in the ledger dir
///
/// Only reads header files from disk and returns the items needed to
/// A) read the body from disk, B) upload the header and body and
/// C) index seq_num (block_num) => header_id (hash)
async fn index_ledger_dir(
    ledger_dir: &Path,
    latest_uploaded: Option<u64>,
) -> Result<Vec<BftUploadItem>> {
    let headers_dir = ledger_dir.join("headers");

    let mut head = fs::read_link(headers_dir.join("finalized_head")).await?;
    let mut upload_items: Vec<BftUploadItem> = Default::default();
    let mut keep_header_bytes = true;

    loop {
        let bytes = match fs::read(&head).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(earliest_item) = upload_items.last() {
                    warn!(
                        earliest_header_seq_num = ?earliest_item.seq_num,
                        earliest_header_id = ?earliest_item.header_id,
                        earliest_body_id = ?earliest_item.body_id,
                        latest_uploaded,
                        "Gap exists between latest uploaded and earliest header in ledger dir"
                    );
                } else {
                    warn!("Gap exists between latest uploaded and earliest header in ledger dir");
                }

                break;
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        let header = BftBlockHeader::decode(&mut &bytes[..]).wrap_err("Failed to decode header")?;
        let item = BftUploadItem {
            seq_num: header.seq_num.0,
            header_id: header.get_id(),
            header_path: head.clone(),
            header_bytes: if keep_header_bytes { Some(bytes) } else { None },
            body_path: ledger_dir
                .join(BFT_BLOCK_BODY_FILE_PATH)
                .join(hex::encode(header.block_body_id.0)),
            body_id: header.block_body_id.0,
        };
        upload_items.push(item);

        // Check if we've exceeded memory threshold for header bytes
        if keep_header_bytes && upload_items.len() > HEADER_BYTES_MEMORY_THRESHOLD {
            info!(
                "Exceeded header bytes memory threshold ({}), switching to lightweight mode",
                HEADER_BYTES_MEMORY_THRESHOLD
            );
            // Clear bytes from already collected items to free memory
            for item in &mut upload_items {
                item.header_bytes = None;
            }
            keep_header_bytes = false;
        }

        head = headers_dir.join(hex::encode(header.get_parent_id().0));

        if let Some(latest_uploaded) = latest_uploaded {
            if header.seq_num.0 <= latest_uploaded {
                break;
            }
        }
    }

    // Reverse so oldest blocks are uploaded first
    upload_items.reverse();
    Ok(upload_items)
}

/// Worker that uploads bft blocks to the kv store
/// It drains the channel until it is empty and then uploads the blocks in batches,
/// looping on each upload until it succeeds which helps with backpressure and naturally
/// throttles upload concurrency if set too high
///
/// Reading block bodies from disk is not retried in-place; read failures are retried in the next batch
/// to avoid advancing latest_uploaded past gaps.
async fn upload_bft_blocks(
    model: BftBlockModel,
    mut to_upload: Receiver<BftUploadItem>,
) -> Result<()> {
    let mut items = Vec::new();
    while let Some(item) = to_upload.recv().await {
        items.push(item);
        while let Ok(item) = to_upload.try_recv() {
            items.push(item);
        }

        let Some(new_latest) = upload_batch(&mut items, model.clone()).await? else {
            warn!("Skipping latest_uploaded update due to empty or failed batch");
            continue;
        };

        while let Err(e) = model.set_latest_uploaded(new_latest).await {
            error!("Failed to set latest uploaded: {e:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    Ok(())
}

async fn upload_batch(items: &mut Vec<BftUploadItem>, model: BftBlockModel) -> Result<Option<u64>> {
    enum ReadOutcome {
        Ready(BftUploadItem, Vec<u8>, Vec<u8>),
        Failed(BftUploadItem),
    }

    let outcomes: Vec<ReadOutcome> = futures::stream::iter(items.drain(..))
        .map(|mut item: BftUploadItem| async move {
            let read_result: Result<(Vec<u8>, Vec<u8>)> = async {
                // Read header bytes from disk if not cached
                let header_bytes = match item.header_bytes.take() {
                    Some(bytes) => bytes,
                    None => fs::read(&item.header_path)
                        .await
                        .wrap_err("Failed to read bft block header")?,
                };

                let body_bytes = fs::read(&item.body_path)
                    .await
                    .wrap_err("Failed to read bft block body")?;

                Ok((header_bytes, body_bytes))
            }
            .await;

            match read_result {
                Ok((header_bytes, body_bytes)) => {
                    ReadOutcome::Ready(item, header_bytes, body_bytes)
                }
                Err(e) => {
                    error!("Failed to read bft block from disk: {e:?}");
                    ReadOutcome::Failed(item)
                }
            }
        })
        .buffered(UPLOAD_CONCURRENCY)
        .collect()
        .await;

    let mut ready_items = Vec::new();
    let mut failed_items = Vec::new();
    for outcome in outcomes {
        match outcome {
            ReadOutcome::Ready(item, header_bytes, body_bytes) => {
                ready_items.push((item, header_bytes, body_bytes));
            }
            ReadOutcome::Failed(item) => failed_items.push(item),
        }
    }

    let had_failures = !failed_items.is_empty();
    items.extend(failed_items);

    if ready_items.is_empty() {
        return Ok(None);
    }

    let max_uploaded = futures::stream::iter(ready_items)
        .map(|(item, header_bytes, body_bytes)| {
            let model = model.clone();
            async move {
                loop {
                    match upload(&model, &item, header_bytes.clone(), body_bytes.clone()).await {
                        Ok(_) => break,
                        Err(e) => {
                            error!("Failed to upload bft block: {e:?}");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                item.seq_num
            }
        })
        .buffer_unordered(UPLOAD_CONCURRENCY)
        .fold(None, |acc: Option<u64>, num| async move {
            Some(acc.map_or(num, |acc| acc.max(num)))
        })
        .await;

    if had_failures {
        return Ok(None);
    }

    Ok(max_uploaded)
}

async fn upload(
    model: &BftBlockModel,
    item: &BftUploadItem,
    header_bytes: Vec<u8>,
    body_bytes: Vec<u8>,
) -> Result<()> {
    try_join!(
        model.put_header_bytes(&item.header_id, &header_bytes),
        model.put_body_bytes(&item.body_id, &body_bytes),
        model.put_index(item.seq_num, &item.header_id),
    )
    .map(|_| ())
}
