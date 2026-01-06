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
    num: u64,
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
                        earliest_header_num = ?earliest_item.num,
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
            num: header.seq_num.0,
            header_id: header.get_id(),
            header_path: head.clone(),
            header_bytes: if keep_header_bytes { Some(bytes) } else { None },
            body_path: ledger_dir
                .join(BFT_BLOCK_BODY_FILE_PATH)
                .join(&hex::encode(header.block_body_id.0)),
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
/// Reading block bodies from disk is not retried since those failures are not intermitent and
/// will not eventually resolve
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
            warn!("No items uploaded in batch, skipping latest_uploaded update");
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
    Ok(futures::stream::iter(items.drain(..))
        .map(|mut item: BftUploadItem| async move {
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

            Ok((item, header_bytes, body_bytes))
        })
        .buffered(UPLOAD_CONCURRENCY)
        .filter_map(|result: Result<(BftUploadItem, Vec<u8>, Vec<u8>)>| async {
            match result {
                Ok(item) => Some(item),
                Err(e) => {
                    error!("Failed to read bft block from disk: {e:?}");
                    None
                }
            }
        })
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
                item.num
            }
        })
        .buffer_unordered(UPLOAD_CONCURRENCY)
        .fold(None, |acc: Option<u64>, num| async move {
            Some(acc.map_or(num, |acc| acc.max(num)))
        })
        .await)
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
        model.put_index(item.num, &item.header_id),
    )
    .map(|_| ())
}
