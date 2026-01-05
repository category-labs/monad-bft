use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

use futures::{
    join,
    stream::{self, StreamExt},
};
use monad_archive::{
    model::bft_ledger::{BftBlockHeader, BftBlockModel},
    prelude::*,
};
use monad_types::{BlockId, SeqNum};
use tokio::{
    fs,
    sync::mpsc::{self, Receiver},
};

const UPLOAD_CONCURRENCY: usize = 10;
const BFT_BLOCK_HEADER_FILE_PATH: &str = "headers/";
const BFT_BLOCK_BODY_FILE_PATH: &str = "bodies/";

struct BftUploadItem {
    num: u64,
    header_id: BlockId,
    header_bytes: Vec<u8>,
    body_id: monad_types::Hash,
    body_path: PathBuf,
}

pub async fn bft_archive_worker(
    model: BftBlockModel,
    ledger_dir: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
) -> Result<()> {
    let latest_uploaded = model.get_latest_uploaded().await?;

    let (tx, rx) = mpsc::channel(100);
    tokio::spawn({
        let ledger_dir = ledger_dir.clone();
        upload_bft_blocks(model, ledger_dir, rx)
    });

    let mut interval = tokio::time::interval(poll_frequency);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let items = index_ledger_dir(&ledger_dir, latest_uploaded).await?;
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
    let mut num_to_hash: Vec<BftUploadItem> = Default::default();

    loop {
        let bytes = match fs::read(&head).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                if let Some(earliest_item) = num_to_hash.last() {
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

                return Ok(num_to_hash);
            }
            Err(e) => {
                return Err(e.into());
            }
        };

        let header = BftBlockHeader::decode(&mut &bytes[..]).wrap_err("Failed to decode header")?;
        let block_id = head.file_name().ok_or_eyre("Failed to get file name")?;
        let item = BftUploadItem {
            num: header.seq_num.0,
            header_id: header.get_id(),
            header_bytes: bytes,
            body_path: ledger_dir
                .join(BFT_BLOCK_BODY_FILE_PATH)
                .join(&hex::encode(header.block_body_id.0)),
            body_id: header.block_body_id.0,
        };

        // remove after debugging
        // assert!(
        //     item.header_id == hex::encode(block_id.to_string_lossy().as_bytes()),
        //     "Filename and header id mismatch"
        // );
        num_to_hash.push(item);

        head = headers_dir.join(hex::encode(header.get_parent_id().0));

        if let Some(latest_uploaded) = latest_uploaded {
            if header.seq_num.0 <= latest_uploaded {
                return Ok(num_to_hash);
            }
        }
    }
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
    ledger_dir: PathBuf,
    mut to_upload: Receiver<BftUploadItem>,
) -> Result<()> {
    let mut items = Vec::new();
    while let Some(item) = to_upload.recv().await {
        items.push(item);
        while let Ok(item) = to_upload.try_recv() {
            items.push(item);
        }

        let new_latest = items
            .iter()
            .map(|item| item.num)
            .max()
            .expect("Items non-empty");

        futures::stream::iter(items.drain(..))
            .map(|item: BftUploadItem| async move {
                let body_bytes = fs::read(&item.body_path)
                    .await
                    .wrap_err("Failed to read bft block body")?;

                Ok((item, body_bytes))
            })
            .buffered(UPLOAD_CONCURRENCY)
            .filter_map(|result: Result<(BftUploadItem, Vec<u8>)>| async {
                match result {
                    Ok(item) => Some(item),
                    Err(e) => {
                        error!("Failed to read bft block body from disk: {e:?}");
                        None
                    }
                }
            })
            .for_each_concurrent(Some(UPLOAD_CONCURRENCY), |(item, body_bytes)| {
                let model = model.clone();
                async move {
                    loop {
                        match upload(&model, &item, body_bytes.clone()).await {
                            Ok(_) => break,
                            Err(e) => {
                                error!("Failed to upload bft block: {e:?}");
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                    }
                }
            })
            .await;

        while let Err(e) = model.set_latest_uploaded(new_latest).await {
            error!("Failed to set latest uploaded: {e:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    Ok(())
}

async fn upload(model: &BftBlockModel, item: &BftUploadItem, body_bytes: Vec<u8>) -> Result<()> {
    try_join!(
        model.put_header_bytes(&item.header_id, &item.header_bytes),
        model.put_body_bytes(&item.body_id, &body_bytes),
        model.put_index(item.num, &item.header_id),
    )
    .map(|_| ())
}
