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
    model::bft_block_index::{
        bft_block_num_key_from_seq_num, put_index, BftBlockHeader, BftBlockIndex,
    },
    prelude::*,
};
use monad_types::SeqNum;
use tokio::{
    fs,
    sync::mpsc::{self, Receiver},
};

use crate::generic_folder_archiver::recursive_dir_archiver;

// const BFT_BLOCK_PREFIX: &str = "bft_block/";
// const BFT_BLOCK_HEADER_EXTENSION: &str = ".header";
// const BFT_BLOCK_BODY_EXTENSION: &str = ".body";

// const BFT_BLOCK_HEADER_FILE_PATH: &str = "headers/";
// const BFT_BLOCK_BODY_FILE_PATH: &str = "bodies/";

// Number of concurrent uploads
const UPLOAD_CONCURRENCY: usize = 10;

const BFT_BLOCK_BODIES_PREFIX: &str = "ledger/bodies/";
const BFT_BLOCK_HEADERS_PREFIX: &str = "ledger/headers/";

fn bft_block_header_key_from_hash(hash: &str) -> String {
    format!("{BFT_BLOCK_HEADERS_PREFIX}{hash}")
}

fn bft_block_body_key_from_hash(hash: &str) -> String {
    format!("{BFT_BLOCK_BODIES_PREFIX}{hash}")
}

const LATEST_UPLOADED_KEY: &str = "ledger/latest_uploaded";

async fn get_latest_uploaded(kv: &KVStoreErased) -> Result<Option<u64>> {
    let Some(bytes) =  kv.get(LATEST_UPLOADED_KEY).await? else {
        return Ok(None);
    };
    let mut arr: [u8; 8] = Default::default();
    arr.copy_from_slice(&bytes[..]);
    Ok(Some(u64::from_be_bytes(arr)))
}

async fn set_latest_uploaded(kv: &KVStoreErased, latest_uploaded: u64) -> Result<()> {
    kv.put(LATEST_UPLOADED_KEY, latest_uploaded.to_be_bytes().to_vec())
        .await
        .wrap_err("Failed to set latest uploaded")
}

async fn bft_archive_worker(kv: KVStoreErased, ledger_dir: &Path) -> Result<()> {
    let latest_uploaded = get_latest_uploaded(&kv).await?;

    let (tx, rx) = mpsc::channel(100);
    tokio::spawn(async move {
        upload_bft_blocks(kv, rx).await;
    });

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let items = index_ledger_dir(ledger_dir, latest_uploaded).await?;
        for item in items {
            tx.send(item).await?;
        }
    }
}

async fn upload_bft_blocks(
    kv: KVStoreErased,
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
                let body_path = PathBuf::from(BFT_BLOCK_BODIES_PREFIX).join(&item.body_id);
                let body_bytes = fs::read(body_path)
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
                let kv = kv.clone();
                async move {
                    loop {
                        match upload(&kv, &item, body_bytes.clone()).await {
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

        set_latest_uploaded(&kv, new_latest).await?;
    }

    Ok(())
}

async fn upload(kv: &KVStoreErased, item: &BftUploadItem, body_bytes: Vec<u8>) -> Result<()> {
    try_join!(
        // Header id => header bytes
        kv.put(
            bft_block_header_key_from_hash(&item.header_id),
            item.header_bytes.clone()
        ),
        // Body id => body bytes
        kv.put(
            bft_block_body_key_from_hash(&item.body_id),
            body_bytes.clone()
        ),
        kv.put(
            bft_block_num_key_from_seq_num(&SeqNum(item.num)),
            item.header_id.as_bytes().to_vec()
        ),
    )
    .map(|_| ())
    .wrap_err("Failed to upload bft block")
}

struct BftUploadItem {
    num: u64,
    header_id: String,
    header_bytes: Vec<u8>,
    body_id: String,
}

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
                        earliest_header_num = earliest_item.num,
                        earliest_header_id = earliest_item.header_id,
                        earliest_body_id = earliest_item.body_id,
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
            header_id: hex::encode(header.get_id().0),
            header_bytes: bytes,
            body_id: hex::encode(header.block_body_id.0),
        };

        // remove after debugging
        assert!(
            item.header_id == hex::encode(block_id.to_string_lossy().as_bytes()),
            "Filename and header id mismatch"
        );
        num_to_hash.push(item);

        head = headers_dir.join(hex::encode(header.get_parent_id().0));

        if let Some(latest_uploaded) = latest_uploaded {
            if header.seq_num.0 <= latest_uploaded {
                return Ok(num_to_hash);
            }
        }
    }
}
