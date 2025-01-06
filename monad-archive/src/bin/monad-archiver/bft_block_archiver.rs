use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};

use eyre::{Context, Result};
use futures::StreamExt;
use monad_archive::BlobStoreErased;
use tokio::time::sleep;
use tracing::{debug, error, info};

use crate::{BlobStore, Metrics};

const BFT_BLOCK_PREFIX: &'static str = "bft_block/";

pub async fn parallel_upload_iter(
    uploaded: &mut HashSet<OsString>,
    s3: BlobStoreErased,
    fnames: Vec<PathBuf>,
) -> usize {
    futures::stream::iter(fnames)
        .filter_map(|path| {
            let s3 = s3.clone();
            spawning_upload(s3, path)
        })
        .buffer_unordered(20)
        .map(|file_name_uploaded| {
            info!(?file_name_uploaded, "Uploaded bft block");
            uploaded.insert(file_name_uploaded);
            futures::future::ready(())
        })
        .count()
        .await
}

async fn spawning_upload(
    s3: BlobStoreErased,
    path: PathBuf,
) -> Option<futures::future::Ready<OsString>> {
    tokio::spawn(upload(s3, path)).await.ok()?
}

async fn upload(s3: BlobStoreErased, path: PathBuf) -> Option<futures::future::Ready<OsString>> {
    let fname_os = path.file_name()?.to_os_string();
    let fname = fname_os.to_str()?;

    let data = match tokio::fs::read(&path).await {
        Ok(x) => x,
        Err(e) => {
            error!(?e, ?path, "Failed to read bft block file from folder");
            return None;
        }
    };

    match s3.upload(&format!("{BFT_BLOCK_PREFIX}{fname}"), data).await {
        Ok(_) => Some(futures::future::ready(fname_os)),
        Err(e) => {
            error!(?e, fname, "Failed to archive bft block");
            None
        }
    }
}

pub async fn bft_block_archive_worker(
    s3: BlobStoreErased,
    header_path: PathBuf,
    body_path: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
) -> Result<()> {
    info!("Fetching set of already uploaded bft blocks...");
    let mut uploaded = initialize_uploaded_set(&s3)
        .await
        .wrap_err("Failed to fetch set of already uploaded bft blocks")?;

    loop {
        sleep(poll_frequency).await;
        info!("Checking for bft blocks to upload...");

        let to_upload = match find_unuploaded_blocks(&mut uploaded, &header_path, &body_path).await
        {
            Ok(x) => x,
            Err(e) => {
                error!(
                    ?e,
                    ?header_path,
                    ?body_path,
                    "Failed to scan directory for bft blocks, sleeping..."
                );
                continue;
            }
        };
        let num_to_upload = to_upload.len();
        if num_to_upload == 0 {
            continue;
        }
        info!(num_to_upload, "Found bft blocks to upload");

        let num_uploaded = parallel_upload_iter(&mut uploaded, s3.clone(), to_upload).await;

        if num_uploaded > 0 {
            info!(num_uploaded, num_to_upload, "Uploaded bft blocks");
        } else {
            info!(num_to_upload)
        }
        metrics.gauge("bft_blocks_uploaded", uploaded.len() as u64);
    }
}

async fn find_unuploaded_blocks(
    uploaded: &HashSet<OsString>,
    header_path: &Path,
    body_path: &Path,
) -> Result<Vec<PathBuf>> {
    let mut to_upload = Vec::new();
    let mut read_dir = tokio::fs::read_dir(header_path).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let meta = entry.metadata().await?;
        let fname = entry.file_name();
        if meta.is_file() && fname != "wal" && !uploaded.contains(&fname) {
            debug!(?fname, "Found file to upload");
            to_upload.push(entry.path());
        } else {
            debug!(?fname, "Found file, won't upload");
        }
    }

    if body_path != header_path {
        let mut read_dir = tokio::fs::read_dir(header_path).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let meta = entry.metadata().await?;
            let fname = entry.file_name();
            if meta.is_file() && fname != "wal" && !uploaded.contains(&fname) {
                debug!(?fname, "Found file to upload");
                to_upload.push(entry.path());
            } else {
                debug!(?fname, "Found file, won't upload");
            }
        }
    }

    Ok(to_upload)
}

async fn initialize_uploaded_set(s3: &impl BlobStore) -> Result<HashSet<OsString>> {
    Ok(s3
        .scan_prefix(BFT_BLOCK_PREFIX)
        .await?
        .into_iter()
        .map(|object| {
            let block_hash = object.trim_start_matches(BFT_BLOCK_PREFIX);
            OsString::from(block_hash)
        })
        .collect())
}
