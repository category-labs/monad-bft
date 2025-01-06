use std::{path::PathBuf, time::Duration};

use eyre::{Context, Result};
use monad_archive::{BlobStore, BlobStoreErased};
use tracing::error;

pub async fn file_checkpoint_worker(
    store: BlobStoreErased,
    path: PathBuf,
    blob_prefix: String,
    poll_frequency: Duration,
) {
    let mut interval = tokio::time::interval(poll_frequency);

    loop {
        interval.tick().await;

        if let Err(e) = read_and_upload(&store, &path, &blob_prefix).await {
            error!(path = ?&path, ?e);
        }
    }
}

async fn read_and_upload(store: &BlobStoreErased, path: &PathBuf, blob_prefix: &str) -> Result<()> {
    let buf = tokio::fs::read(&path)
        .await
        .wrap_err_with(|| format!("Failed to read checkpoint_file file, path: {:?}", path))?;

    let key = format!("{blob_prefix}/{}", get_timestamp());
    store
        .upload(&key, buf)
        .await
        .wrap_err_with(|| format!("Failed to upload wal to blob store, key: {}", &key))
}

fn get_timestamp() -> String {
    let now = chrono::Local::now();
    now.format("%d/%m/%Y_%H:%M:%S").to_string()
}
