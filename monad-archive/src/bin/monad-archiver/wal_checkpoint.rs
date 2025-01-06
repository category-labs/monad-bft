use std::{path::PathBuf, time::Duration};

use eyre::{Context, Result};

use monad_archive::{BlobStore, BlobStoreErased};
use tracing::error;

const WAL_PREFIX: &'static str = "wal/";

pub async fn wal_checkpoint_worker(
    store: BlobStoreErased,
    path: PathBuf,
    poll_frequency: Duration,
) {
    let mut interval = tokio::time::interval(poll_frequency);

    loop {
        interval.tick().await;

        if let Err(e) = read_and_upload(&store, &path).await {
            error!(path = ?&path, ?e);
        }
    }
}

async fn read_and_upload(store: &BlobStoreErased, path: &PathBuf) -> Result<()> {
    let buf = tokio::fs::read(&path)
        .await
        .wrap_err("Failed to read wal file")?;

    let key = format!("{WAL_PREFIX}{}", get_timestamp());
    store
        .upload(&key, buf)
        .await
        .wrap_err("Failed to upload wal to blob store")
}

fn get_timestamp() -> String {
    let now = chrono::Local::now();
    now.format("%d/%m/%Y_%H:%M:%S").to_string()
}
