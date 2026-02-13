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

use std::path::PathBuf;

use eyre::Result;
use monad_archive::{kvstore::WritePolicy, prelude::*};

/// Worker that periodically saves files to durable storage.
/// Reads files from disk and uploads them with timestamps.
///
/// # Arguments
/// * `store` - Storage backend to write checkpoints to
/// * `path` - Path to file to checkpoint
/// * `blob_prefix` - Prefix for checkpoint files in storage
/// * `poll_frequency` - How often to save checkpoints
pub async fn file_checkpoint_worker(
    store: KVStoreErased,
    path: PathBuf,
    blob_prefix: String,
    poll_frequency: Duration,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_frequency);

    loop {
        interval.tick().await;

        if let Err(e) = read_and_upload(&store, &path, &blob_prefix).await {
            error!(path = ?&path, ?e);
        }
    }
}

async fn read_and_upload(store: &KVStoreErased, path: &PathBuf, blob_prefix: &str) -> Result<()> {
    let buf = tokio::fs::read(&path)
        .await
        .wrap_err_with(|| format!("Failed to read checkpoint_file file, path: {:?}", path))?;

    let key = format!("{blob_prefix}/{}", get_timestamp());
    store
        .put(&key, buf, WritePolicy::AllowOverwrite)
        .await
        .wrap_err_with(|| format!("Failed to upload wal to blob store, key: {}", &key))?;
    Ok(())
}

fn get_timestamp() -> String {
    let now = chrono::Local::now();
    now.format("%d/%m/%Y_%H:%M:%S").to_string()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use monad_archive::kvstore::memory::MemoryStorage;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_read_and_upload() -> Result<()> {
        // Create temp file with test content
        let mut temp_file = NamedTempFile::new()?;
        let test_content = b"test checkpoint data";
        temp_file.write_all(test_content)?;

        let store: KVStoreErased = MemoryStorage::new("test").into();
        let blob_prefix = "test_checkpoints";

        // Perform upload
        read_and_upload(&store, &temp_file.path().to_path_buf(), blob_prefix).await?;

        // Verify upload - scan prefix and check content
        let uploaded_files = store.scan_prefix(blob_prefix).await?;
        assert_eq!(uploaded_files.len(), 1);

        let content = store.get(&uploaded_files[0]).await?.unwrap();
        assert_eq!(content.to_vec().as_slice(), test_content);

        Ok(())
    }

    #[tokio::test]
    async fn test_file_checkpoint_worker() {
        let temp_file = NamedTempFile::new().unwrap();
        let test_content = b"test checkpoint data";
        std::fs::write(&temp_file, test_content).unwrap();

        let store: KVStoreErased = MemoryStorage::new("test").into();
        let store_clone = store.clone();
        let path = temp_file.path().to_path_buf();

        // Start worker with very short poll interval
        let worker_handle = tokio::spawn(async move {
            file_checkpoint_worker(
                store_clone,
                path,
                "test_checkpoints".to_string(),
                Duration::from_millis(100),
            )
            .await
        });

        // Wait for at least one upload
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify upload occurred
        let uploaded = store.scan_prefix("test_checkpoints").await.unwrap();
        assert!(!uploaded.is_empty());

        let content = store.get(&uploaded[0]).await.unwrap().unwrap();
        assert_eq!(content.to_vec().as_slice(), test_content);

        worker_handle.abort();
    }

    #[tokio::test]
    async fn test_read_and_upload_nonexistent_file() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let nonexistent_path = PathBuf::from("/nonexistent/file");

        let result = read_and_upload(&store, &nonexistent_path, "test_prefix").await;
        assert!(result.is_err());
    }
}
