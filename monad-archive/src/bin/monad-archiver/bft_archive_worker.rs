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

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::SystemTime,
};

use eyre::Result;
use futures::stream::{self, StreamExt};
use monad_archive::{kvstore::WritePolicy, prelude::*};

const BFT_BLOCK_PREFIX: &str = "bft_block/";
const BFT_BLOCK_HEADER_EXTENSION: &str = ".header";
const BFT_BLOCK_BODY_EXTENSION: &str = ".body";

const BFT_BLOCK_HEADER_FILE_PATH: &str = "headers/";
const BFT_BLOCK_BODY_FILE_PATH: &str = "bodies/";

// Number of concurrent uploads
const UPLOAD_CONCURRENCY: usize = 10;

#[allow(clippy::doc_overindented_list_items)]
/// Worker that archives BFT consensus block files to durable storage.
/// Simple algorithm:
/// - Keep a set of known-in-S3 keys across iterations (starts empty).
/// - On each tick:
///   - List local headers/ and bodies/ into a single HashMap<key, path>.
///   - GC: remove any known-in-S3 keys that are no longer present locally.
///   - For each local (key, path):
///       - If key is in known set, skip.
///       - Else do a poor-man's exists: scan_prefix(key) and check for exact match.
///           - If exists: insert key into known set, skip.
///           - Else: read file and put(key, bytes). Do not insert into known set here; the next
///             iteration will observe existence via the exists check and add it then.
/// - Sleep and repeat.
pub async fn bft_block_archive_worker(
    store: KVStoreErased,
    ledger_path: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
    min_age: Option<Duration>,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_frequency);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Keys we have confirmed exist in S3 across ticks.
    let mut known_in_s3: HashSet<String> = HashSet::new();

    let mut headers_path = PathBuf::from(&ledger_path);
    headers_path.push(BFT_BLOCK_HEADER_FILE_PATH);
    let mut bodies_path = PathBuf::from(&ledger_path);
    bodies_path.push(BFT_BLOCK_BODY_FILE_PATH);

    loop {
        interval.tick().await;
        info!("Scanning for BFT blocks to upload...");

        let result = archive_bft_blocks(
            store.clone(),
            &mut known_in_s3,
            headers_path.clone(),
            bodies_path.clone(),
            &metrics,
            min_age,
        )
        .await;

        match result {
            Ok(()) => info!("Finished scanning for BFT blocks to upload"),
            Err(e) => error!(?e, "Failed to archive BFT blocks"),
        }
    }
}

async fn archive_bft_blocks(
    store: KVStoreErased,
    known_in_s3: &mut HashSet<String>,
    headers_path: PathBuf,
    bodies_path: PathBuf,
    metrics: &Metrics,
    min_age: Option<Duration>,
) -> Result<()> {
    // 1) Build a single map of local keys to paths.
    let mut local: HashMap<String, PathBuf> = HashMap::new();
    if let Err(e) = add_dir_to_local_map(
        &mut local,
        &headers_path,
        BFT_BLOCK_HEADER_EXTENSION,
        metrics,
        min_age,
    )
    .await
    {
        error!(?e, ?headers_path, "Failed to read headers directory");
    }
    if let Err(e) = add_dir_to_local_map(
        &mut local,
        &bodies_path,
        BFT_BLOCK_BODY_EXTENSION,
        metrics,
        min_age,
    )
    .await
    {
        error!(?e, ?bodies_path, "Failed to read bodies directory");
    }

    if local.is_empty() {
        debug!("No local BFT files found this tick");
        return Ok(());
    }

    info!(num_local = local.len(), "Local BFT files found");
    info!(known_in_s3 = known_in_s3.len(), "Known in S3");

    // 2) GC: drop known keys that no longer exist locally (memory hygiene only).
    known_in_s3.retain(|k| local.contains_key(k));

    info!(num_known_in_s3 = known_in_s3.len(), "Known in S3 after GC");

    // Remove keys that are already known to be in S3
    local.retain(|k, _| !known_in_s3.contains(k));

    info!(
        num_local_to_upload = local.len(),
        "Local BFT files to upload after removing known in S3"
    );

    // 3) Process files concurrently using streams
    stream::iter(local.into_iter())
        .map(|(key, path)| {
            let store = store.clone();
            let metrics = metrics.clone();
            async move {
                match process_single_file(store, &key, &path, &metrics).await {
                    Ok(x) => x,
                    Err(e) => {
                        error!(?e, ?key, ?path, "Failed to process BFT block");
                        metrics.inc_counter(MetricNames::BFT_BLOCK_FILES_FAILED_TO_PROCESS);
                        None
                    }
                }
            }
        })
        .buffer_unordered(UPLOAD_CONCURRENCY)
        .for_each(|x| {
            if let Some(key) = x {
                known_in_s3.insert(key);
            }
            futures::future::ready(())
        })
        .await;

    Ok(())
}

async fn process_single_file(
    store: KVStoreErased,
    key: &str,
    path: &PathBuf,
    metrics: &Metrics,
) -> Result<Option<String>> {
    // Check if file exists in S3
    if s3_exists_key(&store, key).await? {
        metrics.inc_counter(MetricNames::BFT_BLOCK_FILES_ALREADY_IN_S3);
        // Already exists, mark as known and skip
        return Ok(Some(key.to_string()));
    }

    // Need to upload; read file then put
    let bytes = tokio::fs::read(&path)
        .await
        .wrap_err("Failed to read local BFT file")?;

    store
        .put(&key, bytes, WritePolicy::NoClobber)
        .await
        .wrap_err("Failed to upload BFT block")?;
    metrics.inc_counter(MetricNames::BFT_BLOCK_FILES_UPLOADED);

    info!(key, ?path, "Uploaded BFT block");
    // Return the key so it can be added to the known set
    Ok(Some(key.to_string()))
}

/// Add all files from `dir` into `out` as S3 keys -> local paths.
async fn add_dir_to_local_map(
    out: &mut HashMap<String, PathBuf>,
    dir: &Path,
    ext: &str,
    metrics: &Metrics,
    min_age: Option<Duration>,
) -> Result<()> {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(x) => x,
        Err(e) => {
            // If the directory doesn't exist yet, treat as empty.
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(e).wrap_err("Failed to open directory");
        }
    };

    while let Some(entry) = rd.next_entry().await? {
        let meta = entry.metadata().await?;
        if !meta.is_file() {
            continue;
        }

        // Small freshness gate - skip files younger than min_age.
        if let Some(min_age) = min_age {
            let now = SystemTime::now();
            let too_new = match meta
                .modified()
                .wrap_err("Failed to get modified time")
                .and_then(|m| {
                    now.duration_since(m)
                        .wrap_err("Failed to get duration since modified time")
                }) {
                Ok(age) => age < min_age,
                Err(_) => false, // if clock skew or unsupported, don't skip
            };
            if too_new {
                debug!(path=?entry.path(), "Skipping fresh file (< min_age)");
                continue;
            }
        }

        let fname = entry.file_name();
        let fname_str = match fname.to_str() {
            Some(s) => s,
            None => {
                debug!(?fname, "Skipping non-utf8 filename");
                continue;
            }
        };
        let key = format!("{BFT_BLOCK_PREFIX}{fname_str}{ext}");
        out.insert(key, entry.path());
        metrics.inc_counter(MetricNames::BFT_BLOCK_FILES_DISCOVERED);
    }

    Ok(())
}

/// Poor-man's exists: list with the exact key as prefix and look for an exact match.
async fn s3_exists_key(store: &impl KVStore, key: &str) -> Result<bool> {
    let objs = store.scan_prefix(key).await?;
    Ok(objs.iter().any(|k| k == key))
}

#[cfg(test)]
mod tests {
    use monad_archive::kvstore::memory::MemoryStorage;
    use tempfile::tempdir;
    use tokio::fs;

    use super::*;

    #[tokio::test]
    async fn test_archive_bft_blocks_uploads_new_files() {
        // Setup
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let ledger_dir = tempdir().unwrap();
        let headers_path = ledger_dir.path().join(BFT_BLOCK_HEADER_FILE_PATH);
        let bodies_path = ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH);

        fs::create_dir_all(&headers_path).await.unwrap();
        fs::create_dir_all(&bodies_path).await.unwrap();

        // Create test files
        let test_content = b"test block content";
        fs::write(headers_path.join("block_001"), test_content)
            .await
            .unwrap();
        fs::write(bodies_path.join("block_001"), test_content)
            .await
            .unwrap();

        // Run archive
        archive_bft_blocks(
            store.clone(),
            &mut known_in_s3,
            headers_path.clone(),
            bodies_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        // Verify files were uploaded
        let header_key = format!("{BFT_BLOCK_PREFIX}block_001{BFT_BLOCK_HEADER_EXTENSION}");
        let body_key = format!("{BFT_BLOCK_PREFIX}block_001{BFT_BLOCK_BODY_EXTENSION}");

        assert_eq!(
            store
                .get(&header_key)
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            test_content.as_slice()
        );
        assert_eq!(
            store
                .get(&body_key)
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            test_content.as_slice()
        );
    }

    #[tokio::test]
    async fn test_archive_bft_blocks_skips_known_files() {
        // Setup
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let ledger_dir = tempdir().unwrap();
        let headers_path = ledger_dir.path().join(BFT_BLOCK_HEADER_FILE_PATH);
        let bodies_path = ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH);

        fs::create_dir_all(&headers_path).await.unwrap();
        fs::create_dir_all(&bodies_path).await.unwrap();

        // Create test file
        fs::write(headers_path.join("block_002"), b"content")
            .await
            .unwrap();

        // Mark as known
        let header_key = format!("{BFT_BLOCK_PREFIX}block_002{BFT_BLOCK_HEADER_EXTENSION}");
        known_in_s3.insert(header_key.clone());

        // Run archive
        archive_bft_blocks(
            store.clone(),
            &mut known_in_s3,
            headers_path.clone(),
            bodies_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        // Verify file was NOT uploaded (should not exist in store)
        assert!(store.get(&header_key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_archive_bft_blocks_discovers_existing_files() {
        // Setup
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let ledger_dir = tempdir().unwrap();
        let headers_path = ledger_dir.path().join(BFT_BLOCK_HEADER_FILE_PATH);
        let bodies_path = ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH);

        fs::create_dir_all(&headers_path).await.unwrap();
        fs::create_dir_all(&bodies_path).await.unwrap();

        // Pre-upload a file to S3
        let header_key = format!("{BFT_BLOCK_PREFIX}block_003{BFT_BLOCK_HEADER_EXTENSION}");
        store
            .put(
                &header_key,
                b"already uploaded".to_vec(),
                WritePolicy::NoClobber,
            )
            .await
            .unwrap();

        // Create matching local file
        fs::write(headers_path.join("block_003"), b"local content")
            .await
            .unwrap();

        // Run archive
        archive_bft_blocks(
            store.clone(),
            &mut known_in_s3,
            headers_path.clone(),
            bodies_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        // Verify file was discovered and added to known set
        assert!(known_in_s3.contains(&header_key));

        // Verify content was NOT overwritten
        assert_eq!(
            store
                .get(&header_key)
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            b"already uploaded".as_slice()
        );
    }

    #[tokio::test]
    async fn test_archive_bft_blocks_gc_removes_deleted_files() {
        // Setup
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let ledger_dir = tempdir().unwrap();
        let headers_path = ledger_dir.path().join(BFT_BLOCK_HEADER_FILE_PATH);
        let bodies_path = ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH);

        fs::create_dir_all(&headers_path).await.unwrap();
        fs::create_dir_all(&bodies_path).await.unwrap();

        // Add some keys to known set
        known_in_s3.insert(format!(
            "{BFT_BLOCK_PREFIX}deleted_file{BFT_BLOCK_HEADER_EXTENSION}"
        ));
        known_in_s3.insert(format!(
            "{BFT_BLOCK_PREFIX}existing_file{BFT_BLOCK_HEADER_EXTENSION}"
        ));

        // Create only one of the files locally
        fs::write(headers_path.join("existing_file"), b"content")
            .await
            .unwrap();

        // Run archive
        archive_bft_blocks(
            store.clone(),
            &mut known_in_s3,
            headers_path.clone(),
            bodies_path.clone(),
            &Metrics::none(),
            None,
        )
        .await
        .unwrap();

        // Verify GC removed the deleted file from known set
        assert!(!known_in_s3.contains(&format!(
            "{BFT_BLOCK_PREFIX}deleted_file{BFT_BLOCK_HEADER_EXTENSION}"
        )));
        assert!(known_in_s3.contains(&format!(
            "{BFT_BLOCK_PREFIX}existing_file{BFT_BLOCK_HEADER_EXTENSION}"
        )));
    }

    #[tokio::test]
    async fn test_add_dir_to_local_map_handles_missing_dir() {
        let mut local = HashMap::new();
        let missing_path = PathBuf::from("/nonexistent/path");

        // Should succeed with empty result
        add_dir_to_local_map(&mut local, &missing_path, ".test", &Metrics::none(), None)
            .await
            .unwrap();

        assert!(local.is_empty());
    }

    #[tokio::test]
    async fn test_add_dir_to_local_map_builds_correct_keys() {
        let dir = tempdir().unwrap();
        let mut local = HashMap::new();

        // Create test files
        fs::write(dir.path().join("file1"), b"content1")
            .await
            .unwrap();
        fs::write(dir.path().join("file2"), b"content2")
            .await
            .unwrap();

        // Create a subdirectory (should be ignored)
        fs::create_dir(dir.path().join("subdir")).await.unwrap();

        // Add to map
        add_dir_to_local_map(&mut local, dir.path(), ".ext", &Metrics::none(), None)
            .await
            .unwrap();

        // Verify correct keys were created
        assert_eq!(local.len(), 2);
        assert!(local.contains_key(&format!("{BFT_BLOCK_PREFIX}file1.ext")));
        assert!(local.contains_key(&format!("{BFT_BLOCK_PREFIX}file2.ext")));

        // Verify paths are correct
        assert_eq!(
            local[&format!("{BFT_BLOCK_PREFIX}file1.ext")],
            dir.path().join("file1")
        );
        assert_eq!(
            local[&format!("{BFT_BLOCK_PREFIX}file2.ext")],
            dir.path().join("file2")
        );
    }

    #[tokio::test]
    async fn test_s3_exists_key_found() {
        let store: KVStoreErased = MemoryStorage::new("test").into();

        // Add a key to the store
        let key = format!("{BFT_BLOCK_PREFIX}test_block{BFT_BLOCK_HEADER_EXTENSION}");
        store
            .put(&key, b"content".to_vec(), WritePolicy::NoClobber)
            .await
            .unwrap();

        // Check existence
        assert!(s3_exists_key(&store, &key).await.unwrap());
    }

    #[tokio::test]
    async fn test_s3_exists_key_not_found() {
        let store: KVStoreErased = MemoryStorage::new("test").into();

        // Add a different key
        store
            .put("other_key", b"content".to_vec(), WritePolicy::NoClobber)
            .await
            .unwrap();

        // Check non-existent key
        let key = format!("{BFT_BLOCK_PREFIX}missing{BFT_BLOCK_HEADER_EXTENSION}");
        assert!(!s3_exists_key(&store, &key).await.unwrap());
    }

    #[tokio::test]
    async fn test_s3_exists_key_partial_match_not_counted() {
        let store: KVStoreErased = MemoryStorage::new("test").into();

        // Add keys with partial matches
        store
            .put(
                "bft_block/test",
                b"content".to_vec(),
                WritePolicy::NoClobber,
            )
            .await
            .unwrap();
        store
            .put(
                "bft_block/test_longer",
                b"content".to_vec(),
                WritePolicy::NoClobber,
            )
            .await
            .unwrap();

        // Check for exact match only
        assert!(s3_exists_key(&store, "bft_block/test").await.unwrap());
        assert!(!s3_exists_key(&store, "bft_block/te").await.unwrap());
    }

    #[tokio::test]
    async fn test_bft_block_archive_worker_integration() {
        let ledger_dir = tempdir().unwrap();
        let headers_path = ledger_dir.path().join(BFT_BLOCK_HEADER_FILE_PATH);
        let bodies_path = ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH);

        fs::create_dir_all(&headers_path).await.unwrap();
        fs::create_dir_all(&bodies_path).await.unwrap();

        let store: KVStoreErased = MemoryStorage::new("test").into();
        let metrics = Metrics::none();

        // Create test files
        let test_content = b"test block data";
        fs::write(headers_path.join("block_100"), test_content)
            .await
            .unwrap();
        fs::write(bodies_path.join("block_100"), test_content)
            .await
            .unwrap();

        // Run worker with short poll interval
        let store_clone = store.clone();
        let ledger_path = ledger_dir.path().to_path_buf();
        let worker_handle = tokio::spawn(async move {
            let _ = bft_block_archive_worker(
                store_clone,
                ledger_path,
                Duration::from_millis(20),
                metrics,
                None,
            )
            .await;
        });

        // Wait for at least one iteration
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Cancel worker
        worker_handle.abort();

        // Verify files were uploaded
        let header_key = format!("{BFT_BLOCK_PREFIX}block_100{BFT_BLOCK_HEADER_EXTENSION}");
        let body_key = format!("{BFT_BLOCK_PREFIX}block_100{BFT_BLOCK_BODY_EXTENSION}");

        assert_eq!(
            store
                .get(&header_key)
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            test_content.as_slice()
        );
        assert_eq!(
            store
                .get(&body_key)
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            test_content.as_slice()
        );
    }
}
