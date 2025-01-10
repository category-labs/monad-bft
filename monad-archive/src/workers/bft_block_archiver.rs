use crate::prelude::*;

const BFT_BLOCK_PREFIX: &str = "bft_block/";

pub async fn bft_block_archive_worker(
    store: BlobStoreErased,
    block_path: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
) -> Result<()> {
    info!("Fetching set of already uploaded bft blocks...");
    let mut uploaded = initialize_uploaded_set(&store)
        .await
        .wrap_err("Failed to fetch set of already uploaded bft blocks")?;

    loop {
        sleep(poll_frequency).await;
        info!("Checking for bft blocks to upload...");

        let to_upload = match find_unuploaded_blocks(&uploaded, &block_path).await {
            Ok(x) => x,
            Err(e) => {
                error!(
                    ?e,
                    ?block_path,
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

        let num_uploaded = parallel_upload_iter(&mut uploaded, store.clone(), to_upload).await;

        if num_uploaded > 0 {
            info!(num_uploaded, num_to_upload, "Uploaded bft blocks");
        } else {
            info!(num_to_upload)
        }
        metrics.gauge("bft_blocks_uploaded", uploaded.len() as u64);
    }
}

async fn parallel_upload_iter(
    uploaded: &mut HashSet<OsString>,
    store: BlobStoreErased,
    fnames: Vec<PathBuf>,
) -> usize {
    futures::stream::iter(fnames)
        .filter_map(|path| {
            let s3 = store.clone();
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
    store: BlobStoreErased,
    path: PathBuf,
) -> Option<futures::future::Ready<OsString>> {
    tokio::spawn(upload(store, path)).await.ok()?
}

async fn upload(store: BlobStoreErased, path: PathBuf) -> Option<futures::future::Ready<OsString>> {
    let fname_os = path.file_name()?.to_os_string();
    let fname = fname_os.to_str()?;

    let data = match tokio::fs::read(&path).await {
        Ok(x) => x,
        Err(e) => {
            error!(?e, ?path, "Failed to read bft block file from folder");
            return None;
        }
    };

    match store
        .upload(&format!("{BFT_BLOCK_PREFIX}{fname}"), data)
        .await
    {
        Ok(_) => Some(futures::future::ready(fname_os)),
        Err(e) => {
            error!(?e, fname, "Failed to archive bft block");
            None
        }
    }
}

async fn find_unuploaded_blocks(
    uploaded: &HashSet<OsString>,
    block_path: &Path,
) -> Result<Vec<PathBuf>> {
    let mut to_upload = Vec::new();
    let mut read_dir = tokio::fs::read_dir(block_path).await?;
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

    Ok(to_upload)
}

async fn initialize_uploaded_set(store: &impl BlobStore) -> Result<HashSet<OsString>> {
    Ok(store
        .scan_prefix(BFT_BLOCK_PREFIX)
        .await?
        .into_iter()
        .map(|object| {
            let block_hash = object.trim_start_matches(BFT_BLOCK_PREFIX);
            OsString::from(block_hash)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tempfile::tempdir;
    use tokio::fs;

    use super::*;
    use crate::storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_bft_block_archive_worker() {
        let block_dir = tempdir().unwrap();
        let store: BlobStoreErased = MemoryStorage::new("test").into();
        let metrics = Metrics::none();

        // Create test files in both directories
        let header_files = ["block1.header", "block2.header"];
        let body_files = ["block1.body", "block2.body"];
        let test_content = hex::decode("656c6c6f20776f726c64").unwrap();

        // Write header files
        for fname in &header_files {
            fs::write(block_dir.path().join(fname), &test_content)
                .await
                .unwrap();
        }

        // Write body files
        for fname in &body_files {
            fs::write(block_dir.path().join(fname), &test_content)
                .await
                .unwrap();
        }

        // Run worker in background with short poll interval
        let store_clone = store.clone();
        let worker_handle = tokio::spawn(async move {
            let _ = bft_block_archive_worker(
                store_clone,
                block_dir.path().to_owned(),
                Duration::from_millis(100),
                metrics,
            )
            .await;
        });

        // Give worker time to process files
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Cancel worker
        worker_handle.abort();

        // Verify files were uploaded
        let uploaded = initialize_uploaded_set(&store).await.unwrap();
        let mut uploaded = Vec::from_iter(uploaded.into_iter());
        uploaded.sort();

        // Combine expected files
        let mut expected_files = header_files.to_vec();
        expected_files.extend_from_slice(&body_files);
        expected_files.sort();

        assert_eq!(&uploaded, &expected_files);

        // Verify content of uploaded files from both directories
        for fname in &expected_files {
            let key = format!("{BFT_BLOCK_PREFIX}{fname}");
            let content = store.read(&key).await.unwrap();
            assert_eq!(content, test_content);
        }
    }

    #[tokio::test]
    async fn test_find_unuploaded_blocks() {
        let dir = tempdir().unwrap();
        let uploaded = &["123456abc.header", "feab1282.header"];
        let on_disk = &["456.header", "789.header", "feab1282.header"];
        let expected_to_upload = &["456.header", "789.header"];
        for name in on_disk {
            fs::File::create(dir.path().join(name)).await.unwrap();
        }

        let output = find_unuploaded_blocks(
            &HashSet::from_iter(uploaded.iter().map(|s| OsString::from_str(s).unwrap())),
            dir.path(),
        )
        .await
        .unwrap();

        let expected = expected_to_upload
            .iter()
            .map(|s| dir.path().join(s))
            .collect::<HashSet<PathBuf>>();

        for path in output {
            assert!(expected.contains(&path));
        }
    }

    #[tokio::test]
    async fn test_initialize_uploaded_set() {
        let store: BlobStoreErased = MemoryStorage::new("test").into();
        store
            .upload(
                &format!("{BFT_BLOCK_PREFIX}123456abc.header"),
                hex::decode("656c6c6ff20776f726").unwrap(),
            )
            .await
            .unwrap();

        store
            .upload(
                &format!("{BFT_BLOCK_PREFIX}feab1282.header"),
                hex::decode("656c6c6f7c6fc626").unwrap(),
            )
            .await
            .unwrap();

        let set = initialize_uploaded_set(&store).await.unwrap();
        let mut set = Vec::from_iter(set.into_iter());
        set.sort();
        assert_eq!(&set, &["123456abc.header", "feab1282.header"]);
    }
}
