use crate::prelude::*;

const BFT_BLOCK_PREFIX: &str = "bft_block/";
const BFT_BLOCK_HEADER_EXTENSION: &str = ".header";
const BFT_BLOCK_BODY_EXTENSION: &str = ".body";

const BFT_BLOCK_HEADER_FILE_PATH: &str = "headers/";
const BFT_BLOCK_BODY_FILE_PATH: &str = "bodies/";

enum BFTFilePath {
    Header(PathBuf),
    Body(PathBuf),
}

enum BFTFileName {
    Header(OsString),
    Body(OsString),
}

/// Worker that archives BFT consensus block files to durable storage.
/// Monitors a directory for new block files and uploads them.
///
/// # Arguments
/// * `store` - Storage backend to write blocks to
/// * `ledger_path` - Path to directory containing BFT block files
/// * `poll_frequency` - How often to check for new files
/// * `metrics` - Metrics collection interface
pub async fn bft_block_archive_worker(
    store: KVStoreErased,
    ledger_path: PathBuf,
    poll_frequency: Duration,
    metrics: Metrics,
) -> Result<()> {
    info!("Fetching set of already uploaded bft blocks...");
    let (mut uploaded_headers, mut uploaded_bodies) = initialize_uploaded_sets(&store)
        .await
        .wrap_err("Failed to fetch set of already uploaded bft blocks")?;

    let mut headers_path = PathBuf::from(&ledger_path);
    headers_path.push(BFT_BLOCK_HEADER_FILE_PATH);
    let mut bodies_path = PathBuf::from(&ledger_path);
    bodies_path.push(BFT_BLOCK_BODY_FILE_PATH);

    let mut interval = tokio::time::interval(poll_frequency);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        info!("Checking for bft blocks to upload...");

        let mut to_upload = Vec::new();

        info!("Checking for bft block headers to upload...");
        match find_unuploaded_files(&uploaded_headers, &headers_path).await {
            Ok(header_paths) => to_upload.extend(header_paths.into_iter().map(BFTFilePath::Header)),
            Err(e) => {
                error!(
                    ?e,
                    ?headers_path,
                    "Failed to scan directory for bft block headers"
                );
            }
        };

        info!("Checking for bft block bodies to upload...");
        match find_unuploaded_files(&uploaded_bodies, &bodies_path).await {
            Ok(body_paths) => to_upload.extend(body_paths.into_iter().map(BFTFilePath::Body)),
            Err(e) => {
                error!(
                    ?e,
                    ?bodies_path,
                    "Failed to scan directory for bft block bodies"
                );
            }
        };

        let num_to_upload = to_upload.len();
        if num_to_upload == 0 {
            continue;
        }
        info!(num_to_upload, "Found bft blocks to upload");

        let num_uploaded = parallel_upload_iter(
            &mut uploaded_headers,
            &mut uploaded_bodies,
            store.clone(),
            to_upload,
        )
        .await;

        if num_uploaded > 0 {
            info!(num_uploaded, num_to_upload, "Uploaded bft blocks");
        } else {
            info!(num_to_upload)
        }

        let total_uploaded = uploaded_headers.len() + uploaded_bodies.len();
        metrics.gauge(MetricNames::BFT_BLOCKS_UPLOADED, total_uploaded as u64);
    }
}

async fn parallel_upload_iter(
    uploaded_headers: &mut HashSet<OsString>,
    uploaded_bodies: &mut HashSet<OsString>,
    store: KVStoreErased,
    bft_file_paths: Vec<BFTFilePath>,
) -> usize {
    futures::stream::iter(bft_file_paths)
        .filter_map(|bft_file_path| {
            let s3 = store.clone();
            spawning_upload(s3, bft_file_path)
        })
        .buffer_unordered(20)
        .map(|bft_file_name_uploaded| {
            match bft_file_name_uploaded {
                BFTFileName::Header(header_name) => {
                    info!(?header_name, "Uploaded bft block header");
                    uploaded_headers.insert(header_name);
                }
                BFTFileName::Body(body_name) => {
                    info!(?body_name, "Uploaded bft block body");
                    uploaded_bodies.insert(body_name);
                }
            }
            futures::future::ready(())
        })
        .count()
        .await
}

async fn spawning_upload(
    store: KVStoreErased,
    bft_file_path: BFTFilePath,
) -> Option<futures::future::Ready<BFTFileName>> {
    tokio::spawn(upload(store, bft_file_path)).await.ok()?
}

async fn upload(
    store: KVStoreErased,
    bft_file_path: BFTFilePath,
) -> Option<futures::future::Ready<BFTFileName>> {
    let (file_path, extension, bft_file_name) = match bft_file_path {
        BFTFilePath::Header(header_file_path) => {
            let fname_os = header_file_path.file_name()?.to_os_string();
            (
                header_file_path,
                BFT_BLOCK_HEADER_EXTENSION,
                BFTFileName::Header(fname_os),
            )
        }
        BFTFilePath::Body(body_file_path) => {
            let fname_os = body_file_path.file_name()?.to_os_string();
            (
                body_file_path,
                BFT_BLOCK_BODY_EXTENSION,
                BFTFileName::Body(fname_os),
            )
        }
    };
    let fname = match &bft_file_name {
        BFTFileName::Header(header_name) => header_name.to_str()?,
        BFTFileName::Body(body_name) => body_name.to_str()?,
    };

    let data = match tokio::fs::read(&file_path).await {
        Ok(x) => x,
        Err(e) => {
            error!(?e, ?file_path, "Failed to read bft block file from folder");
            return None;
        }
    };

    match store
        .put(&format!("{BFT_BLOCK_PREFIX}{fname}{extension}"), data)
        .await
    {
        Ok(_) => Some(futures::future::ready(bft_file_name)),
        Err(e) => {
            error!(?e, fname, "Failed to archive bft block");
            None
        }
    }
}

async fn find_unuploaded_files(
    uploaded: &HashSet<OsString>,
    files_path: &Path,
) -> Result<Vec<PathBuf>> {
    let mut to_upload = Vec::new();
    let mut read_dir = tokio::fs::read_dir(files_path).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let meta = entry.metadata().await?;
        let fname = entry.file_name();
        if meta.is_file() && !uploaded.contains(&fname) {
            debug!(?fname, "Found file to upload");
            to_upload.push(entry.path());
        } else {
            debug!(?fname, "Found file, won't upload");
        }
    }

    Ok(to_upload)
}

async fn initialize_uploaded_sets(
    store: &impl KVStore,
) -> Result<(HashSet<OsString>, HashSet<OsString>)> {
    let mut uploaded_headers = HashSet::new();
    let mut uploaded_bodies = HashSet::new();

    let uploaded_objects = store.scan_prefix(BFT_BLOCK_PREFIX).await?;
    for object in uploaded_objects {
        let block_hash = object.trim_start_matches(BFT_BLOCK_PREFIX);
        if block_hash.ends_with(BFT_BLOCK_HEADER_EXTENSION) {
            uploaded_headers.insert(OsString::from(
                block_hash.trim_end_matches(BFT_BLOCK_HEADER_EXTENSION),
            ));
        } else {
            uploaded_bodies.insert(OsString::from(
                block_hash.trim_end_matches(BFT_BLOCK_BODY_EXTENSION),
            ));
        }
    }

    Ok((uploaded_headers, uploaded_bodies))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tempfile::tempdir;
    use tokio::fs;

    use super::*;
    use crate::kvstore::memory::MemoryStorage;

    #[tokio::test]
    async fn test_bft_block_archive_worker() {
        let ledger_dir = tempdir().unwrap();
        fs::create_dir(ledger_dir.path().join(BFT_BLOCK_HEADER_FILE_PATH))
            .await
            .unwrap();
        fs::create_dir(ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH))
            .await
            .unwrap();

        let store: KVStoreErased = MemoryStorage::new("test").into();
        let metrics = Metrics::none();

        // Create test files in both directories
        let header_files = ["block_header_1", "block_header_2"];
        let body_files = ["block_body_1", "block_body_2"];
        let test_content = hex::decode("656c6c6f20776f726c64").unwrap();

        // Write header files
        for fname in &header_files {
            fs::write(
                ledger_dir
                    .path()
                    .join(BFT_BLOCK_HEADER_FILE_PATH)
                    .join(fname),
                &test_content,
            )
            .await
            .unwrap();
        }

        // Write body files
        for fname in &body_files {
            fs::write(
                ledger_dir.path().join(BFT_BLOCK_BODY_FILE_PATH).join(fname),
                &test_content,
            )
            .await
            .unwrap();
        }

        // Run worker in background with short poll interval
        let store_clone = store.clone();
        let worker_handle = tokio::spawn(async move {
            let _ = bft_block_archive_worker(
                store_clone,
                ledger_dir.path().to_owned(),
                Duration::from_millis(40),
                metrics,
            )
            .await;
        });

        // Give worker time to process files
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Cancel worker
        worker_handle.abort();

        // Get uploaded files
        let (uploaded_headers, uploaded_bodies) = initialize_uploaded_sets(&store).await.unwrap();

        // Verify headers were uploaded
        let mut uploaded_headers = Vec::from_iter(uploaded_headers.into_iter());
        uploaded_headers.sort();

        let mut expected_headers = header_files.to_vec();
        expected_headers.sort();

        assert_eq!(&uploaded_headers, &expected_headers);

        // Verify content of uploaded headers
        for fname in &expected_headers {
            let key = format!("{BFT_BLOCK_PREFIX}{fname}{BFT_BLOCK_HEADER_EXTENSION}");
            let content = store.get(&key).await.unwrap().unwrap();
            assert_eq!(content, test_content);
        }

        // Verify bodies were uploaded
        let mut uploaded_bodies = Vec::from_iter(uploaded_bodies.into_iter());
        uploaded_bodies.sort();

        let mut expected_bodies = body_files.to_vec();
        expected_bodies.sort();

        assert_eq!(&uploaded_bodies, &expected_bodies);

        // Verify content of uploaded bodies
        for fname in &expected_bodies {
            let key = format!("{BFT_BLOCK_PREFIX}{fname}{BFT_BLOCK_BODY_EXTENSION}");
            let content = store.get(&key).await.unwrap().unwrap();
            assert_eq!(content, test_content);
        }
    }

    #[tokio::test]
    async fn test_find_unuploaded_blocks() {
        let dir = tempdir().unwrap();
        let uploaded = &["123456abc", "feab1282"];
        let on_disk = &["456", "789", "feab1282"];
        let expected_to_upload = &["456", "789"];
        for name in on_disk {
            fs::File::create(dir.path().join(name)).await.unwrap();
        }

        let output = find_unuploaded_files(
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
        let store: KVStoreErased = MemoryStorage::new("test").into();
        store
            .put(
                &format!("{BFT_BLOCK_PREFIX}123456abc{BFT_BLOCK_HEADER_EXTENSION}"),
                hex::decode("656c6c6ff20776f726").unwrap(),
            )
            .await
            .unwrap();

        store
            .put(
                &format!("{BFT_BLOCK_PREFIX}feab1282{BFT_BLOCK_BODY_EXTENSION}"),
                hex::decode("656c6c6f7c6fc626").unwrap(),
            )
            .await
            .unwrap();

        let (uploaded_headers, uploaded_bodies) = initialize_uploaded_sets(&store).await.unwrap();
        let uploaded_headers = Vec::from_iter(uploaded_headers.into_iter());
        assert_eq!(&uploaded_headers, &["123456abc"]);

        let uploaded_bodies = Vec::from_iter(uploaded_bodies.into_iter());
        assert_eq!(&uploaded_bodies, &["feab1282"]);
    }
}
