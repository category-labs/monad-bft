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

use std::{collections::VecDeque, ffi::OsStr, fs::Metadata, path::Path, time::SystemTime};

use eyre::Result;
use futures::stream;
use monad_archive::{kvstore::WritePolicy, prelude::*};

// Number of concurrent uploads
const UPLOAD_CONCURRENCY: usize = 10;

struct DirCacheEntry {
    known_in_s3: HashSet<String>,
    last_seen_modified: Option<SystemTime>,
    last_hot: Instant,
}

impl DirCacheEntry {
    fn new() -> Self {
        Self {
            known_in_s3: HashSet::new(),
            last_seen_modified: None,
            last_hot: Instant::now(),
        }
    }

    fn mark_processed(&mut self, modified: Option<SystemTime>) {
        self.last_hot = Instant::now();
        self.last_seen_modified = modified.or(Some(SystemTime::now()));
    }
}

#[derive(Clone, Debug)]
struct DirPrefix(String);

impl DirPrefix {
    fn root(path: &Path) -> Result<Self> {
        Self::from_parent(None, path)
    }

    fn child(parent: &DirPrefix, path: &Path) -> Result<Self> {
        Self::from_parent(Some(parent), path)
    }

    fn from_parent(parent: Option<&DirPrefix>, path: &Path) -> Result<Self> {
        let base = parent.map(|p| p.as_str()).unwrap_or("");
        derive_prefix(base, path).map(Self)
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for DirPrefix {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

struct QueueItem {
    path: PathBuf,
    prefix: DirPrefix,
}

pub async fn recursive_dir_archiver(
    store: KVStoreErased,
    folder_path: PathBuf,
    poll_frequency: Duration,
    exclude_prefix: String,
    metrics: Metrics,
    min_age: Option<Duration>,
    hot_dir_ttl: Duration,
) -> Result<()> {
    let mut interval = tokio::time::interval(poll_frequency);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut dir_states = HashMap::<PathBuf, DirCacheEntry>::new();

    loop {
        interval.tick().await;
        info!("Scanning recursively for files to upload...");

        let result = archive_recursive_tick(
            store.clone(),
            folder_path.clone(),
            &metrics,
            min_age,
            exclude_prefix.as_str(),
            &mut dir_states,
        )
        .await;

        match result {
            Ok(()) => info!(?folder_path, "Finished scanning for files to upload"),
            Err(e) => error!(?folder_path, ?e, "Failed to archive files in directory"),
        }

        gc_dir_state(&mut dir_states, hot_dir_ttl);
    }
}

async fn archive_recursive_tick(
    store: KVStoreErased,
    root_folder: PathBuf,
    metrics: &Metrics,
    min_age: Option<Duration>,
    exclude_prefix: &str,
    dir_states: &mut HashMap<PathBuf, DirCacheEntry>,
) -> Result<()> {
    let root_prefix = DirPrefix::root(&root_folder)?;
    let mut queue = VecDeque::new();
    queue.push_back(QueueItem {
        path: root_folder,
        prefix: root_prefix,
    });

    while let Some(QueueItem {
        path: dir_path,
        prefix,
    }) = queue.pop_front()
    {
        let metadata = match tokio::fs::metadata(&dir_path).await {
            Ok(meta) => meta,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                dir_states.remove(&dir_path);
                continue;
            }
            Err(e) => {
                error!(?e, path=?dir_path, "Failed to read metadata for directory");
                continue;
            }
        };

        let dir_modified = metadata.modified().ok();
        let last_seen_modified = dir_states
            .get(&dir_path)
            .and_then(|entry| entry.last_seen_modified);

        if directory_changed(last_seen_modified, dir_modified) {
            let entry = dir_states
                .entry(dir_path.clone())
                .or_insert_with(DirCacheEntry::new);
            archive_dir(
                store.clone(),
                &mut entry.known_in_s3,
                dir_path.clone(),
                &prefix,
                metrics,
                min_age,
                exclude_prefix,
            )
            .await?;
            entry.mark_processed(dir_modified);
        }

        let mut rd = match tokio::fs::read_dir(&dir_path).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                dir_states.remove(&dir_path);
                continue;
            }
            Err(e) => {
                error!(?e, path=?dir_path, "Failed to open directory");
                continue;
            }
        };

        while let Some(entry) = rd.next_entry().await? {
            let meta = match entry.metadata().await {
                Ok(meta) => meta,
                Err(e) => {
                    debug!(?e, path=?entry.path(), "Skipping entry with unreadable metadata");
                    continue;
                }
            };
            if meta.is_dir() {
                let child_path = entry.path();
                let child_name = entry.file_name();
                if entry_has_excluded_prefix(child_name.as_os_str(), exclude_prefix) {
                    debug!(path=?child_path, exclude_prefix, "Skipping directory due to exclude prefix");
                    continue;
                }
                let child_prefix = match DirPrefix::child(&prefix, &child_path) {
                    Ok(p) => p,
                    Err(e) => {
                        debug!(?e, path=?child_path, "Skipping directory with invalid name");
                        continue;
                    }
                };
                queue.push_back(QueueItem {
                    path: child_path,
                    prefix: child_prefix,
                });
            }
        }
    }

    Ok(())
}

async fn archive_dir(
    store: KVStoreErased,
    known_in_s3: &mut HashSet<String>,
    folder_path: PathBuf,
    prefix: &DirPrefix,
    metrics: &Metrics,
    min_age: Option<Duration>,
    exclude_prefix: &str,
) -> Result<()> {
    let prefix_str = prefix.as_str();
    if prefix_str.is_empty() {
        bail!("Archive prefix must not be empty");
    }

    // Build local map of key -> file path (non-recursive)
    let mut local: HashMap<String, PathBuf> = HashMap::new();
    let mut rd = match tokio::fs::read_dir(&folder_path).await {
        Ok(x) => x,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                // Directory missing: treat as empty
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

        let fname = entry.file_name();
        if entry_has_excluded_prefix(fname.as_os_str(), exclude_prefix) {
            debug!(path=?entry.path(), exclude_prefix, "Skipping file due to exclude prefix");
            continue;
        }

        // Freshness filter
        if let Some(min_age) = min_age {
            if file_is_too_new(&meta, min_age) {
                debug!(path=?entry.path(), "Skipping fresh file (< min_age)");
                continue;
            }
        }

        let fname_str = fname.to_string_lossy();
        let key = format!("{prefix_str}/{fname_str}");
        local.insert(key, entry.path());
        metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_DISCOVERED);
    }

    if local.is_empty() {
        debug!(?folder_path, "No local files found this tick");
        return Ok(());
    }

    // GC: drop known keys not present locally
    known_in_s3.retain(|k| local.contains_key(k));
    // Remove keys that are already known to be in S3
    local.retain(|k, _| !known_in_s3.contains(k));

    // Process concurrently
    stream::iter(local.into_iter())
        .map(|(key, path)| {
            let store = store.clone();
            let metrics = metrics.clone();
            async move {
                match process_single_file(store, &key, &path, &metrics).await {
                    Ok(x) => x,
                    Err(e) => {
                        error!(?e, ?key, ?path, "Failed to process file for archive");
                        metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_FAILED_TO_PROCESS);
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
    if s3_exists_key(&store, key).await? {
        metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_ALREADY_IN_S3);
        return Ok(Some(key.to_string()));
    }

    let bytes = tokio::fs::read(&path)
        .await
        .wrap_err("Failed to read local file")?;
    store
        .put(&key, bytes, WritePolicy::NoClobber)
        .await
        .wrap_err("Failed to upload file to archive store")?;
    metrics.inc_counter(MetricNames::GENERIC_ARCHIVE_FILES_UPLOADED);
    info!(key, ?path, "Uploaded file to archive store");
    // Do NOT mark as known here; wait for next tick's exists check
    Ok(None)
}

async fn s3_exists_key(store: &impl KVStore, key: &str) -> Result<bool> {
    let objs = store.scan_prefix(key).await?;
    Ok(objs.iter().any(|k| k == key))
}

fn derive_prefix(base_prefix: &str, folder_path: &Path) -> Result<String> {
    let dir_os_name = folder_path
        .file_name()
        .ok_or_else(|| eyre!("Folder path must have a basename (last component)"))?;
    let dir_name = dir_os_name
        .to_str()
        .ok_or_else(|| eyre!("Folder name must be valid UTF-8"))?;
    if dir_name == "." || dir_name.is_empty() {
        bail!("Invalid directory name for key prefix: {dir_name}");
    }

    if base_prefix.is_empty() {
        Ok(dir_name.to_string())
    } else {
        Ok(format!("{base_prefix}/{dir_name}"))
    }
}

fn entry_has_excluded_prefix(name: &OsStr, exclude_prefix: &str) -> bool {
    !exclude_prefix.is_empty() && name.to_string_lossy().starts_with(exclude_prefix)
}

fn directory_changed(
    last_seen_modified: Option<SystemTime>,
    current_modified: Option<SystemTime>,
) -> bool {
    match last_seen_modified {
        Some(last) => current_modified.is_some_and(|current| current > last),
        None => true,
    }
}

fn file_is_too_new(meta: &Metadata, min_age: Duration) -> bool {
    let now = SystemTime::now();
    match meta.modified() {
        Ok(modified) => now
            .duration_since(modified)
            .map(|age| age < min_age)
            .unwrap_or(false),
        Err(_) => false,
    }
}

fn gc_dir_state(dir_states: &mut HashMap<PathBuf, DirCacheEntry>, ttl: Duration) {
    let now = Instant::now();
    dir_states.retain(|path, entry| {
        let keep = now.duration_since(entry.last_hot) <= ttl;
        if !keep {
            debug!(?path, "Dropping cold directory cache");
        }
        keep
    });
}

#[cfg(test)]
mod tests {
    use monad_archive::kvstore::memory::MemoryStorage;
    use tempfile::tempdir;
    use tokio::fs;

    use super::*;

    #[tokio::test]
    async fn test_archive_dir_uploads_new_files() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let base = tempdir().unwrap();
        let dir_path = base.path().join("my-dir");
        fs::create_dir_all(&dir_path).await.unwrap();
        let prefix = DirPrefix::root(&dir_path).unwrap();

        let test_content = b"hello";
        fs::write(dir_path.join("two.json"), test_content)
            .await
            .unwrap();
        fs::write(dir_path.join("happy.rs"), test_content)
            .await
            .unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &prefix,
            &Metrics::none(),
            None,
            ".",
        )
        .await
        .unwrap();

        let key1 = "my-dir/two.json";
        let key2 = "my-dir/happy.rs";

        assert_eq!(
            store.get(key1).await.unwrap().unwrap().to_vec().as_slice(),
            test_content.as_slice()
        );
        assert_eq!(
            store.get(key2).await.unwrap().unwrap().to_vec().as_slice(),
            test_content.as_slice()
        );

        // On first upload we should NOT add to known set yet
        assert!(known_in_s3.is_empty());
    }

    #[tokio::test]
    async fn test_archive_dir_discovers_existing_files() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let base = tempdir().unwrap();
        let dir_path = base.path().join("some-data");
        fs::create_dir_all(&dir_path).await.unwrap();
        let prefix = DirPrefix::root(&dir_path).unwrap();

        // Pre-upload a file
        let key = "some-data/item.bin";
        store
            .put(key, b"remote".to_vec(), WritePolicy::NoClobber)
            .await
            .unwrap();

        // Create local file with same name
        fs::write(dir_path.join("item.bin"), b"local")
            .await
            .unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &prefix,
            &Metrics::none(),
            None,
            ".",
        )
        .await
        .unwrap();

        assert!(known_in_s3.contains(key));
        // Ensure content not overwritten
        assert_eq!(
            store.get(key).await.unwrap().unwrap().to_vec().as_slice(),
            b"remote".as_slice()
        );
    }

    #[tokio::test]
    async fn test_archive_dir_gc_removes_deleted() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::from(["foo/bar".to_string(), "foo/baz".to_string()]);

        let base = tempdir().unwrap();
        let dir_path = base.path().join("foo");
        fs::create_dir_all(&dir_path).await.unwrap();
        let prefix = DirPrefix::root(&dir_path).unwrap();
        fs::write(dir_path.join("baz"), b"x").await.unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &prefix,
            &Metrics::none(),
            None,
            ".",
        )
        .await
        .unwrap();

        assert!(!known_in_s3.contains("foo/bar"));
        assert!(known_in_s3.contains("foo/baz"));
    }

    #[tokio::test]
    async fn test_archive_dir_skips_prefixed_files() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut known_in_s3 = HashSet::new();

        let base = tempdir().unwrap();
        let dir_path = base.path().join("pref");
        fs::create_dir_all(&dir_path).await.unwrap();
        let prefix = DirPrefix::root(&dir_path).unwrap();

        fs::write(dir_path.join(".secret"), b"hidden")
            .await
            .unwrap();
        fs::write(dir_path.join("visible.txt"), b"shown")
            .await
            .unwrap();

        archive_dir(
            store.clone(),
            &mut known_in_s3,
            dir_path.clone(),
            &prefix,
            &Metrics::none(),
            None,
            ".",
        )
        .await
        .unwrap();

        assert!(store.get("pref/.secret").await.unwrap().is_none());
        assert_eq!(
            store
                .get("pref/visible.txt")
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            b"shown"
        );
    }

    #[test]
    fn test_derive_prefix_errors_on_bad_dir_name() {
        // Root path has no basename
        let folder_path = PathBuf::from("/");
        let err = DirPrefix::root(&folder_path).unwrap_err();
        assert!(format!("{err}").contains("basename"));
    }

    #[tokio::test]
    async fn test_recursive_tick_archives_nested_directories() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut dir_states = HashMap::new();

        let base = tempdir().unwrap();
        let root = base.path().join("root");
        let inner = root.join("inner");
        let deeper = inner.join("deeper");
        fs::create_dir_all(&deeper).await.unwrap();

        fs::write(root.join("root.txt"), b"root").await.unwrap();
        fs::write(inner.join("inner.txt"), b"inner").await.unwrap();
        fs::write(deeper.join("deep.txt"), b"deep").await.unwrap();

        archive_recursive_tick(
            store.clone(),
            root.clone(),
            &Metrics::none(),
            None,
            ".",
            &mut dir_states,
        )
        .await
        .unwrap();

        assert_eq!(
            store
                .get("root/root.txt")
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            b"root"
        );
        assert_eq!(
            store
                .get("root/inner/inner.txt")
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            b"inner"
        );
        assert_eq!(
            store
                .get("root/inner/deeper/deep.txt")
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            b"deep"
        );

        assert!(dir_states.contains_key(&root));
        assert!(dir_states.contains_key(&inner));
        assert!(dir_states.contains_key(&deeper));
    }

    #[tokio::test]
    async fn test_recursive_tick_skips_prefixed_directories() {
        let store: KVStoreErased = MemoryStorage::new("test").into();
        let mut dir_states = HashMap::new();

        let base = tempdir().unwrap();
        let root = base.path().join("root");
        fs::create_dir_all(root.join(".git")).await.unwrap();
        fs::create_dir_all(root.join("data")).await.unwrap();

        fs::write(root.join(".git/config"), b"nope").await.unwrap();
        fs::write(root.join("data/good.txt"), b"ok").await.unwrap();

        archive_recursive_tick(
            store.clone(),
            root.clone(),
            &Metrics::none(),
            None,
            ".",
            &mut dir_states,
        )
        .await
        .unwrap();

        assert!(store.get("root/.git/config").await.unwrap().is_none());
        assert_eq!(
            store
                .get("root/data/good.txt")
                .await
                .unwrap()
                .unwrap()
                .to_vec()
                .as_slice(),
            b"ok"
        );
    }

    #[test]
    fn test_gc_drops_cold_directories() {
        let mut dir_states = HashMap::new();
        let mut entry = DirCacheEntry::new();
        entry.last_hot = Instant::now() - Duration::from_secs(120);
        dir_states.insert(PathBuf::from("/tmp/foo"), entry);

        gc_dir_state(&mut dir_states, Duration::from_secs(60));
        assert!(dir_states.is_empty());
    }
}
