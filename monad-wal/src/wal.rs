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
    collections::VecDeque,
    fmt::Debug,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::fd::AsRawFd;

use bytes::Bytes;
use monad_types::Serializable;
use tracing::{debug, info};

use crate::WALError;

pub use monad_wal_derive::{NotLogged, WALLogged};

pub trait WALLoggable {
    fn is_wal_logged(&self) -> bool;
}

pub trait WALLogged: WALLoggable {}

pub trait NotLogged: WALLoggable {}

/// Header prepended to each event in the log
pub(crate) type EventHeaderType = u32;
pub(crate) const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

/// Default chunk size. 128MB.
pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 128 * 1024 * 1024;
pub(crate) const DEFAULT_CHUNKS: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredChunk {
    pub path: PathBuf,
    pub generation: u64,
    pub used_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkState {
    path: PathBuf,
    generation: u64,
}

#[derive(Debug)]
struct ActiveChunkState {
    path: PathBuf,
    generation: u64,
    used_len: u64,
    needs_terminator: bool,
    handle: File,
}

impl ActiveChunkState {
    fn into_chunk_state(self) -> ChunkState {
        ChunkState {
            path: self.path,
            generation: self.generation,
        }
    }
}

/// Config for a write-ahead-log
#[derive(Clone)]
pub struct WALoggerConfig<M> {
    file_path: PathBuf,

    /// option for fsync after write. There is a cost to doing
    /// an fsync so its left configurable
    sync: bool,
    chunks: usize,
    chunk_size: u64,

    _marker: PhantomData<M>,
}

impl<M> WALoggerConfig<M>
where
    M: Serializable<Bytes> + Debug,
{
    pub fn new(file_path: PathBuf, sync: bool) -> Self {
        Self {
            file_path,
            sync,
            chunks: DEFAULT_CHUNKS,
            chunk_size: DEFAULT_CHUNK_SIZE,
            _marker: PhantomData,
        }
    }

    pub fn with_chunks(mut self, chunks: usize) -> Self {
        self.chunks = chunks;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    // this definition of the build function means that we can only have one type of message in this WAL
    // should enforce this in `push`/have WALogger parametrized by the message type
    pub fn build(self) -> Result<WALogger<M>, WALError> {
        if self.chunks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "wal chunks must be greater than zero",
            )
            .into());
        }

        let mut discovered = discover_chunks(&self.file_path)?;
        if discovered.len() > self.chunks {
            info!(
                path = %self.file_path.display(),
                configured_chunks = self.chunks,
                discovered_chunks = discovered.len(),
                "recreating wal with incompatible chunk count",
            );
            discovered = Vec::new();
            recreate_chunks(&self.file_path)?;
        }

        let discovered_count = discovered.len() as u64;
        let current = match discovered.pop() {
            Some(chunk) => {
                if chunk.used_len > self.chunk_size {
                    info!(
                        path = %self.file_path.display(),
                        chunk_path = %chunk.path.display(),
                        chunk_generation = chunk.generation,
                        chunk_used_len = chunk.used_len,
                        configured_chunk_size = self.chunk_size,
                        "recreating wal with incompatible chunk size",
                    );
                    recreate_chunks(&self.file_path)?;
                    discovered = Vec::new();
                    create_chunk(&self.file_path, 0, self.chunk_size)?
                } else {
                    let needs_terminator = chunk.generation + 1 > discovered_count;
                    reopen_chunk(chunk, needs_terminator)?
                }
            }
            None => create_chunk(&self.file_path, 0, self.chunk_size)?,
        };

        Ok(WALogger {
            _marker: PhantomData,
            file_path: self.file_path,
            current,
            rotated: discovered
                .into_iter()
                .map(|chunk| ChunkState {
                    path: chunk.path,
                    generation: chunk.generation,
                })
                .collect(),
            chunks: self.chunks,
            chunk_size: self.chunk_size,
            sync: self.sync,
        })
    }
}

/// Write-ahead-logger that Serializes Events to an append-only-file
#[derive(Debug)]
pub struct WALogger<M> {
    _marker: PhantomData<M>,
    file_path: PathBuf,
    current: ActiveChunkState,
    rotated: VecDeque<ChunkState>,
    chunks: usize,
    chunk_size: u64,
    sync: bool,
}

impl<M> WALogger<M>
where
    M: Serializable<Bytes> + Debug,
{
    pub fn push(&mut self, message: &M) -> Result<(), WALError> {
        let msg_buf = message.serialize();
        let msg_len = (EVENT_HEADER_LEN + msg_buf.len()) as u64;
        if msg_len > self.chunk_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serialized wal event exceeds chunk_size",
            )
            .into());
        }

        let next_offset = self.current.used_len + msg_len;
        if next_offset > self.chunk_size {
            self.rotate()?;
        }

        let len_buf = (msg_buf.len() as EventHeaderType).to_le_bytes();
        self.current.handle.write_all(&len_buf)?;
        self.current.handle.write_all(&msg_buf)?;
        self.current.used_len += msg_len;
        if self.sync || self.current.needs_terminator {
            write_chunk_terminator(
                &mut self.current.handle,
                self.current.used_len,
                self.chunk_size,
            )?;
        }

        if self.sync {
            self.current.handle.sync_all()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), WALError> {
        self.current.handle.sync_all()?;

        let next_generation = self.current.generation.saturating_add(1);
        let next_current = if self.chunks == 1 {
            reuse_chunk(
                &self.file_path,
                ChunkState {
                    path: self.current.path.clone(),
                    generation: self.current.generation,
                },
                next_generation,
                self.chunk_size,
            )?
        } else if self.rotated.len() + 1 < self.chunks {
            create_chunk(&self.file_path, next_generation, self.chunk_size)?
        } else {
            let reusable = self
                .rotated
                .pop_front()
                .expect("rotated must contain a chunk to reuse");
            reuse_chunk(&self.file_path, reusable, next_generation, self.chunk_size)?
        };
        let previous = std::mem::replace(&mut self.current, next_current);
        if self.chunks > 1 {
            self.rotated.push_back(previous.into_chunk_state());
        }

        Ok(())
    }
}

pub(crate) fn discover_chunks(file_path: &Path) -> Result<Vec<DiscoveredChunk>, WALError> {
    let Some(file_name) = file_path.file_name() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "wal path must include a file name",
        )
        .into());
    };
    let Some(file_name) = file_name.to_str() else {
        return Err(
            io::Error::new(io::ErrorKind::InvalidInput, "wal path must be valid utf-8").into(),
        );
    };
    let parent = file_path.parent().unwrap_or_else(|| Path::new("."));
    let chunk_prefix = format!("{file_name}.");

    let mut chunks = Vec::new();
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(chunks),
        Err(err) => return Err(err.into()),
    };

    for entry in entries {
        let entry = entry?;
        if let Some(chunk) = discover_chunk(entry, &chunk_prefix)? {
            chunks.push(chunk);
        }
    }

    chunks.sort_by_key(|chunk| chunk.generation);
    Ok(chunks)
}

fn discover_chunk(
    entry: fs::DirEntry,
    chunk_prefix: &str,
) -> Result<Option<DiscoveredChunk>, WALError> {
    let path = entry.path();
    let entry_type = entry.file_type()?;
    if !entry_type.is_file() {
        debug!(path = %path.display(), "skipping non-file wal entry");
        return Ok(None);
    }

    let entry_name = entry.file_name();
    let Some(entry_name) = entry_name.to_str() else {
        debug!(path = %path.display(), "skipping wal entry with non-utf8 name");
        return Ok(None);
    };
    let Some(generation) = entry_name
        .strip_prefix(chunk_prefix)
        .and_then(|suffix| suffix.parse::<u64>().ok())
    else {
        debug!(path = %path.display(), "skipping non-wal directory entry");
        return Ok(None);
    };

    Ok(Some(DiscoveredChunk {
        used_len: discover_chunk_used_len(&path)?,
        path,
        generation,
    }))
}

pub(crate) fn chunk_path(file_path: &Path, generation: u64) -> PathBuf {
    let mut chunk_path = file_path.as_os_str().to_os_string();
    chunk_path.push(".");
    chunk_path.push(generation.to_string());
    chunk_path.into()
}

pub(crate) fn discover_chunk_used_len(path: &Path) -> Result<u64, WALError> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let file_len = file.metadata()?.len();
    let mut used_len = 0u64;
    let mut len_buf = [0u8; EVENT_HEADER_LEN];

    while used_len + EVENT_HEADER_LEN as u64 <= file_len {
        file.read_exact(&mut len_buf)?;
        let event_len = EventHeaderType::from_le_bytes(len_buf) as u64;
        // Preallocated chunks are zero-filled past the written prefix.
        if event_len == 0 {
            break;
        }

        let next_offset = used_len + EVENT_HEADER_LEN as u64 + event_len;
        if next_offset > file_len {
            break;
        }

        file.seek(SeekFrom::Current(event_len as i64))?;
        used_len = next_offset;
    }

    Ok(used_len)
}

fn create_chunk(
    file_path: &Path,
    generation: u64,
    chunk_size: u64,
) -> Result<ActiveChunkState, WALError> {
    let path = chunk_path(file_path, generation);
    let handle = reset_chunk(&path, chunk_size)?;
    Ok(ActiveChunkState {
        path,
        generation,
        used_len: 0,
        needs_terminator: false,
        handle,
    })
}

fn reuse_chunk(
    file_path: &Path,
    chunk: ChunkState,
    generation: u64,
    chunk_size: u64,
) -> Result<ActiveChunkState, WALError> {
    let path = chunk_path(file_path, generation);
    fs::rename(&chunk.path, &path)?;
    let handle = reopen_reused_chunk(&path, chunk_size)?;
    Ok(ActiveChunkState {
        path,
        generation,
        used_len: 0,
        needs_terminator: true,
        handle,
    })
}

fn recreate_chunks(file_path: &Path) -> Result<(), WALError> {
    for chunk in discover_chunks(file_path)? {
        fs::remove_file(chunk.path)?;
    }
    Ok(())
}

fn reopen_chunk(
    chunk: DiscoveredChunk,
    needs_terminator: bool,
) -> Result<ActiveChunkState, WALError> {
    let mut handle = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&chunk.path)?;
    handle.seek(SeekFrom::Start(chunk.used_len))?;
    Ok(ActiveChunkState {
        path: chunk.path,
        generation: chunk.generation,
        used_len: chunk.used_len,
        needs_terminator,
        handle,
    })
}

fn reset_chunk(path: &Path, chunk_size: u64) -> Result<File, WALError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    preallocate_chunk(&file, chunk_size)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn reopen_reused_chunk(path: &Path, chunk_size: u64) -> Result<File, WALError> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    if file.metadata()?.len() < chunk_size {
        preallocate_chunk(&file, chunk_size)?;
    }
    file.seek(SeekFrom::Start(0))?;
    write_chunk_terminator(&mut file, 0, chunk_size)?;
    Ok(file)
}

fn write_chunk_terminator(file: &mut File, used_len: u64, chunk_size: u64) -> Result<(), WALError> {
    if used_len + EVENT_HEADER_LEN as u64 > chunk_size {
        return Ok(());
    }

    file.write_all(&0u32.to_le_bytes())?;
    file.seek(SeekFrom::Start(used_len))?;
    Ok(())
}

fn preallocate_chunk(file: &File, chunk_size: u64) -> Result<(), WALError> {
    #[cfg(unix)]
    {
        let chunk_size: libc::off_t = chunk_size.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "wal chunk_size does not fit into off_t",
            )
        })?;
        let err = unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, chunk_size) };
        if err == 0 {
            return Ok(());
        }
        if err != libc::EOPNOTSUPP && err != libc::ENOSYS {
            return Err(io::Error::from_raw_os_error(err).into());
        }
    }

    file.set_len(chunk_size)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::array::TryFromSliceError;

    use bytes::Bytes;
    use monad_types::{Deserializable, Serializable};

    use crate::{
        reader::{WALClient, WALClientConfig, WALReader, WALReaderConfig},
        wal::{chunk_path, WALogger, WALoggerConfig, EVENT_HEADER_LEN},
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestEvent {
        data: u64,
    }

    impl Serializable<Bytes> for TestEvent {
        fn serialize(&self) -> Bytes {
            self.data.to_be_bytes().to_vec().into()
        }
    }

    impl Deserializable<[u8]> for TestEvent {
        type ReadError = TryFromSliceError;

        fn deserialize(message: &[u8]) -> Result<Self, Self::ReadError> {
            let buf: [u8; 8] = message.try_into()?;
            Ok(Self {
                data: u64::from_be_bytes(buf),
            })
        }
    }

    #[derive(Debug, PartialEq, Eq, Default)]
    struct VecState {
        events: Vec<TestEvent>,
    }

    impl VecState {
        fn update(&mut self, event: TestEvent) {
            self.events.push(event);
        }
    }

    fn generate_test_events(num: u64) -> Vec<TestEvent> {
        (0..num).map(|i| TestEvent { data: i }).collect()
    }

    #[test]
    fn load_events() {
        // setup
        use std::fs::create_dir_all;

        use tempfile::tempdir;

        let input1 = generate_test_events(10);

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log1_path = tmpdir.path().join("wal");
        let logger1_config = WALoggerConfig::new(
            log1_path.clone(),
            false, // sync
        );

        let mut logger1: WALogger<TestEvent> = logger1_config.build().unwrap();
        let mut state1 = VecState::default();

        // driver loop (simulate executor by iterating events)
        for event in input1.into_iter() {
            logger1.push(&event).unwrap();

            state1.update(event);
        }

        // read events from the wal, assert equal
        let logger2_config = WALClientConfig::new(log1_path);
        let mut logger2: WALClient<TestEvent> = logger2_config.build().unwrap();
        let mut state2 = VecState::default();
        while let Ok(event) = logger2.load_one() {
            state2.update(event);
        }
        assert_eq!(state1, state2);
    }

    #[test]
    fn rotate_wal() {
        // setup
        use std::fs::{create_dir_all, read_dir};

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let serialized_event_len = EVENT_HEADER_LEN + payload_len;
        let num_events_per_file = 3;
        let chunk_size = (serialized_event_len * num_events_per_file) as u64;
        let logger_config = WALoggerConfig::new(log_path.clone(), false)
            .with_chunks(2)
            .with_chunk_size(chunk_size);

        let num_total_events = 8;
        let events = generate_test_events(num_total_events as u64);

        let mut logger: WALogger<TestEvent> = logger_config.build().unwrap();

        for event in events {
            logger.push(&event).unwrap();
        }

        assert_eq!(logger.current.generation, 2);
        assert_eq!(logger.rotated.len(), 1);
        assert_eq!(logger.rotated.front().unwrap().generation, 1);

        let wal_files = read_dir(tmpdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .count();
        assert_eq!(wal_files, 2);

        let mut reader: WALClient<TestEvent> = WALClientConfig::new(log_path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(
            events,
            generate_test_events(8)
                .into_iter()
                .skip(3)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn restart_discovers_current_chunk() {
        use std::fs::create_dir_all;

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let chunk_size = (EVENT_HEADER_LEN + payload_len) as u64 * 3;

        let logger_config = || {
            WALoggerConfig::new(log_path.clone(), false)
                .with_chunks(2)
                .with_chunk_size(chunk_size)
        };

        {
            let mut logger: WALogger<TestEvent> = logger_config().build().unwrap();
            for event in generate_test_events(5) {
                logger.push(&event).unwrap();
            }
        }

        {
            let mut logger: WALogger<TestEvent> = logger_config().build().unwrap();
            assert_eq!(logger.current.generation, 1);
            assert_eq!(
                logger.current.used_len,
                (EVENT_HEADER_LEN + payload_len) as u64 * 2
            );
            assert_eq!(logger.rotated.len(), 1);
            assert_eq!(logger.rotated.front().unwrap().generation, 0);

            logger.push(&TestEvent { data: 5 }).unwrap();
            logger.push(&TestEvent { data: 6 }).unwrap();
        }

        let mut reader: WALClient<TestEvent> = WALClientConfig::new(log_path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(
            events,
            generate_test_events(7)
                .into_iter()
                .skip(3)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn restart_recreates_when_chunk_count_shrinks() {
        use std::fs::create_dir_all;

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let chunk_size = (EVENT_HEADER_LEN + payload_len) as u64 * 3;

        {
            let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
                .with_chunks(3)
                .with_chunk_size(chunk_size)
                .build()
                .unwrap();
            for event in generate_test_events(8) {
                logger.push(&event).unwrap();
            }
        }

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
            .with_chunks(2)
            .with_chunk_size(chunk_size)
            .build()
            .unwrap();
        assert_eq!(logger.current.generation, 0);
        assert!(logger.rotated.is_empty());

        logger.push(&TestEvent { data: 100 }).unwrap();
        drop(logger);

        let mut reader: WALClient<TestEvent> = WALClientConfig::new(log_path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(events, vec![TestEvent { data: 100 }]);
    }

    #[test]
    fn restart_recreates_when_chunk_size_shrinks() {
        use std::fs::create_dir_all;

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let large_chunk_size = (EVENT_HEADER_LEN + payload_len) as u64 * 3;
        let small_chunk_size = (EVENT_HEADER_LEN + payload_len) as u64;

        {
            let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
                .with_chunks(2)
                .with_chunk_size(large_chunk_size)
                .build()
                .unwrap();
            for event in generate_test_events(5) {
                logger.push(&event).unwrap();
            }
        }

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
            .with_chunks(2)
            .with_chunk_size(small_chunk_size)
            .build()
            .unwrap();
        assert_eq!(logger.current.generation, 0);
        assert!(logger.rotated.is_empty());

        logger.push(&TestEvent { data: 100 }).unwrap();
        drop(logger);

        let mut reader: WALClient<TestEvent> = WALClientConfig::new(log_path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(events, vec![TestEvent { data: 100 }]);
    }

    #[test]
    fn reader_stops_at_preallocated_tail() {
        use std::fs::create_dir_all;

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let chunk_size = (EVENT_HEADER_LEN + payload_len) as u64 * 4;

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
            .with_chunk_size(chunk_size)
            .build()
            .unwrap();
        for event in generate_test_events(2) {
            logger.push(&event).unwrap();
        }
        drop(logger);

        let path = chunk_path(&log_path, 0);
        let mut reader: WALReader<TestEvent> = WALReaderConfig::new(path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(events, generate_test_events(2));
    }

    #[test]
    fn single_chunk_keeps_latest_events() {
        use std::fs::{create_dir_all, read_dir};

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let serialized_event_len = EVENT_HEADER_LEN + payload_len;
        let chunk_size = (serialized_event_len * 3) as u64;

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
            .with_chunks(1)
            .with_chunk_size(chunk_size)
            .build()
            .unwrap();
        for event in generate_test_events(5) {
            logger.push(&event).unwrap();
        }

        assert_eq!(logger.current.generation, 1);
        assert!(logger.rotated.is_empty());

        let wal_files = read_dir(tmpdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .count();
        assert_eq!(wal_files, 1);

        let mut reader: WALClient<TestEvent> = WALClientConfig::new(log_path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(
            events,
            generate_test_events(5)
                .into_iter()
                .skip(3)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn supports_more_than_two_chunks() {
        use std::fs::{create_dir_all, read_dir};

        use tempfile::tempdir;

        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let serialized_event_len = EVENT_HEADER_LEN + payload_len;
        let chunk_size = (serialized_event_len * 3) as u64;

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(log_path.clone(), false)
            .with_chunks(3)
            .with_chunk_size(chunk_size)
            .build()
            .unwrap();
        for event in generate_test_events(11) {
            logger.push(&event).unwrap();
        }

        assert_eq!(logger.current.generation, 3);
        assert_eq!(logger.rotated.len(), 2);
        assert_eq!(logger.rotated.front().unwrap().generation, 1);
        assert_eq!(logger.rotated.back().unwrap().generation, 2);

        let wal_files = read_dir(tmpdir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .count();
        assert_eq!(wal_files, 3);

        let mut reader: WALClient<TestEvent> = WALClientConfig::new(log_path).build().unwrap();
        let events: Vec<_> = std::iter::from_fn(|| reader.load_one().ok()).collect();
        assert_eq!(
            events,
            generate_test_events(11)
                .into_iter()
                .skip(3)
                .collect::<Vec<_>>()
        );
    }
}
