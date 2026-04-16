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
    io::{self, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use monad_types::Serializable;
use tracing::debug;

use crate::WALError;

pub trait WALLog {
    fn is_wal_logged(&self) -> bool;
}

/// Header prepended to each event in the log
pub(crate) type EventHeaderType = u32;
pub(crate) const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

pub(crate) const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_CHUNKS: u64 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredChunk {
    pub path: PathBuf,
    pub timestamp: u64,
    pub generation: u64,
}

#[derive(Debug)]
struct ActiveChunkState {
    path: PathBuf,
    generation: u64,
    size: u64,
    handle: File,
}

#[derive(Clone)]
pub struct WALoggerConfig<M> {
    dir_path: PathBuf,
    sync: bool,
    chunks: u64,
    chunk_size: u64,
    _marker: PhantomData<M>,
}

impl<M> WALoggerConfig<M>
where
    M: Serializable<Bytes> + Debug,
{
    pub fn new(dir_path: PathBuf, sync: bool) -> Self {
        Self {
            dir_path,
            sync,
            chunks: DEFAULT_CHUNKS,
            chunk_size: DEFAULT_CHUNK_SIZE,
            _marker: PhantomData,
        }
    }

    pub fn with_chunks(mut self, chunks: u64) -> Self {
        self.chunks = chunks;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: u64) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn build(self) -> Result<WALogger<M>, WALError> {
        if self.chunks == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "wal chunks must be greater than zero",
            )
            .into());
        }

        fs::create_dir_all(&self.dir_path)?;

        let discovered = discover_chunks(&self.dir_path)?;
        let (timestamp, current) = create_initial_chunk(&self.dir_path)?;

        let mut wal = WALogger {
            _marker: PhantomData,
            dir_path: self.dir_path,
            timestamp,
            generation: 0,
            current,
            rotated: discovered.into(),
            chunks: self.chunks,
            chunk_size: self.chunk_size,
            sync: self.sync,
        };
        wal.trim_oldest_chunks()?;

        Ok(wal)
    }
}

#[derive(Debug)]
pub struct WALogger<M> {
    _marker: PhantomData<M>,
    dir_path: PathBuf,
    timestamp: u64,
    generation: u64,
    current: ActiveChunkState,
    rotated: VecDeque<DiscoveredChunk>,
    chunks: u64,
    chunk_size: u64,
    sync: bool,
}

impl<M> WALogger<M>
where
    M: Serializable<Bytes> + Debug,
{
    pub fn push(&mut self, message: &M) -> Result<(), WALError> {
        let msg_buf = message.serialize();
        if msg_buf.len() > EventHeaderType::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serialized wal event exceeds u32 header size",
            )
            .into());
        }

        let msg_len = (EVENT_HEADER_LEN + msg_buf.len()) as u64;
        if msg_len > self.chunk_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "serialized wal event exceeds chunk_size",
            )
            .into());
        }

        if self.current.size + msg_len > self.chunk_size {
            self.rotate()?;
        }

        let len_buf = (msg_buf.len() as EventHeaderType).to_le_bytes();
        self.current.handle.write_all(&len_buf)?;
        self.current.handle.write_all(&msg_buf)?;
        self.current.size += msg_len;

        if self.sync {
            self.current.handle.sync_all()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> Result<(), WALError> {
        self.generation += 1;
        let previous = std::mem::replace(
            &mut self.current,
            create_chunk(&self.dir_path, self.timestamp, self.generation)?,
        );
        self.rotated.push_back(DiscoveredChunk {
            path: previous.path,
            timestamp: self.timestamp,
            generation: previous.generation,
        });
        self.trim_oldest_chunks()?;
        Ok(())
    }

    fn trim_oldest_chunks(&mut self) -> Result<(), WALError> {
        while self.rotated.len() as u64 + 1 > self.chunks {
            let Some(oldest) = self.rotated.pop_front() else {
                break;
            };
            fs::remove_file(oldest.path)?;
        }
        Ok(())
    }
}

pub fn discover_chunks(dir_path: &Path) -> Result<Vec<DiscoveredChunk>, WALError> {
    let mut chunks = discover_chunk_metadata(dir_path)?;
    chunks.sort_by_key(|chunk| (chunk.timestamp, chunk.generation));
    Ok(chunks)
}

fn discover_chunk_metadata(dir_path: &Path) -> Result<Vec<DiscoveredChunk>, WALError> {
    let mut chunks = Vec::new();
    let entries = match fs::read_dir(dir_path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(chunks),
        Err(err) => return Err(err.into()),
    };

    for entry in entries {
        let entry = entry?;
        if let Some(chunk) = discover_chunk(entry)? {
            chunks.push(chunk);
        }
    }

    Ok(chunks)
}

fn discover_chunk(entry: fs::DirEntry) -> Result<Option<DiscoveredChunk>, WALError> {
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

    let Some((timestamp, generation)) = entry_name
        .strip_prefix("wal_")
        .and_then(|suffix| suffix.split_once('.'))
        .and_then(|(timestamp, generation)| {
            Some((
                timestamp.parse::<u64>().ok()?,
                generation.parse::<u64>().ok()?,
            ))
        })
    else {
        debug!(path = %path.display(), "skipping non-wal directory entry");
        return Ok(None);
    };

    Ok(Some(DiscoveredChunk {
        path,
        timestamp,
        generation,
    }))
}

pub(crate) fn chunk_path(dir_path: &Path, timestamp: u64, generation: u64) -> PathBuf {
    dir_path.join(format!("wal_{timestamp}.{generation}"))
}

fn current_timestamp() -> Result<u64, WALError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        .as_millis()
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "wal timestamp overflow").into())
}

fn create_initial_chunk(dir_path: &Path) -> Result<(u64, ActiveChunkState), WALError> {
    loop {
        let timestamp = current_timestamp()?;
        match create_chunk(dir_path, timestamp, 0) {
            Ok(current) => return Ok((timestamp, current)),
            Err(WALError::IOError(err)) if err.kind() == io::ErrorKind::AlreadyExists => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(err) => return Err(err),
        }
    }
}

fn create_chunk(
    dir_path: &Path,
    timestamp: u64,
    generation: u64,
) -> Result<ActiveChunkState, WALError> {
    let path = chunk_path(dir_path, timestamp, generation);
    let handle = OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .open(&path)?;
    Ok(ActiveChunkState {
        path,
        generation,
        size: 0,
        handle,
    })
}

#[cfg(test)]
mod test {
    use std::{
        array::TryFromSliceError,
        fs::{create_dir_all, write},
        path::PathBuf,
    };

    use bytes::Bytes;
    use monad_types::{Deserializable, Serializable};
    use tempfile::tempdir;

    use crate::{
        reader::WALReader,
        wal::{
            chunk_path, discover_chunks, DiscoveredChunk, EventHeaderType, WALogger,
            WALoggerConfig, EVENT_HEADER_LEN,
        },
        WALError,
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

    fn write_chunk(dir_path: &PathBuf, timestamp: u64, generation: u64, events: &[TestEvent]) {
        create_dir_all(dir_path).unwrap();
        let mut buf = Vec::new();
        for event in events {
            let payload = Serializable::<Bytes>::serialize(event);
            buf.extend_from_slice(&(payload.len() as EventHeaderType).to_le_bytes());
            buf.extend_from_slice(&payload);
        }
        write(chunk_path(dir_path, timestamp, generation), buf).unwrap();
    }

    fn decode_all(dir_path: PathBuf) -> Vec<u64> {
        let mut reader: WALReader<TestEvent> = WALReader::new(dir_path).unwrap();
        std::iter::from_fn(|| reader.load_one().unwrap())
            .map(|event| event.data)
            .collect()
    }

    #[test]
    fn reads_events_across_multiple_chunks() {
        let tmpdir = tempdir().unwrap();
        let wal_dir = tmpdir.path().join("wal");
        let chunk_size = (EVENT_HEADER_LEN
            + Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len())
            as u64
            * 2;

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(wal_dir.clone(), false)
            .with_chunks(4)
            .with_chunk_size(chunk_size)
            .build()
            .unwrap();
        for data in 0..6 {
            logger.push(&TestEvent { data }).unwrap();
        }
        drop(logger);

        assert_eq!(decode_all(wal_dir), (0..6).collect::<Vec<_>>());
    }

    #[test]
    fn preserves_rotation_order_across_restarts() {
        let tmpdir = tempdir().unwrap();
        let wal_dir = tmpdir.path().join("wal");
        let chunk_size = (EVENT_HEADER_LEN
            + Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len())
            as u64
            * 2;

        write_chunk(
            &wal_dir,
            1,
            0,
            &[TestEvent { data: 0 }, TestEvent { data: 1 }],
        );
        write_chunk(
            &wal_dir,
            1,
            1,
            &[TestEvent { data: 2 }, TestEvent { data: 3 }],
        );

        {
            let mut logger: WALogger<TestEvent> = WALoggerConfig::new(wal_dir.clone(), false)
                .with_chunks(4)
                .with_chunk_size(chunk_size)
                .build()
                .unwrap();
            for data in 4..8 {
                logger.push(&TestEvent { data }).unwrap();
            }
        }

        let discovered = discover_chunks(&wal_dir).unwrap();
        assert_eq!(discovered.len(), 4);
        let chunks = discovered
            .iter()
            .map(|chunk| (chunk.timestamp, chunk.generation))
            .collect::<Vec<_>>();
        let current_timestamp = chunks[2].0;
        assert_eq!(
            chunks,
            vec![
                (1, 0),
                (1, 1),
                (current_timestamp, 0),
                (current_timestamp, 1),
            ]
        );
        assert_eq!(decode_all(wal_dir), (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn never_retains_more_chunks_than_configured() {
        let tmpdir = tempdir().unwrap();
        let wal_dir = tmpdir.path().join("wal");
        let chunk_size = (EVENT_HEADER_LEN
            + Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len())
            as u64
            * 2;

        let mut logger: WALogger<TestEvent> = WALoggerConfig::new(wal_dir.clone(), false)
            .with_chunks(2)
            .with_chunk_size(chunk_size)
            .build()
            .unwrap();
        for data in 0..10 {
            logger.push(&TestEvent { data }).unwrap();
            assert!(discover_chunks(&wal_dir).unwrap().len() <= 2);
        }
        drop(logger);

        assert_eq!(discover_chunks(&wal_dir).unwrap().len(), 2);
        assert_eq!(decode_all(wal_dir), vec![6, 7, 8, 9]);
    }

    #[test]
    fn reader_reports_incomplete_chunks() {
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        write_chunk(&log_path, 1, 0, &[TestEvent { data: 1 }]);
        write(chunk_path(&log_path, 1, 1), 8_u32.to_le_bytes()).unwrap();
        write_chunk(&log_path, 1, 2, &[TestEvent { data: 2 }]);

        let mut reader: WALReader<TestEvent> = WALReader::new(log_path).unwrap();
        assert_eq!(reader.load_one().unwrap(), Some(TestEvent { data: 1 }));
        let err = reader.load_one().unwrap_err();
        assert!(matches!(
            err,
            WALError::IOError(err) if err.kind() == std::io::ErrorKind::UnexpectedEof
        ));
    }

    #[test]
    fn reader_reports_partial_header() {
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");

        write_chunk(&log_path, 1, 0, &[TestEvent { data: 1 }]);
        write(chunk_path(&log_path, 1, 1), [0_u8; EVENT_HEADER_LEN - 1]).unwrap();
        write_chunk(&log_path, 1, 2, &[TestEvent { data: 2 }]);

        let mut reader: WALReader<TestEvent> = WALReader::new(log_path).unwrap();
        assert_eq!(reader.load_one().unwrap(), Some(TestEvent { data: 1 }));
        let err = reader.load_one().unwrap_err();
        assert!(matches!(
            err,
            WALError::IOError(err) if err.kind() == std::io::ErrorKind::UnexpectedEof
        ));
    }

    #[test]
    fn discover_chunks_skips_non_matching_entries() {
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");
        create_dir_all(&log_path).unwrap();

        write(log_path.join("not_a_wal"), []).unwrap();
        write(log_path.join("wal_abc.0"), []).unwrap();
        write(log_path.join("wal_1.abc"), []).unwrap();
        write(log_path.join("wal_1"), []).unwrap();
        std::fs::create_dir(log_path.join("wal_1.0")).unwrap();
        write(chunk_path(&log_path, 2, 3), []).unwrap();

        assert_eq!(
            discover_chunks(&log_path).unwrap(),
            vec![DiscoveredChunk {
                path: chunk_path(&log_path, 2, 3),
                timestamp: 2,
                generation: 3,
            }]
        );
    }
}
