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
    fmt::Debug,
    fs::{File, OpenOptions},
    io::Write,
    marker::PhantomData,
    path::PathBuf,
};

use bytes::Bytes;
use monad_types::Serializable;

use crate::WALError;

/// Header prepended to each event in the log
pub(crate) type EventHeaderType = u32;
pub(crate) const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

/// Maximum file size. 1GB.
pub(crate) const MAX_FILE_SIZE: usize = 1024 * 1024 * 1024;

/// Config for a write-ahead-log
#[derive(Clone)]
pub struct WALoggerConfig<M> {
    file_path: PathBuf,

    /// option for fsync after write. There is a cost to doing
    /// an fsync so its left configurable
    sync: bool,

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
            _marker: PhantomData,
        }
    }

    // this definition of the build function means that we can only have one type of message in this WAL
    // should enforce this in `push`/have WALogger parametrized by the message type
    pub fn build(self) -> Result<WALogger<M>, WALError> {
        let curr_file_index = 0;

        let mut new_file_path = self.file_path.clone().into_os_string();
        new_file_path.push(".");
        new_file_path.push(curr_file_index.to_string());

        let curr_file_handle = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(new_file_path)?;

        Ok(WALogger {
            _marker: PhantomData,
            file_path: self.file_path,
            curr_file_index,
            curr_file_handle,
            curr_file_offset: 0,
            sync: self.sync,
        })
    }
}

/// Write-ahead-logger that Serializes Events to an append-only-file
#[derive(Debug)]
pub struct WALogger<M> {
    _marker: PhantomData<M>,
    file_path: PathBuf,

    curr_file_index: u64,
    curr_file_handle: File,
    curr_file_offset: u64,

    sync: bool,
}

impl<M> WALogger<M>
where
    M: Serializable<Bytes> + Debug,
{
    pub fn push(&mut self, message: &M) -> Result<(), WALError> {
        let msg_buf = message.serialize();
        let buf = (msg_buf.len() as EventHeaderType).to_le_bytes().to_vec();
        let msg_len = (EVENT_HEADER_LEN + msg_buf.len()) as u64;

        // check file length before appending message
        let next_offset = self.curr_file_offset + msg_len;
        if next_offset > MAX_FILE_SIZE as u64 {
            // open new file
            // set length of current file and sync
            self.curr_file_handle.set_len(self.curr_file_offset)?;
            self.curr_file_handle.sync_all()?;

            // update file index and open new file
            self.curr_file_index += 1;
            let mut new_file_path = self.file_path.clone().into_os_string();
            new_file_path.push(".");
            new_file_path.push(self.curr_file_index.to_string());

            self.curr_file_handle = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(new_file_path)?;
            self.curr_file_offset = 0;
        }

        self.curr_file_handle.write_all(&buf)?;
        self.curr_file_handle.write_all(&msg_buf)?;
        self.curr_file_offset += msg_len;

        if self.sync {
            self.curr_file_handle.sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::array::TryFromSliceError;

    use bytes::Bytes;
    use monad_types::{Deserializable, Serializable};

    use crate::{
        reader::{WALReader, WALReaderConfig},
        wal::{WALogger, WALoggerConfig, EVENT_HEADER_LEN, MAX_FILE_SIZE},
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
        let mut log2_path = log1_path.into_os_string();
        log2_path.push(".0");
        let logger2_config = WALReaderConfig::new(log2_path.into());
        let mut logger2: WALReader<TestEvent> = logger2_config.build().unwrap();
        let mut state2 = VecState::default();
        while let Ok(event) = logger2.load_one() {
            state2.update(event);
        }
        assert_eq!(state1, state2);
    }

    #[ignore = "too long for 1GB MAX_FILE_SIZE"]
    #[test]
    fn rotate_wal() {
        // setup
        use std::fs::create_dir_all;

        use tempfile::tempdir;

        let num_files = 5;
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        let log_path = tmpdir.path().join("wal");
        let logger_config = WALoggerConfig::new(
            log_path, false, // sync
        );

        let payload_len = Serializable::<Bytes>::serialize(&TestEvent { data: 0 }).len();
        let serialized_event_len = EVENT_HEADER_LEN + payload_len;
        let num_events_per_file = MAX_FILE_SIZE / serialized_event_len;

        let num_total_events = num_files * num_events_per_file;
        let events = generate_test_events(num_total_events as u64);

        let mut logger: WALogger<TestEvent> = logger_config.build().unwrap();

        for (i, event) in events.into_iter().enumerate() {
            logger.push(&event).unwrap();
            assert!(logger.curr_file_index == (i / num_events_per_file) as u64)
        }
    }
}
