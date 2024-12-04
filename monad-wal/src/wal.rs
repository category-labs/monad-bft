use std::{fmt::Debug, io, marker::PhantomData, path::PathBuf};

use bytes::Bytes;
use monad_types::{Deserializable, Serializable};

use crate::{aof::AppendOnlyFile, PersistenceLogger, PersistenceLoggerBuilder, WALError};

/// Header prepended to each event in the log
type EventHeaderType = u32;
const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

/// Maximum file size. 1GB.
const MAX_FILE_SIZE: usize = 1024 * 1024 * 1024;

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
    M: Serializable<Bytes> + Deserializable<[u8]> + Debug,
{
    pub fn new(file_path: PathBuf, sync: bool) -> Self {
        Self {
            file_path,
            sync,
            _marker: PhantomData,
        }
    }

    // an iterator to the events in "self.file_path"
    // TODO self.file_path includes the file index if used for events()
    pub fn events(self) -> impl Iterator<Item = M> {
        let curr_file_handle =
            AppendOnlyFile::read_only(self.file_path.clone()).expect("failed to read file");
        let mut logger = WALogger {
            _marker: PhantomData,
            file_path: self.file_path,

            curr_file_index: 0,
            curr_file_handle,
            curr_file_offset: 0,
            sync: self.sync,
        };

        std::iter::repeat(()).map_while(move |()| match logger.load_one() {
            Ok(msg) => Some(msg),
            Err(WALError::IOError(err)) if err.kind() == io::ErrorKind::UnexpectedEof => None,
            Err(err) => panic!("error reading WAL: {:?}", err),
        })
    }
}

impl<M> PersistenceLoggerBuilder for WALoggerConfig<M>
where
    M: Serializable<Bytes> + Deserializable<[u8]> + Debug,
{
    type PersistenceLogger = WALogger<M>;

    // this definition of the build function means that we can only have one type of message in this WAL
    // should enforce this in `push`/have WALogger parametrized by the message type
    fn build(self) -> Result<Self::PersistenceLogger, WALError> {
        let curr_file_index = 0;

        let mut new_file_path = self.file_path.clone().into_os_string();
        new_file_path.push(".");
        new_file_path.push(curr_file_index.to_string());

        let curr_file_handle = AppendOnlyFile::new(new_file_path.into())?;

        Ok(Self::PersistenceLogger {
            _marker: PhantomData,
            file_path: self.file_path,
            curr_file_index,
            curr_file_handle,
            curr_file_offset: 0,
            sync: self.sync,
        })
    }
}

/// Write-ahead-logger that Serializes/Deserializes Events to an append-only-file
#[derive(Debug)]
pub struct WALogger<M> {
    _marker: PhantomData<M>,
    file_path: PathBuf,

    curr_file_index: u64,
    curr_file_handle: AppendOnlyFile,
    curr_file_offset: u64,

    sync: bool,
}

impl<M: Serializable<Bytes> + Deserializable<[u8]> + Debug> PersistenceLogger for WALogger<M> {
    type Event = M;

    fn push(&mut self, message: &Self::Event) -> Result<(), WALError> {
        self.push(message)
    }
}

impl<M> WALogger<M>
where
    M: Serializable<Bytes> + Deserializable<[u8]> + Debug,
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

            self.curr_file_handle = AppendOnlyFile::new(new_file_path.into())?;
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

    fn load_one(&mut self) -> Result<M, WALError> {
        let mut len_buf = [0u8; EVENT_HEADER_LEN];
        self.curr_file_handle.read_exact(&mut len_buf)?;
        let len = EventHeaderType::from_le_bytes(len_buf);
        let mut buf = vec![0u8; len as usize];
        self.curr_file_handle.read_exact(&mut buf)?;
        let offset = (len_buf.len() + buf.len()) as u64;
        self.curr_file_offset += offset;
        let msg = M::deserialize(&buf).map_err(|e| WALError::DeserError(Box::new(e)))?;
        Ok(msg)
    }
}

#[cfg(test)]
mod test {
    use std::array::TryFromSliceError;

    use bytes::Bytes;
    use monad_types::{Deserializable, Serializable};

    use crate::{
        wal::{WALogger, WALoggerConfig, EVENT_HEADER_LEN, MAX_FILE_SIZE},
        PersistenceLoggerBuilder,
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
        let logger2_config = WALoggerConfig::new(
            log2_path.into(),
            false, // sync
        );
        let mut state2 = VecState::default();
        for event in logger2_config.events() {
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
