use std::{error::Error, fmt::Debug, io, marker::PhantomData, path::PathBuf};

use bytes::Bytes;
use monad_types::{Deserializable, Serializable};

use crate::{aof::AppendOnlyFile, PersistenceLogger};

/// Header prepended to each event in the log
type EventHeaderType = u32;
const EVENT_HEADER_LEN: usize = std::mem::size_of::<EventHeaderType>();

#[derive(Debug)]
pub enum WALError<E: Error> {
    IOError(io::Error),
    DeserError(E),
}

impl<E> From<io::Error> for WALError<E>
where
    E: Error,
{
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

impl<E> std::fmt::Display for WALError<E>
where
    E: Error,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Debug>::fmt(self, f)
    }
}

impl<E> std::error::Error for WALError<E> where E: Error {}

/// Config for a write-ahead-log
#[derive(Clone)]
pub struct WALoggerConfig {
    pub file_path: PathBuf,

    /// option for fsync after write. There is a cost to doing
    /// an fsync so its left configurable
    pub sync: bool,
}

/// Write-ahead-logger that Serializes/Deserializes Events to an append-only-file
#[derive(Debug)]
pub struct WALogger<M> {
    _marker: PhantomData<M>,
    file_handle: AppendOnlyFile,
    sync: bool,
}

impl<M: Serializable<Bytes> + Deserializable<[u8]> + Debug> PersistenceLogger for WALogger<M> {
    type Event = M;
    type Error = WALError<<M as Deserializable<[u8]>>::ReadError>;
    type Config = WALoggerConfig;

    // this definition of the new function means that we can only have one type of message in this WAL
    // should enforce this in `push`/have WALogger parametrized by the message type
    fn new(config: Self::Config) -> Result<(Self, Vec<Self::Event>), Self::Error> {
        // read the events to replay, then append-only
        let file = AppendOnlyFile::new(config.file_path)?;
        let mut logger = Self {
            _marker: PhantomData,
            file_handle: file,
            sync: config.sync,
        };
        let mut msg_vec = Vec::new();
        // load msgs from file one at a time
        let mut read_offset = 0;
        loop {
            match logger.load_one() {
                Ok((msg, offset)) => {
                    msg_vec.push(msg);
                    read_offset += offset;
                }
                Err(WALError::IOError(err)) => match err.kind() {
                    io::ErrorKind::UnexpectedEof => {
                        // truncate the file to end of last message
                        logger.file_handle.set_len(read_offset)?;
                        return Ok((logger, msg_vec));
                    }
                    _ => return Err(WALError::IOError(err)),
                },
                Err(err) => return Err(err),
            }
        }
    }

    fn push(&mut self, message: &Self::Event) -> Result<(), Self::Error> {
        self.push(message)
    }
}

impl<M> WALogger<M>
where
    M: Serializable<Bytes> + Deserializable<[u8]> + Debug,
{
    pub fn push(&mut self, message: &M) -> Result<(), <Self as PersistenceLogger>::Error> {
        let msg_buf = message.serialize();
        let buf = (msg_buf.len() as EventHeaderType).to_le_bytes().to_vec();
        self.file_handle.write_all(&buf)?;
        self.file_handle.write_all(&msg_buf)?;
        if self.sync {
            self.file_handle.sync_all()?;
        }
        Ok(())
    }

    fn load_one(&mut self) -> Result<(M, u64), <Self as PersistenceLogger>::Error> {
        let mut len_buf = [0u8; EVENT_HEADER_LEN];
        self.file_handle.read_exact(&mut len_buf)?;
        let len = EventHeaderType::from_le_bytes(len_buf);
        let mut buf = vec![0u8; len as usize];
        self.file_handle.read_exact(&mut buf)?;
        let offset = (len_buf.len() + buf.len()) as u64;
        let msg = M::deserialize(&buf).map_err(WALError::DeserError)?;
        Ok((msg, offset))
    }
}
