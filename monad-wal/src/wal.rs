use std::{error::Error, fmt::Debug, io, marker::PhantomData, path::PathBuf};

use monad_types::{Deserializable, Serializable};

use crate::{aof::AppendOnlyFile, PersistenceLogger};

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

#[derive(Clone)]
pub struct WALoggerConfig {
    pub file_path: PathBuf,
}

#[derive(Debug)]
pub struct WALogger<M: Serializable + Deserializable + Debug> {
    _marker: PhantomData<M>,
    file_handle: AppendOnlyFile,
}

impl<M: Serializable + Deserializable + Debug> PersistenceLogger for WALogger<M> {
    type Event = M;
    type Error = WALError<<M as Deserializable>::ReadError>;
    type Config = WALoggerConfig;

    // this definition of the new function means that we can only have one type of message in this WAL
    // should enforce this in `push`/have WALogger parametrized by the message type
    fn new(config: Self::Config) -> Result<(Self, Vec<Self::Event>), Self::Error> {
        // read the events to replay, then append-only
        let file = AppendOnlyFile::new(config.file_path)?;
        let mut logger = Self {
            _marker: PhantomData,
            file_handle: file,
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
    M: Serializable + Deserializable + Debug,
{
    pub fn push_two_write(
        &mut self,
        message: &M,
    ) -> Result<(), <Self as PersistenceLogger>::Error> {
        let msg_buf = message.serialize();
        let len_buf = msg_buf.len().to_be_bytes().to_vec();

        self.file_handle.write_all(&len_buf)?;
        self.file_handle.write_all(&msg_buf)?;
        self.file_handle.sync_all()?;
        Ok(())
    }

    pub fn push(&mut self, message: &M) -> Result<(), <Self as PersistenceLogger>::Error> {
        let mut msg_buf = message.serialize();
        let mut buf = msg_buf.len().to_be_bytes().to_vec();
        buf.append(&mut msg_buf);
        self.file_handle.write_all(&buf)?;
        self.file_handle.sync_all()?;
        Ok(())
    }

    fn load_one(&mut self) -> Result<(M, u64), <Self as PersistenceLogger>::Error> {
        let mut len_buf = [0u8; 8];
        self.file_handle.read_exact(&mut len_buf)?;
        let len = usize::from_be_bytes(len_buf);
        let mut buf = vec![0u8; len];
        self.file_handle.read_exact(&mut buf)?;
        let offset = (len_buf.len() + buf.len()) as u64;
        let msg = M::deserialize(&buf).map_err(WALError::DeserError)?;
        Ok((msg, offset))
    }
}
