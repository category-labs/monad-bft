use std::io;
use std::marker::PhantomData;
use std::path::PathBuf;

use monad_executor::{Deserializable, Serializable};

use crate::aof::AppendOnlyFile;

#[derive(Debug)]
pub enum WALError<M: Deserializable> {
    IOError(io::Error),
    DeserError(<M as Deserializable>::ReadError),
}

impl<M> From<io::Error> for WALError<M>
where
    M: Deserializable,
{
    fn from(value: io::Error) -> Self {
        Self::IOError(value)
    }
}

// the logger only accepts one type of message
// we can refactor M to Verified/Unverified type if we write to WAL after verifying the message
#[derive(Debug)]
pub struct WALogger<M: Serializable + Deserializable> {
    _marker: PhantomData<M>,
    file_handle: AppendOnlyFile,
}

impl<M> WALogger<M>
where
    M: Serializable + Deserializable,
{
    // this definition of the new function means that we can only have one type of message in this WAL
    // should enforce this in `push`/have WALogger parametrized by the message type
    pub fn new(file_path: PathBuf) -> Result<(Self, Vec<M>), WALError<M>> {
        // read the events to replay, then append-only
        let file = AppendOnlyFile::new(file_path)?;
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

    pub fn push_two_write(&mut self, message: &M) -> Result<(), WALError<M>> {
        let msg_buf = message.serialize();
        let len_buf = msg_buf.len().to_be_bytes().to_vec();

        self.file_handle.write_all(&len_buf)?;
        self.file_handle.write_all(&msg_buf)?;
        Ok(())
    }

    pub fn push(&mut self, message: &M) -> Result<(), WALError<M>> {
        let mut msg_buf = message.serialize();
        let mut buf = msg_buf.len().to_be_bytes().to_vec();
        buf.append(&mut msg_buf);
        self.file_handle.write_all(&buf)?;
        Ok(())
    }

    fn load_one(&mut self) -> Result<(M, u64), WALError<M>> {
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
