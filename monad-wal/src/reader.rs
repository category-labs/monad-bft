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
    io::{BufReader, Read},
    marker::PhantomData,
    path::PathBuf,
};

use bytes::Bytes;
use monad_types::Deserializable;

use crate::{
    wal::{EventHeaderType, EVENT_HEADER_LEN},
    WALError,
};

const WAL_READ_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// Config for a write-ahead-log
#[derive(Clone)]
pub struct WALReaderConfig<M> {
    file_path: PathBuf,

    _marker: PhantomData<M>,
}

impl<M> WALReaderConfig<M>
where
    M: Deserializable<[u8]> + Debug,
{
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            _marker: PhantomData,
        }
    }

    pub fn build(self) -> Result<WALReader<M>, WALError> {
        let file = OpenOptions::new().read(true).open(self.file_path)?;

        Ok(WALReader {
            _marker: PhantomData,
            reader: BufReader::with_capacity(WAL_READ_BUFFER_SIZE, file),
        })
    }
}

#[derive(Debug)]
pub struct WALReader<M> {
    _marker: PhantomData<M>,
    reader: BufReader<File>,
}

impl<M> WALReader<M>
where
    M: Deserializable<[u8]> + Debug,
{
    pub fn load_one_raw(&mut self) -> Result<Bytes, WALError> {
        let mut len_buf = [0u8; EVENT_HEADER_LEN];
        self.reader.read_exact(&mut len_buf)?;
        let len = EventHeaderType::from_le_bytes(len_buf);
        let mut buf = vec![0u8; len as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf.into())
    }

    pub fn load_one(&mut self) -> Result<M, WALError> {
        let buf = self.load_one_raw()?;
        let msg = M::deserialize(&buf).map_err(|e| WALError::DeserError(Box::new(e)))?;
        Ok(msg)
    }
}
