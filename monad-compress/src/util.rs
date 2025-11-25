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

use std::io::{Error, ErrorKind, Write};

use bytes::Bytes;

pub struct BoundedWriter {
    max_size: usize,
    vec: Vec<u8>,
}

impl BoundedWriter {
    pub fn new(bound: u32) -> Self {
        Self {
            max_size: bound as usize,
            vec: Vec::with_capacity(bound as usize),
        }
    }

    pub fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }
}

impl From<BoundedWriter> for Vec<u8> {
    fn from(value: BoundedWriter) -> Self {
        value.vec
    }
}

impl From<BoundedWriter> for Bytes {
    fn from(value: BoundedWriter) -> Self {
        value.vec.into()
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.vec.len() + buf.len() > self.max_size {
            return Err(Error::new(ErrorKind::UnexpectedEof, "write over max size"));
        }

        self.vec.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use super::BoundedWriter;

    #[test]
    fn test_write_multiple() {
        let mut bounded_writer = BoundedWriter::new(10);
        let buf = [1, 2, 3, 4, 5];

        assert_eq!(bounded_writer.vec.len(), 0);

        // first write succeeds
        assert!(bounded_writer.write(&buf).is_ok());
        assert_eq!(bounded_writer.vec.len(), 5);

        // second write succeeds
        assert!(bounded_writer.write(&buf).is_ok());
        assert_eq!(bounded_writer.vec.len(), 10);

        assert_eq!(bounded_writer.vec.as_slice()[..5], buf);
        assert_eq!(bounded_writer.vec.as_slice()[5..10], buf);
    }

    #[test]
    fn test_error_over_max_size() {
        let mut bounded_writer = BoundedWriter::new(10);
        let buf = [0; 10];
        assert!(bounded_writer.write(&buf).is_ok());
        assert!(bounded_writer.write(&[1]).is_err());

        let mut bounded_writer = BoundedWriter::new(10);
        let buf = [0; 11];
        assert!(bounded_writer.write(&buf).is_err());
    }
}
