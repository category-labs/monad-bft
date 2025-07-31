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

use std::io::{self, Cursor, Write};

use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};

use crate::{util::BoundedWriter, CompressionAlgo};

pub const MAX_COMPRESSION_LEVEL: u32 = 9;

pub struct DeflateCompression {
    // compression level range 0..=9
    level: Compression,
}

impl CompressionAlgo for DeflateCompression {
    type CompressError = std::io::Error;
    type DecompressError = std::io::Error;

    fn new(quality: u32, _window_bits: u32, _custom_dictionary: Vec<u8>) -> Self {
        Self {
            level: Compression::new(quality.min(MAX_COMPRESSION_LEVEL)),
        }
    }

    fn compress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::CompressError> {
        let mut writer = DeflateEncoder::new(output, self.level);

        writer.write_all(input)?;
        writer.finish().map(|_| ())
    }

    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError> {
        let temp_buffer = vec![0x00_u8; 4096];
        let mut reader = DeflateDecoder::new_with_buf(Cursor::new(input), temp_buffer);
        io::copy(&mut reader, output)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Read};

    use bytes::Bytes;

    use super::*;
    #[test]
    fn test_lossless_compression() {
        let mut data = Vec::new();
        File::open("examples/txbatch.rlp")
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();

        let algo = DeflateCompression::new(6, 0, Vec::new());

        let mut compressed_writer = BoundedWriter::new(data.len() as u32);
        assert!(algo.compress(&data, &mut compressed_writer).is_ok());
        let compressed: Bytes = compressed_writer.into();
        assert!(compressed.len() < data.len());

        let mut decompressed_writer = BoundedWriter::new(data.len() as u32);
        assert!(algo
            .decompress(&compressed, &mut decompressed_writer)
            .is_ok());
        let decompressed: Bytes = decompressed_writer.into();
        assert_eq!(data, decompressed);
    }
}
