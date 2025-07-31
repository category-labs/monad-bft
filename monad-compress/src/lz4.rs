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

use lz4::{Decoder, EncoderBuilder};

use crate::{util::BoundedWriter, CompressionAlgo};

pub const MAX_COMPRESSION_LEVEL: u32 = 16;

pub struct Lz4Compression {
    // compression level range 0..=16
    builder: EncoderBuilder,
}

impl CompressionAlgo for Lz4Compression {
    type CompressError = std::io::Error;
    type DecompressError = std::io::Error;

    fn new(quality: u32, _window_bits: u32, _custom_dictionary: Vec<u8>) -> Self {
        Self {
            builder: EncoderBuilder::new()
                .level(quality.min(MAX_COMPRESSION_LEVEL))
                .to_owned(),
        }
    }

    fn compress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::CompressError> {
        let mut encoder = self.builder.build(output)?;
        encoder.write_all(input)?;
        encoder.finish().1
    }

    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError> {
        let mut decoder = Decoder::new(Cursor::new(input))?;
        io::copy(&mut decoder, output)?;
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

        let algo = Lz4Compression::new(8, 0, Vec::new());

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
