use std::io::{Cursor, Read, Write};

use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};

use crate::CompressionAlgo;

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

    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::CompressError> {
        let mut writer = DeflateEncoder::new(output, self.level);

        writer.write_all(input)?;
        writer.finish().map(|_| ())
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::DecompressError> {
        let temp_buffer = vec![0x00_u8; 4096];
        let mut reader = DeflateDecoder::new_with_buf(Cursor::new(input), temp_buffer);
        reader.read_to_end(output)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::*;
    #[test]
    fn test_lossless_compression() {
        let mut data = Vec::new();
        File::open("examples/txbatch.rlp")
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();

        let algo = DeflateCompression::new(6, 0, Vec::new());

        let mut compressed = Vec::new();
        assert!(algo.compress(&data, &mut compressed).is_ok());
        assert!(compressed.len() < data.len());

        let mut decompressed = Vec::new();
        assert!(algo.decompress(&compressed, &mut decompressed).is_ok());
        assert_eq!(data, decompressed.as_slice());
    }
}
