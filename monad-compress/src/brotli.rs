use std::io::{Cursor, Read, Write};

use brotli::{CompressorWriter, Decompressor};

use crate::CompressionAlgo;

pub struct BrotliCompression {
    // quality range 0..=11
    quality: u32,
    // window_bits range:
    // 10..=24 without large_window_size feature
    // 10..=30 with large_window_size feature
    window_bits: u32,
    // library default is 11/22 for Brotli
}

impl Default for BrotliCompression {
    fn default() -> Self {
        Self {
            quality: 11,
            window_bits: 22,
        }
    }
}

impl CompressionAlgo for BrotliCompression {
    type CompressError = std::io::Error;
    type DecompressError = std::io::Error;

    fn new(quality: u32, window_bits: u32) -> Self {
        Self {
            quality,
            window_bits,
        }
    }

    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::CompressError> {
        let mut writer = CompressorWriter::new(output, 4096, self.quality, self.window_bits);
        writer.write_all(input)?;
        writer.flush()
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::DecompressError> {
        let mut reader = Decompressor::new(Cursor::new(input), 4096);

        reader.read_to_end(output)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_lossless_compression() {
        let data = [0xfe_u8; 5000];
        let algo = BrotliCompression::new(11, 22);

        let mut compressed = Vec::new();
        assert!(algo.compress(&data, &mut compressed).is_ok());
        assert!(compressed.len() < data.len());

        let mut decompressed = Vec::new();
        assert!(algo.decompress(&compressed, &mut decompressed).is_ok());
        assert_eq!(data, decompressed.as_slice());
    }
}
