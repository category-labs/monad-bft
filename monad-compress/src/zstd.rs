use zstd::{compression_level_range, DEFAULT_COMPRESSION_LEVEL};

use crate::CompressionAlgo;

pub const MAX_COMPRESSION_LEVEL: u32 = 15;

pub struct ZstdCompression {
    // compression level range 0..=15
    level: i32,
}

impl Default for ZstdCompression {
    fn default() -> Self {
        Self {
            level: DEFAULT_COMPRESSION_LEVEL,
        }
    }
}

impl CompressionAlgo for ZstdCompression {
    type CompressError = std::io::Error;
    type DecompressError = std::io::Error;

    fn new(quality: u32, _window_bits: u32, _custom_dictionary: Vec<u8>) -> Self {
        assert!(compression_level_range().contains(&(quality as i32)));

        Self {
            level: quality as i32,
        }
    }

    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::CompressError> {
        zstd::stream::copy_encode(input, output, self.level)
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::DecompressError> {
        zstd::stream::copy_decode(input, output)
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Read};

    use super::*;
    #[test]
    fn test_lossless_compression() {
        let mut data = Vec::new();
        File::open("examples/txbatch.rlp")
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();

        let algo = ZstdCompression::default();

        let mut compressed = Vec::new();
        assert!(algo.compress(&data, &mut compressed).is_ok());
        assert!(compressed.len() < data.len());

        let mut decompressed = Vec::new();
        assert!(algo.decompress(&compressed, &mut decompressed).is_ok());
        assert_eq!(data, decompressed.as_slice());
    }
}
