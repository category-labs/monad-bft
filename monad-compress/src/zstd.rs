use zstd::{compression_level_range, DEFAULT_COMPRESSION_LEVEL};

use crate::{util::BoundedWriter, CompressionAlgo};

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

    fn compress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::CompressError> {
        zstd::stream::copy_encode(input, output, self.level)
    }

    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError> {
        zstd::stream::copy_decode(input, output)
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

        let algo = ZstdCompression::default();

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

    #[test]
    fn test_bounded_decompression() {
        let mut data = Vec::new();
        File::open("examples/txbatch.rlp")
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        let data_len = data.len() as u32;

        let algo = ZstdCompression::default();

        let mut compressed_writer = BoundedWriter::new(data_len);
        assert!(algo.compress(&data, &mut compressed_writer).is_ok());
        let compressed: Bytes = compressed_writer.into();
        assert!(compressed.len() < data.len());

        let mut decompressed_writer = BoundedWriter::new(data_len - 1);
        assert!(algo
            .decompress(&compressed, &mut decompressed_writer)
            .is_err());
    }
}
