use std::io::{Cursor, Read, Write};

use lz4::{Decoder, EncoderBuilder};

use crate::CompressionAlgo;

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

    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::CompressError> {
        let mut encoder = self.builder.build(output)?;
        encoder.write_all(input)?;
        encoder.finish().1
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::DecompressError> {
        let mut decoder = Decoder::new(Cursor::new(input))?;
        decoder.read_to_end(output)?;
        decoder.finish().1
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

        let algo = Lz4Compression::new(8, 0, Vec::new());

        let mut compressed = Vec::new();
        assert!(algo.compress(&data, &mut compressed).is_ok());
        assert!(compressed.len() < data.len());

        let mut decompressed = Vec::new();
        assert!(algo.decompress(&compressed, &mut decompressed).is_ok());
        assert_eq!(data, decompressed.as_slice());
    }
}
