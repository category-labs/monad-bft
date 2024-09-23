use crate::CompressionAlgo;

pub struct NopCompression;

#[derive(Debug)]
pub struct NopCompressionError;

impl std::fmt::Display for NopCompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for NopCompressionError {}

impl CompressionAlgo for NopCompression {
    type CompressError = NopCompressionError;
    type DecompressError = NopCompressionError;

    fn new(_quality: u32, _window_bits: u32, _custom_dictionary: Vec<u8>) -> Self {
        Self
    }

    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::CompressError> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::DecompressError> {
        output.extend_from_slice(input);
        Ok(())
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

        let algo = NopCompression::new(6, 0, Vec::new());

        let mut compressed = Vec::new();
        assert!(algo.compress(&data, &mut compressed).is_ok());

        let mut decompressed = Vec::new();
        assert!(algo.decompress(&compressed, &mut decompressed).is_ok());
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_nop_compression_format() {
        assert_eq!(format!("{}", NopCompressionError), "NopCompressionError");
    }
}
