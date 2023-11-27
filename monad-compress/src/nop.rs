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

    fn new(_quality: u32, _window_bits: u32) -> Self {
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
