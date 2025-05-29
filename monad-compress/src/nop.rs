use std::io::Write;

use crate::{util::BoundedWriter, CompressionAlgo};

pub struct NopCompression;

#[derive(Debug)]
pub struct NopCompressionError(pub String);

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

    fn compress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::CompressError> {
        output
            .write_all(input)
            .map_err(|e| NopCompressionError(e.to_string()))
    }

    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError> {
        output
            .write_all(input)
            .map_err(|e| NopCompressionError(e.to_string()))
    }
}
