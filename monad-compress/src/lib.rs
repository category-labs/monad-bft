pub mod brotli;
pub mod deflate;
pub mod lz4;
pub mod nop;

pub trait CompressionAlgo {
    type CompressError: std::error::Error;
    type DecompressError: std::error::Error;

    fn new(quality: u32, window_bits: u32) -> Self;

    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::CompressError>;
    fn decompress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), Self::DecompressError>;
}
