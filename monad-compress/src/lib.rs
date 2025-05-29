use util::BoundedWriter;

pub mod brotli;
pub mod deflate;
pub mod lz4;
pub mod nop;
pub mod util;
pub mod zstd;

pub trait CompressionAlgo {
    type CompressError: std::error::Error;
    type DecompressError: std::error::Error;

    fn new(quality: u32, window_bits: u32, custom_dictionary: Vec<u8>) -> Self;

    fn compress(&self, input: &[u8], output: &mut BoundedWriter)
        -> Result<(), Self::CompressError>;
    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError>;
}
