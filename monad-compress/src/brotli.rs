use std::io::{Cursor, ErrorKind};

use brotli::{
    enc::BrotliEncoderInitParams, writer::StandardAlloc, Allocator,
    BrotliCompressCustomIoCustomDict, BrotliDecompressCustomIoCustomDict, IoReaderWrapper,
    IoWriterWrapper, SliceWrapperMut,
};

use crate::{util::BoundedWriter, CompressionAlgo};

pub const MAX_COMPRESSION_LEVEL: u32 = 11;

pub struct BrotliCompression {
    // quality range 0..=11
    quality: u32,
    // window_bits range:
    // 10..=24 without large_window_size feature
    // 10..=30 with large_window_size feature
    window_bits: u32,
    // library default is 11/22 for Brotli
    custom_dictionary: Vec<u8>,
}

impl Default for BrotliCompression {
    fn default() -> Self {
        Self {
            quality: 11,
            window_bits: 22,
            custom_dictionary: Vec::new(),
        }
    }
}

impl CompressionAlgo for BrotliCompression {
    type CompressError = std::io::Error;
    type DecompressError = std::io::Error;

    fn new(quality: u32, window_bits: u32, custom_dictionary: Vec<u8>) -> Self {
        Self {
            quality: quality.min(MAX_COMPRESSION_LEVEL),
            window_bits,
            custom_dictionary,
        }
    }

    fn compress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::CompressError> {
        let mut input_buffer = StandardAlloc {}.alloc_cell(4096);
        let mut output_buffer = StandardAlloc {}.alloc_cell(4096);
        let mut params = BrotliEncoderInitParams();
        params.quality = self.quality as i32;
        params.lgwin = self.window_bits as i32;

        BrotliCompressCustomIoCustomDict(
            &mut IoReaderWrapper(&mut Cursor::new(input)),
            &mut IoWriterWrapper(output),
            input_buffer.slice_mut(),
            output_buffer.slice_mut(),
            &params,
            StandardAlloc {},
            &mut |_a1, _a2, _a3, _a4| {},
            &self.custom_dictionary,
            std::io::Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"),
        )
        .map(|_size| ())
    }

    fn decompress(
        &self,
        input: &[u8],
        output: &mut BoundedWriter,
    ) -> Result<(), Self::DecompressError> {
        let mut input_buffer = StandardAlloc {}.alloc_cell(4096);
        let mut output_buffer = StandardAlloc {}.alloc_cell(4096);

        BrotliDecompressCustomIoCustomDict(
            &mut IoReaderWrapper(&mut Cursor::new(input)),
            &mut IoWriterWrapper(output),
            input_buffer.slice_mut(),
            output_buffer.slice_mut(),
            StandardAlloc {},
            StandardAlloc {},
            StandardAlloc {},
            self.custom_dictionary.clone().into(),
            std::io::Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF"),
        )
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

        let algo = BrotliCompression::new(4, 22, Vec::new());

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
    fn test_lossless_compression_with_custom_dictionary() {
        let mut dictionary = Vec::new();

        let dictionary_path = "examples/example.dict";
        let mut dictionary_file = File::open(dictionary_path).expect("file exists");
        dictionary_file.read_to_end(&mut dictionary).unwrap();

        let mut data = Vec::new();
        File::open("examples/txbatch.rlp")
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        let algo = BrotliCompression::new(4, 22, dictionary);

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
}
