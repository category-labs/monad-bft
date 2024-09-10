#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    Lz4 {
        // TODO(rene): put algorithm-specific parameters here
    },
    BrotliCompression {
        // TODO(rene): put algorithm-specific parameters here
    },
    DeflateCompression {
        // TODO(rene): put algorithm-specific parameters here
    },
}
