use bytes::Bytes;

use crate::{error::Result, family::Hash32};

/// Thin shell for the later zero-copy trace view type.
///
/// Commit 3 keeps trace identity and raw frame storage real while deferring
/// call-frame field decoding to the later storage/layout commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceRef {
    block_num: u64,
    block_hash: Hash32,
    tx_idx: u32,
    trace_idx: u32,
    frame_bytes: Bytes,
}

impl TraceRef {
    pub fn new(
        block_num: u64,
        block_hash: Hash32,
        tx_idx: u32,
        trace_idx: u32,
        frame_bytes: Bytes,
    ) -> Result<Self> {
        Ok(Self {
            block_num,
            block_hash,
            tx_idx,
            trace_idx,
            frame_bytes,
        })
    }

    pub fn block_num(&self) -> u64 {
        self.block_num
    }

    pub fn block_hash(&self) -> &Hash32 {
        &self.block_hash
    }

    pub fn tx_idx(&self) -> u32 {
        self.tx_idx
    }

    pub fn trace_idx(&self) -> u32 {
        self.trace_idx
    }

    pub fn frame_bytes(&self) -> &[u8] {
        self.frame_bytes.as_ref()
    }

    pub fn into_frame_bytes(self) -> Bytes {
        self.frame_bytes
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::TraceRef;

    #[test]
    fn trace_ref_preserves_identity_and_bytes() {
        let trace_ref = TraceRef::new(11, [4; 32], 5, 6, Bytes::from(vec![7, 8, 9]))
            .expect("construct trace ref");

        assert_eq!(trace_ref.block_num(), 11);
        assert_eq!(trace_ref.block_hash(), &[4; 32]);
        assert_eq!(trace_ref.tx_idx(), 5);
        assert_eq!(trace_ref.trace_idx(), 6);
        assert_eq!(trace_ref.frame_bytes(), &[7, 8, 9]);
        assert_eq!(trace_ref.into_frame_bytes(), Bytes::from(vec![7, 8, 9]));
    }
}
