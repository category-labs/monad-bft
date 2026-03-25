use bytes::Bytes;

use crate::{error::Result, family::Hash32};

/// Thin shell for the later zero-copy transaction view type.
///
/// Commit 3 keeps block identity and envelope bytes real while deferring any
/// encoded transaction field decoding to the later storage/layout commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRef {
    block_num: u64,
    block_hash: Hash32,
    tx_idx: u32,
    envelope_bytes: Bytes,
}

impl TxRef {
    pub fn new(
        block_num: u64,
        block_hash: Hash32,
        tx_idx: u32,
        envelope_bytes: Bytes,
    ) -> Result<Self> {
        Ok(Self {
            block_num,
            block_hash,
            tx_idx,
            envelope_bytes,
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

    pub fn envelope_bytes(&self) -> &[u8] {
        self.envelope_bytes.as_ref()
    }

    pub fn into_envelope_bytes(self) -> Bytes {
        self.envelope_bytes
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::TxRef;

    #[test]
    fn tx_ref_preserves_identity_and_bytes() {
        let tx_ref =
            TxRef::new(7, [9; 32], 3, Bytes::from(vec![1, 2, 3])).expect("construct tx ref");

        assert_eq!(tx_ref.block_num(), 7);
        assert_eq!(tx_ref.block_hash(), &[9; 32]);
        assert_eq!(tx_ref.tx_idx(), 3);
        assert_eq!(tx_ref.envelope_bytes(), &[1, 2, 3]);
        assert_eq!(tx_ref.into_envelope_bytes(), Bytes::from(vec![1, 2, 3]));
    }
}
