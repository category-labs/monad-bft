#[derive(Debug, Clone, Copy)]
pub struct PacketLayout {
    app_message_len: usize,
    merkle_tree_depth: u8,
}

impl PacketLayout {
    // prod v1 wire constants
    const SEGMENT_LEN: usize = 1440;
    const HEADER_LEN: usize = 117;
    const CHUNK_HEADER_LEN: usize = 4;
    const MERKLE_HASH_LEN: usize = 20;

    pub fn num_source_chunks(&self) -> usize {
        self.app_message_len.div_ceil(self.symbol_len())
    }

    pub fn symbol_len(&self) -> usize {
        Self::SEGMENT_LEN - Self::HEADER_LEN - self.merkle_proof_len() - Self::CHUNK_HEADER_LEN
    }

    pub fn merkle_proof_len(&self) -> usize {
        Self::MERKLE_HASH_LEN * (self.merkle_tree_depth as usize - 1)
    }

    pub fn merkle_tree_depth(&self) -> u8 {
        self.merkle_tree_depth
    }
}
