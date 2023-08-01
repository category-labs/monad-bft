use monad_types::Hash;
use zerocopy::AsBytes;

use crate::validation::{Hashable, Hasher};

const BLOOM_SIZE: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bloom(pub [u8; BLOOM_SIZE]);

impl Bloom {
    pub fn zero() -> Self {
        Bloom([0x00_u8; BLOOM_SIZE])
    }
}

impl AsRef<[u8]> for Bloom {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[repr(transparent)]
#[derive(Debug, Default, Copy, Clone, Eq, Ord, PartialEq, PartialOrd, AsBytes)]
pub struct Gas(pub u64);

impl AsRef<[u8]> for Gas {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionArtifacts {
    pub parent_hash: Hash,
    pub state_root: Hash,
    pub transactions_root: Hash,
    pub receipts_root: Hash,
    pub logs_bloom: Bloom,
    pub gas_used: Gas,
}

impl ExecutionArtifacts {
    pub fn zero() -> Self {
        ExecutionArtifacts {
            parent_hash: Default::default(),
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            logs_bloom: Bloom::zero(),
            gas_used: Gas(0),
        }
    }
}

impl Hashable for ExecutionArtifacts {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.parent_hash);
        state.update(self.state_root);
        state.update(self.transactions_root);
        state.update(self.receipts_root);
        state.update(self.logs_bloom);
        state.update(self.gas_used.as_bytes());
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
// TODO rename to TransactionHashList or something
pub struct TransactionList(pub Vec<u8>);

impl std::fmt::Debug for TransactionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TxnHashes").field(&self.0).finish()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub struct FullTransactionList(pub Vec<u8>);

impl std::fmt::Debug for FullTransactionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Txns").field(&self.0).finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Payload {
    pub txns: TransactionList,
    pub header: ExecutionArtifacts,
}

impl Hashable for Payload {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.update(self.txns.0.as_bytes());
        self.header.hash(state);
    }
}
