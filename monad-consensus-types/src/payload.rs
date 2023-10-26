use std::collections::BTreeMap;

use monad_crypto::hasher::{Hash, Hashable, Hasher};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::Round;
use zerocopy::AsBytes;

use crate::certificate_signature::{CertificateKeyPair, CertificateSignature};

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
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.parent_hash);
        state.update(self.state_root);
        state.update(self.transactions_root);
        state.update(self.receipts_root);
        state.update(self.logs_bloom);
        state.update(self.gas_used.as_bytes());
    }
}

#[derive(Clone, PartialEq, Eq)]
// TODO rename to TransactionHashList or something
pub struct TransactionList(pub Vec<u8>);

impl Default for TransactionList {
    fn default() -> Self {
        Self(vec![EMPTY_RLP_TX_LIST])
    }
}

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
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RandaoReveal(pub Vec<u8>);

impl RandaoReveal {
    pub fn new<CS: CertificateSignature>(round: Round, keypair: &CS::KeyPairType) -> Self {
        Self(CS::sign(&round.0.to_le_bytes(), keypair).serialize())
    }

    pub fn verify<CS: CertificateSignature>(
        &self,
        round: Round,
        pubkey: &<CS::KeyPairType as CertificateKeyPair>::PubKeyType,
    ) -> Result<(), CS::Error> {
        let sig = CS::deserialize(&self.0)?;

        sig.verify(&round.0.to_le_bytes(), pubkey)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Payload {
    pub txns: TransactionList,
    pub header: ExecutionArtifacts,
    pub seq_num: u64,
    pub beneficiary: EthAddress,
    pub randao_reveal: RandaoReveal,
}

impl Hashable for Payload {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.txns.0.as_bytes());
        self.header.hash(state);
        state.update(self.seq_num.as_bytes());
        state.update(self.beneficiary.0.as_bytes());
        state.update(self.randao_reveal.0.as_bytes());
    }
}

pub trait StateRootValidator {
    fn new(delay: u64) -> Self;
    fn add_state_root(&mut self, seq_num: u64, root_hash: Hash);
    fn get_next_state_root(&self, seq_num: u64) -> Option<Hash>;
    fn remove_old_roots(&mut self, latest_seq_num: u64);
    fn validate(&self, seq_num: u64, block_state_root_hash: Hash) -> StateRootResult;
}

#[derive(Debug, PartialEq)]
pub enum StateRootResult {
    Success,
    Mismatch,
    OutOfRange,
    Missing,
}

#[derive(Debug, Clone)]
pub struct StateRoot {
    // Map executed block seq_num to root hash
    pub root_hashes: BTreeMap<u64, Hash>,
    // Delay gap between root hash to use for current block
    // validation
    pub delay: u64,
}

impl StateRootValidator for StateRoot {
    fn new(delay: u64) -> Self {
        StateRoot {
            root_hashes: BTreeMap::from([(0, Hash([0; 32]))]),
            delay,
        }
    }

    fn add_state_root(&mut self, seq_num: u64, root_hash: Hash) {
        self.root_hashes.insert(seq_num, root_hash);
    }

    fn get_next_state_root(&self, seq_num: u64) -> Option<Hash> {
        if self.delay > seq_num {
            return Some(Hash([0; 32]));
        }

        self.root_hashes.get(&(seq_num - self.delay)).copied()
    }

    fn remove_old_roots(&mut self, latest_seq_num: u64) {
        if self.delay > latest_seq_num {
            return;
        }
        self.root_hashes
            .retain(|k, _| *k > (latest_seq_num - self.delay));
    }

    fn validate(&self, seq_num: u64, block_state_root_hash: Hash) -> StateRootResult {
        // FIXME:
        // for the first N blocks, there are no state roots so we need to decide
        // how to validate them -- right now, assuming anything with hash=0x0 is
        // fine
        if self.delay > seq_num {
            // TODO: Magic Value Hash to keep network moving?
            if Hash([0; 32]) == block_state_root_hash {
                return StateRootResult::Success;
            } else {
                return StateRootResult::Mismatch;
            }
        }

        let target_seq_num = seq_num - self.delay;
        let root_hash = self.root_hashes.get(&target_seq_num);
        match root_hash {
            None => match self.root_hashes.keys().max() {
                None => StateRootResult::OutOfRange,
                Some(max) => {
                    if target_seq_num > *max {
                        StateRootResult::OutOfRange
                    } else {
                        StateRootResult::Missing
                    }
                }
            },
            Some(r) => {
                if block_state_root_hash == *r {
                    StateRootResult::Success
                } else {
                    StateRootResult::Mismatch
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NopStateRoot {}

impl StateRootValidator for NopStateRoot {
    fn new(_delay: u64) -> Self {
        Self {}
    }

    fn add_state_root(&mut self, _seq_num: u64, _root_hash: Hash) {}

    fn get_next_state_root(&self, _seq_num: u64) -> Option<Hash> {
        Some(Hash([0; 32]))
    }

    fn validate(&self, _seq_num: u64, _block_state_root_hash: Hash) -> StateRootResult {
        StateRootResult::Success
    }

    fn remove_old_roots(&mut self, _latest_seq_num: u64) {}
}

#[cfg(test)]
mod test {
    use monad_crypto::hasher::Hash;

    use super::{StateRoot, StateRootValidator};
    use crate::payload::StateRootResult;

    #[test]
    fn state_root_impl_test() {
        let mut state_root = StateRoot::new(0);

        for i in 1..10 {
            state_root.add_state_root(i, Hash([i as u8; 32]));
        }
        for i in 1..10 {
            assert_eq!(
                state_root.validate(i, Hash([i as u8; 32])),
                StateRootResult::Success
            );
        }

        state_root.remove_old_roots(10);
        assert_eq!(state_root.root_hashes.len(), 0);

        assert_eq!(
            state_root.validate(10, Hash([0x0a_u8; 32])),
            StateRootResult::OutOfRange
        );

        state_root.add_state_root(10, Hash([0x01_u8; 32]));
        assert_eq!(
            state_root.validate(10, Hash([0x0a_u8; 32])),
            StateRootResult::Mismatch
        );

        assert_eq!(
            state_root.validate(5, Hash([0x0a_u8; 32])),
            StateRootResult::Missing
        );
    }
}
