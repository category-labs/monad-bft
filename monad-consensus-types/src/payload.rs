use std::{collections::BTreeMap, sync::Arc};

use monad_crypto::hasher::{Hash, Hashable, Hasher};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::{Round, SeqNum};
use zerocopy::AsBytes;

use crate::certificate_signature::{CertificateKeyPair, CertificateSignature};

const BLOOM_SIZE: usize = 256;

/// Type to represent the Ethereum Logs Bloom Filter
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

/// Ethereum unit of gas
#[repr(transparent)]
#[derive(Debug, Default, Copy, Clone, Eq, Ord, PartialEq, PartialOrd, AsBytes)]
pub struct Gas(pub u64);

impl AsRef<[u8]> for Gas {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// A subset of Ethereum block header fields that are included in consensus
/// proposals. The values are populated from the results of executing the
/// previous block
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
        state.update(self.gas_used);
    }
}

/// RLP encoded list of the 256-bit hashes of a set of Eth transactions
#[derive(Clone, PartialEq, Eq)]
pub struct TransactionHashList(Arc<Vec<u8>>);

impl TransactionHashList {
    pub fn new(tx_hashes: Vec<u8>) -> Self {
        Self(Arc::new(tx_hashes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Default for TransactionHashList {
    fn default() -> TransactionHashList {
        Self::new(vec![EMPTY_RLP_TX_LIST])
    }
}

impl std::fmt::Debug for TransactionHashList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TxnHashes").field(&self.0).finish()
    }
}

impl AsRef<[u8]> for TransactionHashList {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// RLP encoded list of a set of full RLP encoded Eth transactions
// Do NOT derive or implement Default!
// Empty byte array is not valid RLP
#[derive(Clone, PartialEq, Eq)]
pub struct FullTransactionList(Arc<Vec<u8>>);

impl FullTransactionList {
    pub fn new(txs: Vec<u8>) -> Self {
        Self(Arc::new(txs))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for FullTransactionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Txns").field(&self.0).finish()
    }
}

/// randao_reveal uses a proposer's public key to contribute randomness
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

impl AsRef<[u8]> for RandaoReveal {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Clone, PartialEq, Eq)]
pub struct Payload {
    pub txns: TransactionHashList,
    pub header: ExecutionArtifacts,
    pub seq_num: SeqNum,
    pub beneficiary: EthAddress,
    pub randao_reveal: RandaoReveal,
}

impl Hashable for Payload {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(&self.txns);
        self.header.hash(state);
        state.update(self.seq_num);
        state.update(self.beneficiary);
        state.update(&self.randao_reveal);
    }
}

/// Provides methods for Consensus to validate state root hashes
pub trait StateRootValidator {
    /// Create a new StateRootValidator with the size of the expected
    /// delay gap between the current Proposal's state root and the state
    /// root to check against
    fn new(delay: SeqNum) -> Self;

    /// Add the state root hash that corresponds to the sequence number
    fn add_state_root(&mut self, seq_num: SeqNum, root_hash: Hash);

    /// Given the current sequence number, retrieve the state root hash
    /// that should be used in the current Proposal (accounting for the delay gap)
    fn get_next_state_root(&self, seq_num: SeqNum) -> Option<Hash>;

    /// Given the current sequence number, remove old state root hashes
    /// that won't be needed anymore (accounting for the delay gap)
    fn remove_old_roots(&mut self, latest_seq_num: SeqNum);

    /// Check the validity of the state root hash in a Proposal from the sequence
    /// number and state root hash it includes
    fn validate(&self, seq_num: SeqNum, block_state_root_hash: Hash) -> StateRootResult;
}

/// The outcomes of validating state root hashes
#[derive(Debug, PartialEq)]
pub enum StateRootResult {
    /// State root hash is valid
    Success,

    /// State root hash does not match our expected value
    Mismatch,

    /// State root hash is from a sequence number that exceeds
    /// what this node has executed. Implies that this node has
    /// fallen behind
    OutOfRange,

    /// State root hash for the sequence number is missing. Implies
    /// this node has missed an update from execution
    Missing,
}

/// Special hash value used in proposals for the first delay-num blocks
/// from genesis when there isn't a valid state root hash to include.
const INITIAL_DELAY_STATE_ROOT_HASH: Hash = Hash([0; 32]);

#[derive(Debug, Clone)]
pub struct StateRoot {
    /// Map executed block seq_num to root hash
    pub root_hashes: BTreeMap<SeqNum, Hash>,

    /// Delay gap between root hash to use for current block
    /// validation
    pub delay: SeqNum,
}

impl StateRootValidator for StateRoot {
    /// creates StateRoot with an initial root hash entry for the genesis block which
    /// hash sequence number 0
    fn new(delay: SeqNum) -> Self {
        StateRoot {
            root_hashes: BTreeMap::from([(SeqNum(0), INITIAL_DELAY_STATE_ROOT_HASH)]),
            delay,
        }
    }

    fn add_state_root(&mut self, seq_num: SeqNum, root_hash: Hash) {
        self.root_hashes.insert(seq_num, root_hash);
    }

    /// Gets the state root hash that should be used in a proposal for a given
    /// sequence number (accounts for the delay gap)
    /// If the sequence number is less than the delay gap (the initial delay-gap num
    /// blocks of the network), the INITIAL_DELAY_STATE_ROOT_HASH is used
    fn get_next_state_root(&self, seq_num: SeqNum) -> Option<Hash> {
        if self.delay > seq_num {
            return Some(INITIAL_DELAY_STATE_ROOT_HASH);
        }

        self.root_hashes.get(&(seq_num - self.delay)).copied()
    }

    fn remove_old_roots(&mut self, latest_seq_num: SeqNum) {
        if self.delay > latest_seq_num {
            return;
        }
        self.root_hashes
            .retain(|k, _| *k > (latest_seq_num - self.delay));
    }

    /// If the sequence number is less than the delay gap (the initial delay-gap num
    /// blocks of the network), the INITIAL_DELAY_STATE_ROOT_HASH is the expected
    /// state root hash
    fn validate(&self, seq_num: SeqNum, block_state_root_hash: Hash) -> StateRootResult {
        if self.delay > seq_num {
            if block_state_root_hash == INITIAL_DELAY_STATE_ROOT_HASH {
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
    fn new(_delay: SeqNum) -> Self {
        Self {}
    }

    fn add_state_root(&mut self, _seq_num: SeqNum, _root_hash: Hash) {}

    fn get_next_state_root(&self, _seq_num: SeqNum) -> Option<Hash> {
        Some(Hash([0; 32]))
    }

    fn validate(&self, _seq_num: SeqNum, _block_state_root_hash: Hash) -> StateRootResult {
        StateRootResult::Success
    }

    fn remove_old_roots(&mut self, _latest_seq_num: SeqNum) {}
}

#[cfg(test)]
mod test {
    use monad_crypto::hasher::Hash;
    use monad_types::SeqNum;

    use super::{StateRoot, StateRootValidator};
    use crate::payload::StateRootResult;

    #[test]
    fn state_root_impl_test() {
        let mut state_root = StateRoot::new(SeqNum(0));

        for i in 1..10 {
            state_root.add_state_root(SeqNum(i), Hash([i as u8; 32]));
        }
        for i in 1..10 {
            assert_eq!(
                state_root.validate(SeqNum(i), Hash([i as u8; 32])),
                StateRootResult::Success
            );
        }

        state_root.remove_old_roots(SeqNum(10));
        assert_eq!(state_root.root_hashes.len(), 0);

        assert_eq!(
            state_root.validate(SeqNum(10), Hash([0x0a_u8; 32])),
            StateRootResult::OutOfRange
        );

        state_root.add_state_root(SeqNum(10), Hash([0x01_u8; 32]));
        assert_eq!(
            state_root.validate(SeqNum(10), Hash([0x0a_u8; 32])),
            StateRootResult::Mismatch
        );

        assert_eq!(
            state_root.validate(SeqNum(5), Hash([0x0a_u8; 32])),
            StateRootResult::Missing
        );
    }
}
