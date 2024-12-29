use std::collections::BTreeMap;

use auto_impl::auto_impl;
use bytes::Bytes;
use monad_crypto::{
    certificate_signature::{CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hashable, Hasher, HasherType},
};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::{DontCare, EnumDiscriminant, Round, SeqNum};
use serde::{Deserialize, Serialize};
use zerocopy::AsBytes;

use crate::state_root_hash::StateRootHash;

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
        &self.0
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

pub const BASE_FEE_PER_GAS: u64 = 50_000_000_000;
pub const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
/// Max proposal size in bytes (average transactions ~400 bytes)
pub const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;

/// A subset of Ethereum block header fields that are included in consensus
/// proposals. The values are populated from the results of executing the
/// previous block
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProtocol {
    pub state_root: StateRootHash,
    pub seq_num: SeqNum,
    pub beneficiary: EthAddress,
    pub randao_reveal: RandaoReveal,
}

impl DontCare for ExecutionProtocol {
    fn dont_care() -> Self {
        ExecutionProtocol {
            state_root: Default::default(),
            seq_num: SeqNum(1),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
        }
    }
}

impl Hashable for ExecutionProtocol {
    fn hash(&self, state: &mut impl Hasher) {
        state.update(self.state_root);
        state.update(self.seq_num);
        state.update(self.beneficiary);
        state.update(&self.randao_reveal);
    }
}

/// RLP encoded list of a set of full RLP encoded Eth transactions
// Do NOT derive or implement Default!
// Empty byte array is not valid RLP
#[derive(Clone, PartialEq, Eq)]
pub struct FullTransactionList(Bytes);

impl FullTransactionList {
    pub fn empty() -> Self {
        Self::new(vec![EMPTY_RLP_TX_LIST].into())
    }

    pub fn new(txs: Bytes) -> Self {
        Self(txs)
    }

    pub fn bytes(&self) -> &Bytes {
        &self.0
    }
}

impl std::fmt::Debug for FullTransactionList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Txns").field(&self.0).finish()
    }
}

impl AsRef<[u8]> for FullTransactionList {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// randao_reveal uses a proposer's public key to contribute randomness
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RandaoReveal(pub Vec<u8>);

impl RandaoReveal {
    pub fn new<CS: CertificateSignature>(round: Round, keypair: &CS::KeyPairType) -> Self {
        Self(CS::sign(&round.0.to_le_bytes(), keypair).serialize())
    }

    pub fn verify<CS: CertificateSignature>(
        &self,
        round: Round,
        pubkey: &CertificateSignaturePubKey<CS>,
    ) -> Result<(), CS::Error> {
        let sig = CS::deserialize(&self.0)?;

        sig.verify(&round.0.to_le_bytes(), pubkey)
    }
}

impl AsRef<[u8]> for RandaoReveal {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionPayload {
    List(FullTransactionList),
    Null,
}

impl Hashable for TransactionPayload {
    fn hash(&self, state: &mut impl Hasher) {
        match self {
            TransactionPayload::List(txns) => {
                EnumDiscriminant(1).hash(state);
                state.update(txns);
            }
            TransactionPayload::Null => {
                EnumDiscriminant(2).hash(state);
            }
        }
    }
}

/// Contents of a proposal that are part of the Monad protocol
/// but not in the core bft consensus protocol
#[derive(Clone, PartialEq, Eq)]
pub struct Payload {
    pub txns: TransactionPayload,
}
impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payload")
            .field(
                "txn_payload_len",
                &match &self.txns {
                    TransactionPayload::List(txns) => {
                        format!("{:?}", txns.bytes().len())
                    }
                    TransactionPayload::Null => "null".to_owned(),
                },
            )
            .finish_non_exhaustive()
    }
}

impl Payload {
    pub fn get_id(&self) -> PayloadId {
        let hash = HasherType::hash_object(self);
        PayloadId(hash)
    }
}

impl Hashable for Payload {
    fn hash(&self, state: &mut impl Hasher) {
        self.txns.hash(state);
    }
}

impl DontCare for Payload {
    fn dont_care() -> Self {
        Self {
            txns: TransactionPayload::List(FullTransactionList::empty()),
        }
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PayloadId(pub Hash);

impl std::fmt::Debug for PayloadId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:>02x}{:>02x}..{:>02x}{:>02x}",
            self.0[0], self.0[1], self.0[30], self.0[31]
        )
    }
}

/// Provides methods for Consensus to validate state root hashes
#[auto_impl(Box)]
pub trait StateRootValidator {
    /// Add the state root hash that corresponds to the sequence number
    fn add_state_root(&mut self, seq_num: SeqNum, root_hash: StateRootHash);

    /// Given the current sequence number, retrieve the state root hash
    /// that should be used in the current Proposal (accounting for the delay gap)
    fn get_next_state_root(&self, seq_num: SeqNum) -> Option<StateRootHash>;

    /// Check the validity of the state root hash in a Proposal from the sequence
    /// number and state root hash it includes
    fn validate(&self, seq_num: SeqNum, block_state_root_hash: StateRootHash) -> StateRootResult;

    /// Return delay parameter
    fn get_delay(&self) -> SeqNum;
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
pub const INITIAL_DELAY_STATE_ROOT_HASH: StateRootHash = StateRootHash(Hash([0; 32]));

#[derive(Debug, Clone)]
pub struct StateRoot {
    /// Map executed block seq_num to root hash
    pub root_hashes: BTreeMap<SeqNum, StateRootHash>,

    /// Delay gap between root hash to use for current block
    /// validation
    pub delay: SeqNum,
}

impl StateRoot {
    /// `delay` is the expected delay gap between the current Proposal's state root and the delay
    /// to check against
    pub fn new(delay: SeqNum) -> Self {
        Self {
            root_hashes: BTreeMap::default(),
            delay,
        }
    }
}

impl StateRootValidator for StateRoot {
    fn add_state_root(&mut self, seq_num: SeqNum, root_hash: StateRootHash) {
        let maybe_removed = self.root_hashes.insert(seq_num, root_hash);
        if let Some(removed) = maybe_removed {
            assert_eq!(removed, root_hash)
        }

        // we only need to maintain the last `delay` number of state roots
        let max_seqnum = *self.root_hashes.last_entry().expect("at least 1").key();
        while self
            .root_hashes
            .first_entry()
            .is_some_and(|entry| *entry.key() + self.delay < max_seqnum)
        {
            self.root_hashes.pop_first();
        }
    }

    /// Gets the state root hash that should be used in a proposal for a given
    /// sequence number (accounts for the delay gap)
    /// If the sequence number is less than the delay gap (the initial delay-gap num
    /// blocks of the network), the INITIAL_DELAY_STATE_ROOT_HASH is used
    fn get_next_state_root(&self, seq_num: SeqNum) -> Option<StateRootHash> {
        if self.delay > seq_num {
            return Some(INITIAL_DELAY_STATE_ROOT_HASH);
        }

        self.root_hashes.get(&(seq_num - self.delay)).copied()
    }

    /// If the sequence number is less than the delay gap (the initial delay-gap num
    /// blocks of the network), the INITIAL_DELAY_STATE_ROOT_HASH is the expected
    /// state root hash
    fn validate(&self, seq_num: SeqNum, block_state_root_hash: StateRootHash) -> StateRootResult {
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

    fn get_delay(&self) -> SeqNum {
        self.delay
    }
}

#[derive(Debug, Clone, Default)]
pub struct NopStateRoot;

impl StateRootValidator for NopStateRoot {
    fn add_state_root(&mut self, _seq_num: SeqNum, _root_hash: StateRootHash) {}

    fn get_next_state_root(&self, _seq_num: SeqNum) -> Option<StateRootHash> {
        Some(StateRootHash(Hash([0; 32])))
    }

    fn validate(&self, _seq_num: SeqNum, _block_state_root_hash: StateRootHash) -> StateRootResult {
        StateRootResult::Success
    }

    fn get_delay(&self) -> SeqNum {
        // TODO we should tie this to the InMemoryStateBackend
        SeqNum(u32::MAX.into())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MissingNextStateRoot {}

impl StateRootValidator for MissingNextStateRoot {
    fn add_state_root(&mut self, _seq_num: SeqNum, _root_hash: StateRootHash) {}

    fn get_next_state_root(&self, _seq_num: SeqNum) -> Option<StateRootHash> {
        None
    }

    fn validate(&self, _seq_num: SeqNum, _block_state_root_hash: StateRootHash) -> StateRootResult {
        StateRootResult::Success
    }

    fn get_delay(&self) -> SeqNum {
        // Note: Zero delay means that this node never requests state root
        // updates
        SeqNum(0)
    }
}

#[cfg(test)]
mod test {
    use monad_crypto::hasher::Hash;
    use monad_types::SeqNum;

    use super::StateRootValidator;
    use crate::{
        payload::{StateRoot, StateRootResult},
        state_root_hash::StateRootHash,
    };

    #[test]
    fn state_root_impl_test() {
        let mut state_root = StateRoot::new(SeqNum(0));

        for i in 1..10 {
            state_root.add_state_root(SeqNum(i), StateRootHash(Hash([i as u8; 32])));
            assert_eq!(
                state_root.validate(SeqNum(i), StateRootHash(Hash([i as u8; 32]))),
                StateRootResult::Success
            );
        }

        assert_eq!(state_root.root_hashes.len(), 1);

        assert_eq!(
            state_root.validate(SeqNum(10), StateRootHash(Hash([0x0a_u8; 32]))),
            StateRootResult::OutOfRange
        );

        state_root.add_state_root(SeqNum(10), StateRootHash(Hash([0x01_u8; 32])));
        assert_eq!(
            state_root.validate(SeqNum(10), StateRootHash(Hash([0x0a_u8; 32]))),
            StateRootResult::Mismatch
        );

        assert_eq!(
            state_root.validate(SeqNum(5), StateRootHash(Hash([0x0a_u8; 32]))),
            StateRootResult::Missing
        );
    }
}
