use auto_impl::auto_impl;
use bytes::Bytes;
use monad_state_backend::{InMemoryState, StateBackend, StateBackendError};
use monad_types::SeqNum;

use crate::{
    block::{BlockPolicy, PassthruBlockPolicy},
    payload::FullTransactionList,
    signature_collection::SignatureCollection,
};

#[derive(Debug)]
pub enum TxPoolInsertionError {
    NotWellFormed,
    NonceTooLow,
    FeeTooLow,
    InsufficientBalance,
    PoolFull,
}

/// This trait represents the storage of transactions that
/// are potentially available for a proposal
#[auto_impl(Box)]
pub trait TxPool<SCT, BPT, SBT>
where
    SCT: SignatureCollection,
    BPT: BlockPolicy<SCT, SBT>,
    SBT: StateBackend,
{
    /// Handle transactions:
    /// 1. submitted by users via RPC
    /// 2. forwarded by other validators/full-nodes
    ///
    /// Note that right now, tx is not guaranteed to be well-formed. The IPC path will always be
    /// well-formed, but the forwarded tx path is not.
    ///
    /// Because of this, for now, tx well-formedness must be checked again inside the insert_tx
    /// implementation. Ideally, the Bytes type should be replaced with a DecodedTx type which
    /// TxPool is generic over.
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        block_policy: &BPT,
        state_backend: &SBT,
    ) -> Vec<Bytes>;

    /// Returns an RLP encoded lists of transactions to include in the proposal
    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        gas_limit: u64,
        block_policy: &BPT,
        pending_blocks: Vec<&BPT::ValidatedBlock>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError>;

    /// Reclaims memory used by internal TxPool datastructures
    fn clear(&mut self);
}

use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};

const MOCK_DEFAULT_SEED: u64 = 1;
const TXN_SIZE: usize = 32;

#[derive(Clone)]
pub struct MockTxPool {
    rng: ChaCha20Rng,
}

impl Default for MockTxPool {
    fn default() -> Self {
        Self {
            rng: ChaCha20Rng::seed_from_u64(MOCK_DEFAULT_SEED),
        }
    }
}

impl<SCT> TxPool<SCT, PassthruBlockPolicy, InMemoryState> for MockTxPool
where
    SCT: SignatureCollection,
{
    fn insert_tx(
        &mut self,
        _tx: Vec<Bytes>,
        _block_policy: &PassthruBlockPolicy,
        _state_backend: &InMemoryState,
    ) -> Vec<Bytes> {
        vec![]
    }

    fn create_proposal(
        &mut self,
        _proposed_seq_num: SeqNum,
        tx_limit: usize,
        _gas_limit: u64,
        _block_policy: &PassthruBlockPolicy,
        _pending_blocks: Vec<
            &<PassthruBlockPolicy as BlockPolicy<SCT, InMemoryState>>::ValidatedBlock,
        >,
        _state_backend: &InMemoryState,
    ) -> Result<FullTransactionList, StateBackendError> {
        if tx_limit == 0 {
            Ok(FullTransactionList::empty())
        } else {
            // Random non-empty value with size = num_fetch_txs * hash_size
            let mut buf = vec![0; tx_limit * TXN_SIZE];
            self.rng.fill_bytes(buf.as_mut_slice());
            Ok(FullTransactionList::new(buf.into()))
        }
    }

    fn clear(&mut self) {}
}
