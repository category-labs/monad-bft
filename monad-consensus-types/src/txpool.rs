use bytes::Bytes;

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
}

/// This trait represents the storage of transactions that
/// are potentially available for a proposal
pub trait TxPool<SCT: SignatureCollection, BPT: BlockPolicy<SCT>> {
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
    fn insert_tx(&mut self, tx: Bytes) -> Result<(), TxPoolInsertionError>;

    /// Returns an RLP encoded lists of transactions to include in the proposal
    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        block_policy: &BPT,
        pending_blocks: Vec<&BPT::ValidatedBlock>,
    ) -> FullTransactionList;

    /// Reclaims memory used by internal TxPool datastructures
    fn clear(&mut self);
}

impl<SCT: SignatureCollection, BPT: BlockPolicy<SCT>, T: TxPool<SCT, BPT> + ?Sized> TxPool<SCT, BPT>
    for Box<T>
{
    fn insert_tx(&mut self, tx: Bytes) -> Result<(), TxPoolInsertionError> {
        (**self).insert_tx(tx)
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        block_policy: &BPT,
        pending_blocks: Vec<&BPT::ValidatedBlock>,
    ) -> FullTransactionList {
        (**self).create_proposal(tx_limit, gas_limit, block_policy, pending_blocks)
    }

    fn clear(&mut self) {
        (**self).clear()
    }
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

impl<SCT: SignatureCollection> TxPool<SCT, PassthruBlockPolicy> for MockTxPool {
    fn insert_tx(&mut self, _tx: Bytes) -> Result<(), TxPoolInsertionError> {
        Ok(())
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        _gas_limit: u64,
        _block_policy: &PassthruBlockPolicy,
        _pending_blocks: Vec<&<PassthruBlockPolicy as BlockPolicy<SCT>>::ValidatedBlock>,
    ) -> FullTransactionList {
        if tx_limit == 0 {
            FullTransactionList::empty()
        } else {
            // Random non-empty value with size = num_fetch_txs * hash_size
            let mut buf = vec![0; tx_limit * TXN_SIZE];
            self.rng.fill_bytes(buf.as_mut_slice());
            FullTransactionList::new(buf.into())
        }
    }

    fn clear(&mut self) {}
}
