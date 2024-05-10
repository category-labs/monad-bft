use std::collections::HashSet;

use alloy_primitives::TxHash;
use bytes::Bytes;

use crate::payload::FullTransactionList;

/// This describes a method of obtaining transaction hashes from an RLP encoded block
pub type HashPolicyOutput = HashSet<TxHash>;
pub type HashPolicy<SCT> = fn(&Block<SCT>) -> Result<HashPolicyOutput, alloy_rlp::Error>;

/// This trait represents the storage of transactions that
/// are potentially available for a proposal
pub trait TxPool {
    /// Handle transactions submitted by users via RPC
    fn insert_tx(&mut self, tx: Bytes);

    /// Returns 2 RLP encoded lists of transactions
    /// The first list are the transactions to include in the
    /// proposal, the second is the leftover list.
    /// The leftover list is intended to be forwarded to another
    /// Node for inclusion in a future proposal
    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        pending_tx_hashes: HashPolicyOutput,
    ) -> (FullTransactionList, Option<FullTransactionList>);

    /// Handle transactions cascaded forward by other nodes
    fn handle_cascading_txns(&mut self) {}
}

impl<T: TxPool + ?Sized> TxPool for Box<T> {
    fn insert_tx(&mut self, tx: Bytes) {
        (**self).insert_tx(tx)
    }

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        gas_limit: u64,
        pending_tx_hashes: HashPolicyOutput,
    ) -> (FullTransactionList, Option<FullTransactionList>) {
        (**self).create_proposal(tx_limit, gas_limit, pending_tx_hashes)
    }
}

use rand::RngCore;
use rand_chacha::{rand_core::SeedableRng, ChaCha20Rng};

use crate::block::Block;

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

impl TxPool for MockTxPool {
    fn insert_tx(&mut self, _tx: Bytes) {}

    fn create_proposal(
        &mut self,
        tx_limit: usize,
        _gas_limit: u64,
        _pending_txs: HashPolicyOutput,
    ) -> (FullTransactionList, Option<FullTransactionList>) {
        if tx_limit == 0 {
            (FullTransactionList::empty(), None)
        } else {
            // Random non-empty value with size = num_fetch_txs * hash_size
            let mut buf = vec![0; tx_limit * TXN_SIZE];
            self.rng.fill_bytes(buf.as_mut_slice());
            (FullTransactionList::new(buf.into()), None)
        }
    }
}
