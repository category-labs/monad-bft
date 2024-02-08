use core::panic;
use std::collections::BTreeMap;

use monad_eth_tx::EthTxHash;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EthTxState {
    NotProposed,
    // Number of blocks (> 0) the transaction (not committed) is in
    Proposed(u8),
    // Number of blocks (> 0) the transaction (committed) is in
    Committed(u8),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EthTxStates {
    pub states: BTreeMap<EthTxHash, EthTxState>,
    pub num_not_proposed: usize,
}

impl EthTxStates {
    /// Used when the transaction is proposed in a new block
    pub fn add_proposed(&mut self, tx_hash: EthTxHash) {
        let tx_state = self.states.entry(tx_hash).or_insert_with(|| {
            self.num_not_proposed += 1;
            EthTxState::NotProposed
        });

        // A committed transaction should not be proposed again
        assert!(matches!(
            *tx_state,
            EthTxState::NotProposed | EthTxState::Proposed(_)
        ));

        match tx_state {
            EthTxState::NotProposed => {
                *tx_state = EthTxState::Proposed(1);

                assert!(self.num_not_proposed > 0);
                self.num_not_proposed -= 1;
            }
            EthTxState::Proposed(num_blocks) => {
                assert!(*num_blocks > 0);
                *num_blocks += 1;
            }
            EthTxState::Committed(_) => {
                panic!("asserted");
            }
        }
    }

    /// Used only when the block with the transaction has been pruned from the blocktree
    pub fn remove_proposed(&mut self, tx_hash: EthTxHash) {
        let tx_state = self
            .states
            .get_mut(&tx_hash)
            .expect("transaction should exist");

        // A transaction that is not proposed should not be removed
        assert!(matches!(
            tx_state,
            EthTxState::Proposed(_) | EthTxState::Committed(_)
        ));

        match tx_state {
            EthTxState::NotProposed => panic!("asserted"),
            EthTxState::Proposed(num_blocks) => {
                if *num_blocks > 1 {
                    *num_blocks -= 1;
                } else {
                    *tx_state = EthTxState::NotProposed;

                    self.num_not_proposed += 1;
                }
            }
            EthTxState::Committed(num_blocks) => {
                if *num_blocks > 1 {
                    *num_blocks -= 1;
                } else {
                    self.states
                        .remove(&tx_hash)
                        .expect("transaction should be removed");
                }
            }
        }
    }

    /// Used only when the block with the transaction is committed
    pub fn update_committed(&mut self, tx_hash: EthTxHash) {
        let tx_state = self
            .states
            .get_mut(&tx_hash)
            .expect("transaction was verified");

        assert!(
            matches!(tx_state, EthTxState::Proposed(_)),
            "transaction should be in proposed state"
        );

        match tx_state {
            EthTxState::NotProposed => panic!("asserted"),
            EthTxState::Proposed(num_blocks) => {
                *tx_state = EthTxState::Committed(*num_blocks);
            }
            EthTxState::Committed(_) => panic!("asserted"),
        }
    }

    /// Evict the first transaction that is not proposed yet. Return None
    /// if there aren't any to evict.
    pub fn evict_not_proposed(&mut self) -> Option<EthTxHash> {
        let tx = self
            .states
            .iter()
            .find(|&(_, tx_state)| *tx_state == EthTxState::NotProposed);

        if let Some((tx_hash, _)) = tx {
            let tx_hash = *tx_hash;
            self.states.remove(&tx_hash);

            assert!(self.num_not_proposed >= 1);
            self.num_not_proposed -= 1;

            return Some(tx_hash);
        }

        None
    }

    /// Returns the number of transactions that are not proposed yet
    pub fn num_not_proposed(&self) -> usize {
        self.num_not_proposed
    }
}
