use core::panic;
use std::collections::BTreeMap;

use monad_eth_tx::EthTxHash;

#[derive(Debug, PartialEq, Eq)]
pub enum EthTxState {
    NotProposed,
    // Number of blocks (> 0) the transaction is in
    Proposed(u8),
}

#[derive(Debug, Default)]
pub struct EthTxStates {
    pub states: BTreeMap<EthTxHash, EthTxState>,
    pub num_not_proposed: usize,
}

impl EthTxStates {
    /// Used when the transaction is proposed in a new block
    pub fn add_proposal(&mut self, tx_hash: EthTxHash) {
        let tx_state = self
            .states
            .entry(tx_hash)
            .or_insert(EthTxState::NotProposed);
        match tx_state {
            EthTxState::NotProposed => {
                *tx_state = EthTxState::Proposed(1);

                assert!(self.num_not_proposed > 0);
                self.num_not_proposed -= 1;
            }
            EthTxState::Proposed(num_blocks) => {
                assert!(*num_blocks >= 1);
                *num_blocks += 1;
            }
        }
    }

    /// Used only when the block with the transaction has been rejected
    pub fn revert_proposal(&mut self, tx_hash: EthTxHash) {
        let tx_state = self
            .states
            .get_mut(&tx_hash)
            .expect("transaction should exist");

        assert!(
            matches!(tx_state, EthTxState::Proposed(_)),
            "logic error: should revert unproposed transaction"
        );

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
        }
    }

    /// Used only when the block with the transaction is committed
    pub fn remove_committed(&mut self, tx_hash: EthTxHash) {
        let tx_state = self.states.get(&tx_hash).expect("transaction was verified");

        assert!(
            matches!(tx_state, EthTxState::Proposed(_)),
            "transaction should be in proposed state"
        );

        self.states
            .remove(&tx_hash)
            .expect("transaction should be in the map");
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
