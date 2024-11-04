use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use monad_eth_types::{Balance, EthAccount, EthAddress, Nonce};
use monad_types::{SeqNum, GENESIS_SEQ_NUM};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq)]
pub enum StateBackendError {
    /// not available yet
    NotAvailableYet,
    /// will never be available
    NeverAvailable,
}

/// Backend provider of account data: balance and nonce
pub trait StateBackend {
    fn get_account_statuses<'a>(
        &self,
        block: SeqNum,
        addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let latest = self.raw_read_latest_block();
        if latest < block {
            // latest < block
            return Err(StateBackendError::NotAvailableYet);
        }
        // block <= latest

        let statuses = addresses
            .map(|address| self.raw_read_account(block, address))
            .collect();

        let earliest = self.raw_read_earliest_block();
        if block < earliest {
            // block < earliest
            return Err(StateBackendError::NeverAvailable);
        }

        // all accounts are now guaranteed to be fully consistent and correct
        Ok(statuses)
    }

    /// Fetches account from storage backend
    /// Must be sequentially consistent
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount>;
    /// Fetches earliest block from storage backend
    /// Must be sequentially consistent
    fn raw_read_earliest_block(&self) -> SeqNum;
    /// Fetches latest block from storage backend
    /// Must be sequentially consistent
    fn raw_read_latest_block(&self) -> SeqNum;
}

pub type InMemoryState = Arc<Mutex<InMemoryStateInner>>;

#[derive(Debug, Clone)]
pub struct InMemoryStateInner {
    states: BTreeMap<SeqNum, InMemoryBlockState>,
    commits: BTreeSet<SeqNum>,
    /// InMemoryState doesn't have access to an execution engine. It returns
    /// `max_account_balance` as the balance every time so txn fee balance check
    /// will pass if the sum doesn't exceed the max account balance
    max_account_balance: Balance,
    execution_delay: SeqNum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryBlockState {
    block: SeqNum,
    nonces: BTreeMap<EthAddress, Nonce>,
}

impl InMemoryBlockState {
    pub fn genesis(nonces: BTreeMap<EthAddress, Nonce>) -> Self {
        Self {
            block: GENESIS_SEQ_NUM,
            nonces,
        }
    }
}

impl InMemoryStateInner {
    pub fn genesis(max_account_balance: Balance, execution_delay: SeqNum) -> InMemoryState {
        Arc::new(Mutex::new(Self {
            states: std::iter::once((
                GENESIS_SEQ_NUM,
                InMemoryBlockState::genesis(Default::default()),
            ))
            .collect(),
            commits: std::iter::once(GENESIS_SEQ_NUM).collect(),
            max_account_balance,
            execution_delay,
        }))
    }
    pub fn new(
        max_account_balance: Balance,
        execution_delay: SeqNum,
        init_state: InMemoryBlockState,
    ) -> InMemoryState {
        Arc::new(Mutex::new(Self {
            states: std::iter::once((init_state.block, init_state)).collect(),
            commits: std::iter::once(GENESIS_SEQ_NUM).collect(),
            max_account_balance,
            execution_delay,
        }))
    }

    // new_account_nonces is the changeset of nonces from a given block
    // if account A's last tx nonce in a block is N, then new_account_nonces should include A=N+1
    // this is because N+1 is the next valid nonce for A
    pub fn ledger_commit(
        &mut self,
        seq_num: SeqNum,
        new_account_nonces: BTreeMap<EthAddress, Nonce>,
    ) {
        assert!(seq_num <= self.raw_read_latest_block() + SeqNum(1));
        self.commits.insert(seq_num);

        if (seq_num.0.saturating_sub(self.execution_delay.0)..seq_num.0)
            .all(|block| self.commits.contains(&SeqNum(block)))
        {
            // we have `delay` number of blocks, so we can execute
            let mut last_state_nonces = self
                .states
                .last_entry()
                .map_or(Default::default(), |entry| entry.get().nonces.clone());
            for (address, account_nonce) in new_account_nonces {
                last_state_nonces.insert(address, account_nonce);
            }
            let last_state = InMemoryBlockState {
                block: seq_num,
                nonces: last_state_nonces,
            };
            assert_eq!(seq_num, last_state.block);
            self.states.insert(seq_num, last_state);
        }
    }

    pub fn block_state(&self, block: &SeqNum) -> Option<&InMemoryBlockState> {
        self.states.get(block)
    }

    pub fn reset_state(&mut self, state: InMemoryBlockState) {
        self.states = std::iter::once((state.block, state)).collect();
    }
}

impl StateBackend for InMemoryStateInner {
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        let nonce = self.states.get(&block)?.nonces.get(address)?;
        Some(EthAccount {
            nonce: *nonce,
            balance: self.max_account_balance,
            code_hash: None,
        })
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        self.states
            .first_key_value()
            .map(|(block, _)| block)
            .copied()
            .unwrap_or(GENESIS_SEQ_NUM)
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        self.states
            .last_key_value()
            .map(|(block, _)| block)
            .copied()
            .unwrap_or(GENESIS_SEQ_NUM)
    }
}

impl<T: StateBackend> StateBackend for Arc<Mutex<T>> {
    fn raw_read_account(&self, block: SeqNum, address: &EthAddress) -> Option<EthAccount> {
        let state = self.lock().unwrap();
        state.raw_read_account(block, address)
    }

    fn raw_read_earliest_block(&self) -> SeqNum {
        let state = self.lock().unwrap();
        state.raw_read_earliest_block()
    }

    fn raw_read_latest_block(&self) -> SeqNum {
        let state = self.lock().unwrap();
        state.raw_read_latest_block()
    }
}
