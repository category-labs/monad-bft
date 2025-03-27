use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use alloy_primitives::Address;
use monad_eth_types::{Balance, EthAccount, Nonce};
use monad_types::{BlockId, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND, GENESIS_SEQ_NUM};
use serde::{Deserialize, Serialize};
use tracing::trace;

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
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError>;

    /// Fetches earliest block from storage backend
    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum>;
    /// Fetches latest block from storage backend
    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum>;
}

pub trait StateBackendTest {
    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_round: Round,
        new_account_nonces: BTreeMap<Address, Nonce>,
    );

    fn ledger_commit(&mut self, block_id: &BlockId);
}

pub type InMemoryState = Arc<Mutex<InMemoryStateInner>>;

#[derive(Debug, Clone)]
pub struct InMemoryStateInner {
    commits: BTreeMap<SeqNum, InMemoryBlockState>,
    proposals: BTreeMap<Round, InMemoryBlockState>,
    /// InMemoryState doesn't have access to an execution engine. It returns
    /// `max_account_balance` as the balance every time so txn fee balance check
    /// will pass if the sum doesn't exceed the max account balance
    max_account_balance: Balance,
    execution_delay: SeqNum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryBlockState {
    block_id: BlockId,
    seq_num: SeqNum,
    round: Round,
    parent_round: Round,
    nonces: BTreeMap<Address, Nonce>,
}

impl InMemoryBlockState {
    pub fn genesis(nonces: BTreeMap<Address, Nonce>) -> Self {
        Self {
            block_id: GENESIS_BLOCK_ID,
            seq_num: GENESIS_SEQ_NUM,
            round: GENESIS_ROUND,
            parent_round: GENESIS_ROUND,
            nonces,
        }
    }
}

impl InMemoryStateInner {
    pub fn genesis(max_account_balance: Balance, execution_delay: SeqNum) -> InMemoryState {
        Arc::new(Mutex::new(Self {
            commits: std::iter::once((
                GENESIS_SEQ_NUM,
                InMemoryBlockState::genesis(Default::default()),
            ))
            .collect(),
            proposals: Default::default(),
            max_account_balance,
            execution_delay,
        }))
    }
    pub fn new(
        max_account_balance: Balance,
        execution_delay: SeqNum,
        last_commit: InMemoryBlockState,
    ) -> InMemoryState {
        Arc::new(Mutex::new(Self {
            commits: std::iter::once((last_commit.seq_num, last_commit)).collect(),
            proposals: Default::default(),
            max_account_balance,
            execution_delay,
        }))
    }

    pub fn committed_state(&self, block: &SeqNum) -> Option<&InMemoryBlockState> {
        self.commits.get(block)
    }

    pub fn reset_state(&mut self, state: InMemoryBlockState) {
        self.proposals = Default::default();
        self.commits = std::iter::once((state.seq_num, state)).collect();
    }
}

impl StateBackendTest for InMemoryStateInner {
    // new_account_nonces is the changeset of nonces from a given block
    // if account A's last tx nonce in a block is N, then new_account_nonces should include A=N+1
    // this is because N+1 is the next valid nonce for A
    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_round: Round,
        new_account_nonces: BTreeMap<Address, Nonce>,
    ) {
        assert!(
            seq_num
                >= self
                    .raw_read_latest_finalized_block()
                    .expect("latest_finalized doesn't exist")
                    + SeqNum(1)
        );

        trace!(?block_id, ?seq_num, ?round, "ledger_propose");
        let mut last_state_nonces = if let Some(parent_state) = self.proposals.get(&parent_round) {
            parent_state.nonces.clone()
        } else {
            let last_committed_entry = self
                .commits
                .last_entry()
                .expect("last_commit doesn't exist");
            let last_committed_state = last_committed_entry.get();
            assert_eq!(last_committed_state.round, parent_round);
            last_committed_state.nonces.clone()
        };

        for (address, account_nonce) in new_account_nonces {
            last_state_nonces.insert(address, account_nonce);
        }

        self.proposals.insert(
            round,
            InMemoryBlockState {
                block_id,
                seq_num,
                round,
                parent_round,
                nonces: last_state_nonces,
            },
        );
    }

    fn ledger_commit(&mut self, block_id: &BlockId) {
        let proposal_round = self
            .proposals
            .values()
            .find(|proposal| &proposal.block_id == block_id)
            .unwrap_or_else(|| {
                panic!(
                    "committed proposal that doesn't exist, block_id={:?}",
                    block_id
                )
            })
            .round;
        let proposal = self
            .proposals
            .remove(&proposal_round)
            .expect("proposal_round exists");
        while self
            .proposals
            .first_entry()
            .is_some_and(|proposal| proposal.key() < &proposal_round)
        {
            self.proposals.pop_first();
        }
        let (_, last_commit) = self
            .commits
            .last_key_value()
            .expect("last_commit doesn't exist");
        assert_eq!(last_commit.seq_num + SeqNum(1), proposal.seq_num);
        assert_eq!(last_commit.round, proposal.parent_round);
        trace!(
            "ledger_commit insert block_id: {:?}, proposal_seq_num: {:?}, proposal: {:?}",
            block_id,
            proposal.seq_num,
            proposal
        );
        self.commits.insert(proposal.seq_num, proposal);
        if self.commits.len() > (self.execution_delay.0 as usize).saturating_mul(1000)
        // this is big just for statesync/blocksync. TODO don't hardcode
        {
            self.commits.pop_first();
        }
    }
}

impl StateBackend for InMemoryStateInner {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let state = if is_finalized
            && self
                .raw_read_latest_finalized_block()
                .is_some_and(|latest_finalized| &latest_finalized >= seq_num)
        {
            if self
                .raw_read_earliest_finalized_block()
                .is_some_and(|earliest_finalized| &earliest_finalized > seq_num)
            {
                return Err(StateBackendError::NeverAvailable);
            }
            let state = self
                .commits
                .get(seq_num)
                .expect("state doesn't exist but >= earliest, <= latest");
            assert_eq!(&state.block_id, block_id);
            state
        } else {
            let Some(proposal) = self.proposals.get(round) else {
                trace!(
                    ?round,
                    ?seq_num,
                    ?block_id,
                    ?is_finalized,
                    "NotAvailableYet"
                );
                return Err(StateBackendError::NotAvailableYet);
            };
            if &proposal.block_id != block_id {
                trace!(?block_id, proposal_block_id=?proposal.block_id, "does not matcn proposal block_id");
                return Err(StateBackendError::NotAvailableYet);
            }
            proposal
        };
        Ok(addresses
            .map(|address| {
                let nonce = state.nonces.get(address)?;
                Some(EthAccount {
                    nonce: *nonce,
                    balance: self.max_account_balance,
                    code_hash: None,
                })
            })
            .collect())
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.commits
            .first_key_value()
            .map(|(block, _)| block)
            .copied()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.commits
            .last_key_value()
            .map(|(block, _)| block)
            .copied()
    }
}

impl<T: StateBackend> StateBackend for Arc<Mutex<T>> {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        is_finalized: bool,
        addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let state = self.lock().unwrap();
        state.get_account_statuses(block_id, seq_num, round, is_finalized, addresses)
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        let state = self.lock().unwrap();
        state.raw_read_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        let state = self.lock().unwrap();
        state.raw_read_latest_finalized_block()
    }
}

impl<T: StateBackendTest> StateBackendTest for Arc<Mutex<T>> {
    fn ledger_commit(&mut self, block_id: &BlockId) {
        let mut state = self.lock().unwrap();
        state.ledger_commit(block_id);
    }

    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_round: Round,
        new_account_nonces: BTreeMap<Address, Nonce>,
    ) {
        let mut state = self.lock().unwrap();
        state.ledger_propose(block_id, seq_num, round, parent_round, new_account_nonces);
    }
}
