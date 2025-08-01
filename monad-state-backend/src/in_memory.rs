// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use alloy_consensus::Header;
use alloy_primitives::Address;
use monad_eth_types::{EthAccount, EthHeader};
use monad_types::{
    Balance, BlockId, Nonce, Round, SeqNum, GENESIS_BLOCK_ID, GENESIS_ROUND, GENESIS_SEQ_NUM,
};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{StateBackend, StateBackendError, StateBackendTest};

pub type InMemoryState = Arc<Mutex<InMemoryStateInner>>;

#[derive(Debug, Clone)]
pub struct InMemoryStateInner {
    commits: BTreeMap<SeqNum, InMemoryBlockState>,
    proposals: HashMap<BlockId, InMemoryBlockState>,
    /// InMemoryState doesn't have access to an execution engine. It returns
    /// `max_account_balance` as the balance every time so txn fee balance check
    /// will pass if the sum doesn't exceed the max account balance
    max_account_balance: Balance,
    execution_delay: SeqNum,

    /// can be used to mess with eth-header execution results
    pub extra_data: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryBlockState {
    block_id: BlockId,
    seq_num: SeqNum,
    round: Round,
    parent_id: BlockId,
    nonces: BTreeMap<Address, Nonce>,
    balances: Option<BTreeMap<Address, Balance>>,
}

impl InMemoryBlockState {
    pub fn genesis(nonces: BTreeMap<Address, Nonce>) -> Self {
        Self {
            block_id: GENESIS_BLOCK_ID,
            seq_num: GENESIS_SEQ_NUM,
            round: GENESIS_ROUND,
            parent_id: GENESIS_BLOCK_ID,
            nonces,
            balances: None,
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
            extra_data: 0,
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
            extra_data: 0,
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
        parent_id: BlockId,
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
        let mut last_state_nonces = if let Some(parent_state) = self.proposals.get(&parent_id) {
            parent_state.nonces.clone()
        } else {
            let last_committed_entry = self
                .commits
                .last_entry()
                .expect("last_commit doesn't exist");
            let last_committed_state = last_committed_entry.get();
            assert_eq!(last_committed_state.block_id, parent_id);
            last_committed_state.nonces.clone()
        };

        for (address, account_nonce) in new_account_nonces {
            last_state_nonces.insert(address, account_nonce);
        }

        self.proposals.insert(
            block_id,
            InMemoryBlockState {
                block_id,
                seq_num,
                round,
                parent_id,
                nonces: last_state_nonces,
                balances: None,
            },
        );
    }

    fn ledger_commit(&mut self, block_id: &BlockId) {
        let committed_proposal = self.proposals.remove(block_id).unwrap_or_else(|| {
            panic!(
                "committed proposal that doesn't exist, block_id={:?}",
                block_id
            )
        });
        self.proposals
            .retain(|_, proposal| proposal.round >= committed_proposal.round);
        let (_, last_commit) = self
            .commits
            .last_key_value()
            .expect("last_commit doesn't exist");
        assert_eq!(last_commit.seq_num + SeqNum(1), committed_proposal.seq_num);
        assert_eq!(last_commit.block_id, committed_proposal.parent_id);
        trace!(
            "ledger_commit insert block_id: {:?}, proposal_seq_num: {:?}, proposal: {:?}",
            block_id,
            committed_proposal.seq_num,
            committed_proposal
        );
        self.commits
            .insert(committed_proposal.seq_num, committed_proposal);
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
            let Some(proposal) = self.proposals.get(block_id) else {
                trace!(?seq_num, ?block_id, ?is_finalized, "NotAvailableYet");
                return Err(StateBackendError::NotAvailableYet);
            };
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

    fn get_execution_result(
        &self,
        _block_id: &BlockId,
        seq_num: &SeqNum,
        _is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        // TODO make this mock less trivial
        Ok(EthHeader(Header {
            number: seq_num.0,
            gas_limit: self.extra_data,
            ..Default::default()
        }))
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

    fn total_db_lookups(&self) -> u64 {
        0
    }
}
