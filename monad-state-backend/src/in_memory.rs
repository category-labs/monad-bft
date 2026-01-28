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
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use alloy_consensus::{Header, Transaction, TxEnvelope};
use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{EthAccount, EthHeader};
use monad_types::{
    Balance, BlockId, Epoch, Nonce, Round, SeqNum, Stake, GENESIS_BLOCK_ID, GENESIS_ROUND,
    GENESIS_SEQ_NUM,
};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{MockExecution, StateBackend, StateBackendError};

pub type InMemoryState<ST, SCT> = Arc<Mutex<InMemoryStateInner<ST, SCT>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub balance: Balance,
    pub reserve_balance: Balance,
    pub nonce: Nonce,
    pub is_delegated: bool,
}

impl AccountState {
    pub fn max_balance() -> Self {
        Self {
            balance: Balance::MAX,
            reserve_balance: Balance::from(1000),
            nonce: 0,
            is_delegated: false,
        }
    }

    pub fn max_balance_with_nonce(nonce: u64) -> Self {
        Self {
            balance: Balance::MAX,
            reserve_balance: Balance::from(1000),
            nonce,
            is_delegated: false,
        }
    }

    pub fn new_with_balance(balance: Balance) -> Self {
        Self {
            balance,
            reserve_balance: Balance::from(1000),
            nonce: 0,
            is_delegated: false,
        }
    }

    pub fn empty_account() -> Self {
        Self {
            balance: Balance::ZERO,
            reserve_balance: Balance::from(1000),
            nonce: 0,
            is_delegated: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InMemoryStateInner<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    commits: BTreeMap<SeqNum, InMemoryBlockState>,
    proposals: HashMap<BlockId, InMemoryBlockState>,
    execution_delay: SeqNum,
    default_reserve_balance: Balance,

    /// can be used to mess with eth-header execution results
    pub extra_data: u64,

    total_mock_lookups: Arc<AtomicU64>,

    _phantom: PhantomData<(ST, SCT)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryBlockState {
    block_id: BlockId,
    seq_num: SeqNum,
    round: Round,
    parent_id: BlockId,
    /// the txns to execute for this seq_num
    txns: Vec<TxEnvelope>,
    /// account states after executing this block seq_num
    accounts: BTreeMap<Address, AccountState>,
}

impl InMemoryBlockState {
    pub fn genesis(accounts: BTreeMap<Address, AccountState>) -> Self {
        Self {
            block_id: GENESIS_BLOCK_ID,
            seq_num: GENESIS_SEQ_NUM,
            round: GENESIS_ROUND,
            parent_id: GENESIS_BLOCK_ID,
            txns: Vec::new(),
            accounts,
        }
    }
}

impl<ST, SCT> InMemoryStateInner<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn genesis(execution_delay: SeqNum) -> InMemoryState<ST, SCT> {
        Arc::new(Mutex::new(Self {
            commits: std::iter::once((
                GENESIS_SEQ_NUM,
                InMemoryBlockState::genesis(BTreeMap::new()),
            ))
            .collect(),
            proposals: Default::default(),
            execution_delay,
            default_reserve_balance: Balance::from(1000),

            extra_data: 0,
            total_mock_lookups: Arc::default(),

            _phantom: PhantomData,
        }))
    }

    pub fn new(execution_delay: SeqNum, last_commit: InMemoryBlockState) -> InMemoryState<ST, SCT> {
        Arc::new(Mutex::new(Self {
            commits: std::iter::once((last_commit.seq_num, last_commit)).collect(),
            proposals: Default::default(),
            execution_delay,
            default_reserve_balance: Balance::from(1000),

            extra_data: 0,
            total_mock_lookups: Arc::default(),

            _phantom: PhantomData,
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

impl<ST, SCT> MockExecution<ST, SCT> for InMemoryStateInner<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn ledger_propose(
        &mut self,
        block_id: BlockId,
        seq_num: SeqNum,
        round: Round,
        parent_id: BlockId,
        txns: Vec<TxEnvelope>,
    ) {
        if self
            .commits
            .get(&seq_num)
            .is_some_and(|committed| committed.block_id == block_id)
        {
            // we can repropose already-finalized blocks on startup
            // this is part of the statesync process
            return;
        }

        if seq_num
            <= self
                .raw_read_latest_finalized_block()
                .expect("latest_finalized doesn't exist")
        {
            return; //already finalized
        }

        trace!(?block_id, ?seq_num, ?round, "ledger_propose");

        // get parent block state. Execute on top of that block. Generate the new state
        let parent_state = self
            .proposals
            .get(&parent_id)
            .or_else(|| self.commits.get(&(seq_num - SeqNum(1))))
            .unwrap_or_else(|| {
                panic!(
                    "parent block not found for proposed block, proposed_seq_num={:?}, round={:?}",
                    seq_num, round
                )
            });

        let mut accounts = parent_state.accounts.clone();

        trace!(
            "block N={:?}, parent account state: {:?}",
            seq_num,
            accounts
        );

        for tx in txns.iter() {
            let addr = tx.recover_signer().expect("invalid eth tx in block");

            let account_entry = accounts.entry(addr).or_insert(AccountState {
                balance: Balance::from(100_000_000_000_000_000_u64),
                reserve_balance: self.default_reserve_balance,
                nonce: 0,
                is_delegated: false,
            });

            // validate the nonce
            if account_entry.nonce != tx.nonce() {
                panic!("tfm state backend executed invalid nonce");
            }
            account_entry.nonce += 1;

            if tx.is_eip7702() {
                let auth_list = tx
                    .authorization_list()
                    .expect("valid 7702 must have auth list");
                for tuple in auth_list {
                    if let Ok(addr) = tuple.recover_authority() {
                        let auth_account_entry = accounts.entry(addr).or_insert(AccountState {
                            balance: Balance::from(100_000_000_000_000_000_u64),
                            reserve_balance: self.default_reserve_balance,
                            nonce: 0,
                            is_delegated: false,
                        });
                        if auth_account_entry.nonce != tuple.nonce() {
                            // invalid tuples are skipped
                            continue;
                        }
                        auth_account_entry.nonce += 1;
                    };
                }
            }
        }

        self.proposals.insert(
            block_id,
            InMemoryBlockState {
                block_id,
                seq_num,
                round,
                parent_id,
                txns,
                accounts,
            },
        );
    }

    fn ledger_commit(&mut self, block_id: &BlockId, seq_num: &SeqNum) {
        if self
            .commits
            .get(seq_num)
            .is_some_and(|committed| &committed.block_id == block_id)
        {
            // we can refinalize already-finalized blocks on startup
            // this is part of the statesync process
            return;
        }

        if *seq_num
            <= self
                .raw_read_latest_finalized_block()
                .expect("latest_finalized doesn't exist")
        {
            return; //already finalized
        }

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

impl<ST, SCT> StateBackend<ST, SCT> for InMemoryStateInner<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
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
                self.total_mock_lookups.fetch_add(1, Ordering::SeqCst);
                let account = state.accounts.get(address)?;
                Some(EthAccount {
                    nonce: account.nonce,
                    balance: account.balance,
                    code_hash: None,
                    is_delegated: account.is_delegated,
                })
            })
            .collect())
    }

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        let block = if is_finalized
            && self
                .raw_read_latest_finalized_block()
                .is_some_and(|latest_seq_num| &latest_seq_num >= seq_num)
        {
            if self
                .raw_read_earliest_finalized_block()
                .is_some_and(|earliest_finalized| &earliest_finalized > seq_num)
            {
                return Err(StateBackendError::NeverAvailable);
            }
            self.commits.get(seq_num).unwrap()
        } else {
            self.proposals
                .get(block_id)
                .ok_or(StateBackendError::NotAvailableYet)?
        };

        assert_eq!(&block.block_id, block_id);
        assert_eq!(&block.seq_num, seq_num);

        Ok(EthHeader(Header {
            number: block.seq_num.0,
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

    fn read_valset_at_block(
        &self,
        _block_num: SeqNum,
        _requested_epoch: Epoch,
    ) -> Vec<(SCT::NodeIdPubKey, SignatureCollectionPubKeyType<SCT>, Stake)> {
        // TODO validator set updates for tests are done in-line in
        // MockValSetUpdater(s) right now. should be done from here instead
        unimplemented!("can't fetch validator set from InMemoryState")
    }

    fn total_db_lookups(&self) -> u64 {
        self.total_mock_lookups.load(Ordering::SeqCst)
    }
}
