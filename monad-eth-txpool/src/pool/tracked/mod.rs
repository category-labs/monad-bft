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

use std::{marker::PhantomData, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::Address;
use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use monad_chain_config::{
    execution_revision::MonadExecutionRevision, revision::ChainRevision, ChainConfig,
};
use monad_consensus_types::block::{
    BlockPolicyBlockValidator, BlockPolicyError, ConsensusBlockHeader,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    nonce_usage::NonceUsageRetrievable, EthBlockPolicy, EthBlockPolicyBlockValidator,
    EthValidatedBlock,
};
use monad_eth_txpool_types::EthTxPoolDropReason;
use monad_eth_types::EthExecutionProtocol;
use monad_state_backend::StateBackend;
use monad_types::{DropTimer, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::{debug, error, info};

use self::{list::TrackedTxList, sequencer::ProposalSequencer};
use super::transaction::ValidEthTransaction;
use crate::EthTxPoolEventTracker;

mod list;
mod sequencer;

// To produce 5k tx blocks, we need the tracked tx map to hold at least 15k addresses so that, after
// pruning the txpool of up to 5k unique addresses in the last committed block update and up to 5k
// unique addresses in the pending blocktree, the tracked tx map will still have at least 5k other
// addresses with at least one tx each to use when creating the next block.
const MAX_ADDRESSES: usize = 16 * 1024;

// Tx batches from rpc can contain up to roughly 500 transactions. Since we don't evict based on how
// many txs are in the pool, we need to ensure that after eviction there is always space for all 500
// txs.
const SOFT_EVICT_ADDRESSES_WATERMARK: usize = MAX_ADDRESSES - 512;

/// Stores transactions using a "snapshot" system by which each address has an associated
/// account_nonce stored in the TrackedTxList which is guaranteed to be the correct
/// account_nonce for the seqnum stored in last_commit_seq_num.
#[derive(Clone, Debug)]
pub struct TrackedTxMap<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    last_commit: Option<ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>>,
    soft_tx_expiry: Duration,
    hard_tx_expiry: Duration,

    // By using IndexMap, we can iterate through the map with Vec-like performance and are able to
    // evict expired txs through the entry API.
    txs: IndexMap<Address, TrackedTxList>,

    _phantom: PhantomData<(SBT, CCT, CRT)>,
}

impl<ST, SCT, SBT, CCT, CRT> TrackedTxMap<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new(soft_tx_expiry: Duration, hard_tx_expiry: Duration) -> Self {
        Self {
            last_commit: None,
            soft_tx_expiry,
            hard_tx_expiry,

            txs: IndexMap::with_capacity(MAX_ADDRESSES),

            _phantom: PhantomData,
        }
    }

    pub fn last_commit(&self) -> Option<&ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>> {
        self.last_commit.as_ref()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_addresses(&self) -> usize {
        self.txs.len()
    }

    pub fn num_txs(&self) -> usize {
        self.txs.values().map(TrackedTxList::num_txs).sum()
    }

    pub fn iter_txs(&self) -> impl Iterator<Item = &ValidEthTransaction> {
        self.txs.values().flat_map(TrackedTxList::iter)
    }

    pub fn iter_mut_txs(&mut self) -> impl Iterator<Item = &mut ValidEthTransaction> {
        self.txs.values_mut().flat_map(TrackedTxList::iter_mut)
    }

    pub fn try_insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        address: Address,
        txs: Vec<ValidEthTransaction>,
        account_nonce: u64,
        on_insert: &mut impl FnMut(&ValidEthTransaction),
    ) {
        let Some(last_commit) = self.last_commit.as_ref() else {
            error!("txpool tracked map asked to insert tx when not ready");
            event_tracker.drop_all(
                txs.into_iter().map(ValidEthTransaction::into_raw),
                EthTxPoolDropReason::PoolNotReady,
            );
            return;
        };

        match self.txs.entry(address) {
            IndexMapEntry::Occupied(o) => {
                let tx_list = o.into_mut();

                for tx in txs {
                    if let Some(tx) = tx_list.try_insert_tx(
                        event_tracker,
                        tx,
                        last_commit.execution_inputs.base_fee_per_gas,
                        self.hard_tx_expiry,
                    ) {
                        on_insert(tx);
                    }
                }
            }
            IndexMapEntry::Vacant(v) => {
                TrackedTxList::try_new(
                    v,
                    event_tracker,
                    txs,
                    account_nonce,
                    on_insert,
                    last_commit.execution_inputs.base_fee_per_gas,
                    self.hard_tx_expiry,
                );
            }
        }
    }

    pub fn create_proposal(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_id: u64,
        proposed_seq_num: SeqNum,
        base_fee: u64,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        state_backend: &SBT,
        chain_config: &CCT,
        chain_revision: &CRT,
        execution_revision: &MonadExecutionRevision,
    ) -> Result<Vec<Recovered<TxEnvelope>>, BlockPolicyError> {
        let Some(last_commit) = &self.last_commit else {
            return Ok(Vec::new());
        };
        let last_commit_seq_num = last_commit.seq_num;

        assert!(
            block_policy.get_last_commit().ge(&last_commit_seq_num),
            "txpool received block policy with lower committed seq num"
        );

        if last_commit_seq_num != block_policy.get_last_commit() {
            error!(
                block_policy_last_commit = block_policy.get_last_commit().0,
                txpool_last_commit = last_commit_seq_num.0,
                "last commit update does not match block policy last commit"
            );

            return Ok(Vec::new());
        }

        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, "txpool create_proposal");
        });

        if self.txs.is_empty() || tx_limit == 0 {
            return Ok(Vec::new());
        }

        let sequencer = ProposalSequencer::new(&self.txs, &extending_blocks, base_fee, tx_limit);
        let sequencer_len = sequencer.len();

        let (account_balances, state_backend_lookups) = {
            let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
                debug!(
                    ?elapsed,
                    "txpool create_proposal compute account base balances"
                );
            });

            let total_db_lookups_before = state_backend.total_db_lookups();

            (
                block_policy.compute_account_base_balances(
                    proposed_seq_num,
                    state_backend,
                    chain_config,
                    Some(&extending_blocks),
                    sequencer.addresses(),
                )?,
                state_backend.total_db_lookups() - total_db_lookups_before,
            )
        };

        info!(
            addresses = self.txs.len(),
            num_txs = self.num_txs(),
            sequencer_len,
            account_balances = account_balances.len(),
            ?state_backend_lookups,
            "txpool sequencing transactions"
        );

        let validator = EthBlockPolicyBlockValidator::new(
            proposed_seq_num,
            block_policy.get_execution_delay(),
            base_fee,
            chain_revision,
            execution_revision,
        )?;

        let proposal = sequencer.build_proposal(
            chain_id,
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            chain_config,
            account_balances,
            validator,
        );

        let proposal_num_txs = proposal.txs.len();

        event_tracker.record_create_proposal(
            self.num_addresses(),
            sequencer_len,
            state_backend_lookups,
            proposal_num_txs,
        );

        info!(
            ?proposed_seq_num,
            ?proposal_num_txs,
            proposal_total_gas = proposal.total_gas,
            "created proposal"
        );

        Ok(proposal.txs)
    }

    pub fn update_committed_block(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        committed_block: EthValidatedBlock<ST, SCT>,
    ) {
        {
            let seqnum = committed_block.get_seq_num();
            debug!(?seqnum, "txpool updating committed block");
        }

        if let Some(last_commit) = &self.last_commit {
            assert_eq!(
                committed_block.get_seq_num(),
                last_commit.seq_num + SeqNum(1),
                "txpool received out of order committed block"
            );
        }
        self.last_commit = Some(committed_block.header().clone());

        for (address, nonce_usage) in committed_block.get_nonce_usages().into_map() {
            match self.txs.entry(address) {
                IndexMapEntry::Occupied(tx_list) => {
                    TrackedTxList::update_committed_nonce_usage(event_tracker, tx_list, nonce_usage)
                }
                IndexMapEntry::Vacant(_) => {}
            }
        }
    }

    pub fn evict_expired_txs(&mut self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        let num_txs = self.num_txs();

        let tx_expiry = if num_txs < SOFT_EVICT_ADDRESSES_WATERMARK {
            self.hard_tx_expiry
        } else {
            info!(?num_txs, "txpool hit soft evict addresses watermark");
            self.soft_tx_expiry
        };

        let mut idx = 0;

        loop {
            if idx >= self.txs.len() {
                break;
            }

            let Some(entry) = self.txs.get_index_entry(idx) else {
                break;
            };

            if TrackedTxList::evict_expired_txs(event_tracker, entry, tx_expiry) {
                continue;
            }

            idx += 1;
        }
    }

    pub fn reset(&mut self, last_delay_committed_blocks: Vec<EthValidatedBlock<ST, SCT>>) {
        self.txs.clear();
        self.last_commit = last_delay_committed_blocks
            .last()
            .map(|block| block.header().clone())
    }

    pub fn static_validate_all_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_id: u64,
        chain_revision: &CRT,
        execution_revision: &MonadExecutionRevision,
    ) {
        self.txs.retain(|_, tx_list| {
            tx_list.static_validate_all_txs(
                event_tracker,
                chain_id,
                chain_revision,
                execution_revision,
            )
        });
    }
}
