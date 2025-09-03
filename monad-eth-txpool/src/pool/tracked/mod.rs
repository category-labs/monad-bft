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

use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::Address;
use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use itertools::Itertools;
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
    nonce_usage::{NonceUsage, NonceUsageRetrievable},
    EthBlockPolicy, EthBlockPolicyBlockValidator, EthValidatedBlock,
};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason};
use monad_eth_types::EthExecutionProtocol;
use monad_state_backend::StateBackend;
use monad_types::{DropTimer, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::{debug, error, info, warn};

use self::{list::TrackedTxList, sequencer::ProposalSequencer};
use super::{
    pending::{PendingTxList, PendingTxMap},
    transaction::ValidEthTransaction,
};
use crate::EthTxPoolEventTracker;

mod list;
mod sequencer;

// To produce 10k tx blocks, we need the tracked tx map to hold at least 20k addresses so that if
// the block in the pending blocktree has 10k txs with 10k unique addresses that are also in the
// tracked tx map then we still have 10k other addresses to use when creating the next block.
const MAX_ADDRESSES: usize = 20 * 1024;

// Tx batches from rpc can contain up to roughly 500 transactions. Since we don't evict based on how
// many txs are in the pool, we need to ensure that after eviction there is always space for all 500
// txs.
const SOFT_EVICT_ADDRESSES_WATERMARK: usize = MAX_ADDRESSES - 512;

// TODO(andr-dev): This currently limits the number of unique addresses in a
// proposal. This will be removed once we move the txpool into its own thread.
const MAX_PROMOTABLE_ON_CREATE_PROPOSAL: usize = 1024 * 10;

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

    /// Produces a reference to the tx if it was inserted, producing None when the tx signer was
    /// tracked but the tx was not inserted. If the tx signer is not tracked or the tracked pool is
    /// not ready to accept txs, an error is produced with the original tx.
    pub fn try_insert_tx(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        tx: ValidEthTransaction,
    ) -> Result<Option<&ValidEthTransaction>, ValidEthTransaction> {
        let Some(last_commit) = self.last_commit.as_ref() else {
            return Err(tx);
        };

        let Some(tx_list) = self.txs.get_mut(tx.signer_ref()) else {
            return Err(tx);
        };

        Ok(tx_list.try_insert_tx(
            event_tracker,
            tx,
            last_commit.execution_inputs.base_fee_per_gas,
            self.hard_tx_expiry,
        ))
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
        pending: &mut PendingTxMap,
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

        if !self.try_promote_pending(
            event_tracker,
            block_policy,
            state_backend,
            pending,
            0,
            MAX_PROMOTABLE_ON_CREATE_PROPOSAL,
        ) {
            error!("txpool failed to promote pending txs during create_proposal");
        }

        if self.txs.is_empty() || tx_limit == 0 {
            return Ok(Vec::new());
        }

        let sequencer = ProposalSequencer::new(
            &self.txs,
            &extending_blocks,
            chain_config.chain_id(),
            base_fee,
        );
        let sequencer_len = sequencer.len();

        let authority_addresses = sequencer.authority_addresses().cloned().collect_vec();

        let (mut account_balances, authority_nonces, state_backend_lookups) = {
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
                block_policy.get_account_base_nonces(
                    proposed_seq_num,
                    state_backend,
                    &extending_blocks,
                    authority_addresses.iter(),
                )?,
                state_backend.total_db_lookups() - total_db_lookups_before,
            )
        };

        info!(
            addresses = self.txs.len(),
            num_txs = self.num_txs(),
            sequencer_len,
            account_balances = account_balances.len(),
            authority_nonces = authority_nonces.len(),
            ?state_backend_lookups,
            "txpool sequencing transactions"
        );

        for authority in authority_addresses.iter() {
            let Some(account_balance) = account_balances.get_mut(&authority) else {
                continue;
            };

            account_balance.is_delegated = true;
        }

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
            authority_nonces,
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

    pub fn try_promote_pending(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
        min_promotable: usize,
        max_promotable: usize,
    ) -> bool {
        let Some(insertable) = MAX_ADDRESSES.checked_sub(self.txs.len()) else {
            return false;
        };

        if insertable < min_promotable {
            return true;
        }

        let insertable = insertable.min(max_promotable);

        if insertable == 0 {
            return true;
        }

        let to_insert = pending.split_off(insertable);

        if to_insert.is_empty() {
            return true;
        }

        let Some(last_commit) = &self.last_commit else {
            warn!("txpool attempted to promote pending before first committed block");
            event_tracker.drop_all(
                to_insert
                    .into_values()
                    .map(PendingTxList::into_map)
                    .flat_map(BTreeMap::into_values)
                    .map(ValidEthTransaction::into_raw),
                EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::NotReady),
            );
            return false;
        };
        let last_commit_seq_num = last_commit.seq_num;

        let addresses = to_insert.len();
        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, addresses, "txpool promote_pending")
        });

        let addresses = to_insert.keys().cloned().collect_vec();

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup outside that range. As a fix, we add 1 so the seqnum is on
        // the edge of the range.
        let account_nonces = match block_policy.get_account_base_nonces(
            last_commit_seq_num + SeqNum(1),
            state_backend,
            &Vec::default(),
            addresses.iter(),
        ) {
            Ok(account_nonces) => account_nonces,
            Err(err) => {
                warn!(
                    ?err,
                    "failed to lookup account nonces during promote pending"
                );
                event_tracker.drop_all(
                    to_insert
                        .into_values()
                        .map(PendingTxList::into_map)
                        .flat_map(BTreeMap::into_values)
                        .map(ValidEthTransaction::into_raw),
                    EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError),
                );
                return false;
            }
        };

        for (address, pending_tx_list) in to_insert {
            let Some(account_nonce) = account_nonces.get(&address) else {
                error!("txpool address missing from state backend");

                event_tracker
                    .pending_drop_unknown(pending_tx_list.into_map().values().map(|tx| tx.hash()));

                continue;
            };

            match self.txs.entry(address) {
                IndexMapEntry::Occupied(_) => {
                    unreachable!("pending address present in tracked map")
                }
                IndexMapEntry::Vacant(v) => {
                    let Some(tracked_tx_list) = TrackedTxList::new_from_promote_pending(
                        event_tracker,
                        *account_nonce,
                        pending_tx_list,
                    ) else {
                        continue;
                    };

                    v.insert(tracked_tx_list);
                }
            }
        }

        true
    }

    pub fn update_committed_block(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        committed_block: EthValidatedBlock<ST, SCT>,
        pending: &mut PendingTxMap,
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

        let mut insertable = MAX_ADDRESSES.saturating_sub(self.txs.len());

        for (address, nonce_usage) in committed_block.get_nonce_usages().into_map() {
            match self.txs.entry(address) {
                IndexMapEntry::Occupied(tx_list) => {
                    TrackedTxList::update_committed_nonce_usage(event_tracker, tx_list, nonce_usage)
                }
                IndexMapEntry::Vacant(v) => match nonce_usage {
                    NonceUsage::Possible(_) => continue,
                    NonceUsage::Known(nonce) => {
                        if insertable == 0 {
                            continue;
                        }

                        let Some(pending_tx_list) = pending.remove(&address) else {
                            continue;
                        };

                        let account_nonce = nonce
                            .checked_add(1)
                            .expect("account nonce does not overflow");

                        let Some(tracked_tx_list) = TrackedTxList::new_from_promote_pending(
                            event_tracker,
                            account_nonce,
                            pending_tx_list,
                        ) else {
                            continue;
                        };

                        insertable -= 1;

                        v.insert(tracked_tx_list);
                    }
                },
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
}
