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

use std::time::Duration;

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, transaction::Recovered, TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use alloy_primitives::Address;
use alloy_rlp::Encodable;
use itertools::Itertools;
use monad_chain_config::{
    execution_revision::MonadExecutionRevision,
    revision::{ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_consensus_types::{
    block::{BlockPolicyError, ProposedExecutionInputs},
    payload::RoundSignature,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{timestamp_ns_to_secs, EthBlockPolicy, EthValidatedBlock};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason, EthTxPoolSnapshot};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ExtractEthAddress, ProposedEthHeader};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_system_calls::{SystemTransactionGenerator, SYSTEM_SENDER_ETH_ADDRESS};
use monad_types::{Balance, Epoch, NodeId, Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::{debug, info, warn};

use self::{pending::PendingTxMap, tracked::TrackedTxMap, transaction::ValidEthTransaction};
use crate::EthTxPoolEventTracker;

mod pending;
mod tracked;
mod transaction;

// This constants controls the maximum number of addresses that get promoted during the tx insertion
// process. It was set based on intuition and should be changed once we have more data on txpool
// performance.
// Each account lookup takes about 30us so this should block the thread for at most roughly 8ms.
const INSERT_TXS_MAX_PROMOTE: usize = 256;
const PENDING_MAX_PROMOTE: usize = 128;

#[derive(Clone, Debug)]
pub struct EthTxPool<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pending: PendingTxMap,
    tracked: TrackedTxMap<ST, SCT, SBT, CCT, CRT>,

    chain_id: u64,
    chain_revision: CRT,
    execution_revision: MonadExecutionRevision,

    do_local_insert: bool,
}

impl<ST, SCT, SBT, CCT, CRT> EthTxPool<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
{
    pub fn new(
        soft_tx_expiry: Duration,
        hard_tx_expiry: Duration,
        chain_id: u64,
        chain_revision: CRT,
        execution_revision: MonadExecutionRevision,
        do_local_insert: bool,
    ) -> Self {
        Self {
            pending: PendingTxMap::default(),
            tracked: TrackedTxMap::new(soft_tx_expiry, hard_tx_expiry),

            chain_id,
            chain_revision,
            execution_revision,

            do_local_insert,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.tracked.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.pending
            .num_txs()
            .checked_add(self.tracked.num_txs())
            .expect("pool size does not overflow")
    }

    pub fn insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        chain_config: &CCT,
        txs: Vec<Recovered<TxEnvelope>>,
        owned: bool,
        mut on_insert: impl FnMut(&ValidEthTransaction),
    ) {
        if !self.do_local_insert {
            event_tracker.drop_all(txs.into_iter(), EthTxPoolDropReason::PoolNotReady);
            return;
        }

        let Some(last_commit) = self.tracked.last_commit() else {
            event_tracker.drop_all(txs.into_iter(), EthTxPoolDropReason::PoolNotReady);
            return;
        };

        let txs = txs
            .into_iter()
            .filter_map(|tx| {
                ValidEthTransaction::validate(
                    event_tracker,
                    last_commit,
                    self.chain_id,
                    self.chain_revision.chain_params(),
                    self.execution_revision.execution_chain_params(),
                    tx,
                    owned,
                )
            })
            .collect_vec();

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup at N-k. As a fix, we add 1 so the seqnum is on the edge of
        // the range at N-k+1.
        let block_seq_num = block_policy.get_last_commit() + SeqNum(1);

        let addresses = txs.iter().map(ValidEthTransaction::signer).collect_vec();

        let account_balances = match block_policy.compute_account_base_balances(
            block_seq_num,
            state_backend,
            chain_config,
            None,
            addresses.iter(),
        ) {
            Ok(account_balances) => account_balances,
            Err(err) => {
                warn!(?err, "failed to insert transactions");
                event_tracker.drop_all(
                    txs.into_iter().map(ValidEthTransaction::into_raw),
                    EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError),
                );
                return;
            }
        };

        let last_commit_base_fee = last_commit.execution_inputs.base_fee_per_gas;

        for tx in txs {
            if account_balances
                .get(tx.signer_ref())
                .is_none_or(|x| x.balance == Balance::ZERO)
            {
                event_tracker.drop(tx.hash(), EthTxPoolDropReason::InsufficientBalance);
                continue;
            }

            let Some(tx) = self
                .tracked
                .try_insert_tx(event_tracker, tx)
                .unwrap_or_else(|tx| {
                    self.pending
                        .try_insert_tx(event_tracker, tx, last_commit_base_fee)
                })
            else {
                continue;
            };

            on_insert(tx);
        }

        if !self.tracked.try_promote_pending(
            event_tracker,
            block_policy,
            state_backend,
            &mut self.pending,
            0,
            INSERT_TXS_MAX_PROMOTE,
        ) && self.pending.is_at_promote_txs_watermark()
        {
            warn!("txpool failed to promote at pending promote txs watermark");
        }

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn promote_pending(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
    ) {
        if !self.tracked.try_promote_pending(
            event_tracker,
            block_policy,
            state_backend,
            &mut self.pending,
            0,
            PENDING_MAX_PROMOTE,
        ) {
            warn!("txpool failed to promote during promote_pending call");
        }
    }

    pub fn create_proposal(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        epoch: Epoch,
        round: Round,
        proposed_seq_num: SeqNum,
        base_fee: u64,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        beneficiary: [u8; 20],
        timestamp_ns: u128,
        node_id: NodeId<CertificateSignaturePubKey<ST>>,
        round_signature: RoundSignature<SCT::SignatureType>,
        extending_blocks: Vec<EthValidatedBlock<ST, SCT>>,

        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        chain_config: &CCT,
    ) -> Result<ProposedExecutionInputs<EthExecutionProtocol>, BlockPolicyError> {
        info!(
            ?proposed_seq_num,
            ?tx_limit,
            ?proposal_gas_limit,
            ?proposal_byte_limit,
            "txpool creating proposal"
        );

        self.tracked.evict_expired_txs(event_tracker);

        let timestamp_seconds = timestamp_ns_to_secs(timestamp_ns);
        let execution_revision = chain_config.get_execution_chain_revision(timestamp_seconds);

        {
            let chain_id = chain_config.chain_id();

            if self.chain_id != chain_id {
                panic!(
                    "txpool chain id changed from {} to {} in create_proposal",
                    self.chain_id, chain_id
                );
            }

            let chain_revision = chain_config.get_chain_revision(round);

            if chain_revision.chain_params() != self.chain_revision.chain_params()
                || self.execution_revision != execution_revision
            {
                self.chain_revision = chain_revision;
                self.execution_revision = execution_revision;

                info!(
                    chain_params =? chain_revision.chain_params(),
                    execution_revision =? execution_revision,
                    "updating chain params and execution revision in create_proposal"
                );

                self.static_validate_all_txs(event_tracker);
            }
        }

        let self_eth_address = node_id.pubkey().get_eth_address();
        let system_transactions = self.get_system_transactions(
            epoch,
            proposed_seq_num,
            self_eth_address,
            &extending_blocks.iter().collect(),
            block_policy,
            state_backend,
            chain_config,
        )?;
        let system_txs_size: u64 = system_transactions
            .iter()
            .map(|tx| tx.length() as u64)
            .sum();

        let user_transactions = self.tracked.create_proposal(
            event_tracker,
            self.chain_id,
            proposed_seq_num,
            base_fee,
            tx_limit - system_transactions.len(),
            proposal_gas_limit,
            proposal_byte_limit - system_txs_size,
            block_policy,
            extending_blocks.iter().collect(),
            state_backend,
            chain_config,
            &mut self.pending,
            &self.chain_revision,
            &self.execution_revision,
        )?;

        let body = EthBlockBody {
            transactions: system_transactions
                .into_iter()
                .chain(user_transactions)
                .map(|tx| tx.into_tx())
                .collect(),
            ommers: Vec::new(),
            withdrawals: Vec::new(),
        };

        let maybe_request_hash = if execution_revision.execution_chain_params().prague_enabled {
            Some([0_u8; 32])
        } else {
            None
        };

        let header = ProposedEthHeader {
            transactions_root: *alloy_consensus::proofs::calculate_transaction_root(
                &body.transactions,
            ),
            ommers_hash: {
                assert_eq!(body.ommers.len(), 0);
                *EMPTY_OMMER_ROOT_HASH
            },
            withdrawals_root: {
                assert_eq!(body.withdrawals.len(), 0);
                *EMPTY_WITHDRAWALS
            },

            beneficiary: beneficiary.into(),
            difficulty: 0,
            number: proposed_seq_num.0,
            gas_limit: proposal_gas_limit,
            timestamp: timestamp_seconds,
            mix_hash: round_signature.get_hash().0,
            nonce: [0_u8; 8],
            extra_data: [0_u8; 32],
            base_fee_per_gas: base_fee,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
            requests_hash: maybe_request_hash,
        };

        self.update_aggregate_metrics(event_tracker);

        Ok(ProposedExecutionInputs { header, body })
    }

    pub fn enter_round(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_config: &impl ChainConfig<CRT>,
        round: Round,
    ) {
        let chain_id = chain_config.chain_id();

        if self.chain_id != chain_id {
            panic!(
                "txpool chain id changed from {} to {}",
                self.chain_id, chain_id
            );
        }

        let chain_revision = chain_config.get_chain_revision(round);

        if chain_revision.chain_params() != self.chain_revision.chain_params() {
            self.chain_revision = chain_revision;
            info!(chain_params =? self.chain_revision.chain_params(), "updating chain revision");

            self.static_validate_all_txs(event_tracker);
        }
    }

    pub fn update_committed_block(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_config: &impl ChainConfig<CRT>,
        committed_block: EthValidatedBlock<ST, SCT>,
    ) {
        let execution_revision = chain_config
            .get_execution_chain_revision(committed_block.header().execution_inputs.timestamp);

        self.tracked
            .update_committed_block(event_tracker, committed_block, &mut self.pending);

        self.tracked.evict_expired_txs(event_tracker);

        if self.execution_revision != execution_revision {
            self.execution_revision = execution_revision;
            info!(execution_revision =? self.execution_revision, "updating execution revision");

            self.static_validate_all_txs(event_tracker);
        }

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn reset(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_config: &impl ChainConfig<CRT>,
        last_delay_committed_blocks: Vec<EthValidatedBlock<ST, SCT>>,
    ) {
        let execution_revision = chain_config.get_execution_chain_revision(
            last_delay_committed_blocks
                .last()
                .map_or(0, |committed_block| {
                    committed_block.header().execution_inputs.timestamp
                }),
        );

        self.tracked.reset(last_delay_committed_blocks);

        if self.execution_revision != execution_revision {
            self.execution_revision = execution_revision;
            info!(execution_revision =? self.execution_revision, "updating execution revision");

            self.static_validate_all_txs(event_tracker);
        }

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn static_validate_all_txs(&mut self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        self.tracked.static_validate_all_txs(
            event_tracker,
            self.chain_id,
            &self.chain_revision,
            &self.execution_revision,
        );
        self.pending.static_validate_all_txs(
            event_tracker,
            self.chain_id,
            &self.chain_revision,
            &self.execution_revision,
        );
    }

    pub fn get_forwardable_txs<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        &mut self,
    ) -> Option<impl Iterator<Item = &TxEnvelope>> {
        let last_commit = self.tracked.last_commit()?;

        let last_commit_seq_num = last_commit.seq_num;
        let last_commit_base_fee = last_commit.execution_inputs.base_fee_per_gas;

        Some(
            self.pending
                .iter_mut_txs()
                .chain(self.tracked.iter_mut_txs())
                .filter_map(move |tx| {
                    tx.get_if_forwardable::<MIN_SEQNUM_DIFF, MAX_RETRIES>(
                        last_commit_seq_num,
                        last_commit_base_fee,
                    )
                }),
        )
    }

    fn update_aggregate_metrics(&self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        event_tracker.update_aggregate_metrics(
            self.pending.num_addresses() as u64,
            self.pending.num_txs() as u64,
            self.tracked.num_addresses() as u64,
            self.tracked.num_txs() as u64,
        );
    }

    pub fn generate_snapshot(&self) -> EthTxPoolSnapshot {
        EthTxPoolSnapshot {
            pending: self
                .pending
                .iter_txs()
                .map(ValidEthTransaction::hash)
                .collect(),
            tracked: self
                .tracked
                .iter_txs()
                .map(ValidEthTransaction::hash)
                .collect(),
        }
    }

    pub fn generate_sender_snapshot(&self) -> Vec<Address> {
        self.tracked
            .iter_txs()
            .map(ValidEthTransaction::signer)
            .chain(self.pending.iter_txs().map(ValidEthTransaction::signer))
            .unique()
            .collect()
    }

    fn get_system_transactions(
        &self,
        proposed_epoch: Epoch,
        proposed_seq_num: SeqNum,
        block_author: Address,
        extending_blocks: &Vec<&EthValidatedBlock<ST, SCT>>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        chain_config: &impl ChainConfig<CRT>,
    ) -> Result<Vec<Recovered<TxEnvelope>>, StateBackendError> {
        // TODO this should be inside SystemTransactionGenerator to prevent
        // exposing SYSTEM_SENDER_ETH_ADDRESS outside the crate
        let next_system_txn_nonce = *block_policy
            .get_account_base_nonces(
                proposed_seq_num,
                state_backend,
                extending_blocks,
                [SYSTEM_SENDER_ETH_ADDRESS].iter(),
            )?
            .get(&SYSTEM_SENDER_ETH_ADDRESS)
            .unwrap();

        let parent_block_epoch = {
            if let Some(extending_block) = extending_blocks.last() {
                extending_block.get_epoch()
            } else {
                assert_eq!(proposed_seq_num, block_policy.get_last_commit() + SeqNum(1));
                block_policy.get_last_commit_epoch()
            }
        };

        let sys_txns = SystemTransactionGenerator::generate_system_transactions(
            proposed_seq_num,
            proposed_epoch,
            parent_block_epoch,
            block_author,
            next_system_txn_nonce,
            chain_config,
        );

        debug!(
            ?proposed_seq_num,
            ?sys_txns,
            "generated system transactions"
        );

        Ok(sys_txns
            .into_iter()
            .map(|sys_txn| sys_txn.into())
            .collect_vec())
    }
}

impl<ST, SCT, SBT> EthTxPool<ST, SCT, SBT, MockChainConfig, MockChainRevision>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
{
    pub fn default_testing() -> Self {
        Self::new(
            Duration::from_secs(60),
            Duration::from_secs(60),
            MockChainConfig::DEFAULT.chain_id(),
            MockChainRevision::DEFAULT,
            MonadExecutionRevision::LATEST,
            true,
        )
    }
}
