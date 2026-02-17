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
use itertools::{Either, Itertools};
use monad_chain_config::{
    execution_revision::MonadExecutionRevision,
    revision::{ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_consensus_types::{
    block::{BlockPolicyError, ConsensusBlockHeader, ProposedExecutionInputs},
    payload::RoundSignature,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_txn_max_gas_cost, timestamp_ns_to_secs, EthBlockPolicy, EthBlockPolicyBlockValidator,
    EthValidatedBlock,
};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason, EthTxPoolSnapshot};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ExtractEthAddress, ProposedEthHeader};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_system_calls::{SystemTransactionGenerator, SYSTEM_SENDER_ETH_ADDRESS};
use monad_types::{DropTimer, Epoch, NodeId, Round, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{debug, error, info, warn};

pub use self::{
    config::EthTxPoolConfig,
    tracked::TrackedTxLimitsConfig,
    transaction::{max_eip2718_encoded_length, PoolTxKind},
};
use self::{sequencer::ProposalSequencer, tracked::TrackedTxMap, transaction::PoolTx};
use crate::EthTxPoolEventTracker;

mod config;
mod sequencer;
mod tracked;
mod transaction;

#[derive(Clone, Debug)]
pub struct EthTxPool<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    tracked: TrackedTxMap<ST, SCT, SBT, CCT, CRT>,

    last_commit: Option<ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>>,

    chain_id: u64,
    chain_revision: CRT,
    execution_revision: MonadExecutionRevision,
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
        config: EthTxPoolConfig,
        chain_id: u64,
        chain_revision: CRT,
        execution_revision: MonadExecutionRevision,
    ) -> Self {
        let EthTxPoolConfig {
            limits: config_limits,
        } = config;

        Self {
            tracked: TrackedTxMap::new(config_limits),

            last_commit: None,

            chain_id,
            chain_revision,
            execution_revision,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tracked.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.tracked.num_txs()
    }

    pub fn current_revision(&self) -> (&CRT, &MonadExecutionRevision) {
        (&self.chain_revision, &self.execution_revision)
    }

    pub fn insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        chain_config: &CCT,
        txs: Vec<(Recovered<TxEnvelope>, PoolTxKind)>,
        mut on_insert: impl FnMut(&PoolTx),
    ) {
        let Some(last_commit) = self.last_commit.as_ref() else {
            event_tracker.drop_all(
                txs.into_iter().map(|(tx, _)| tx),
                EthTxPoolDropReason::PoolNotReady,
            );
            return;
        };

        let chain_params = self.chain_revision.chain_params();
        let execution_params = self.execution_revision.execution_chain_params();

        let (txs, invalid_txs): (Vec<_>, Vec<_>) =
            txs.into_par_iter().partition_map(|(tx, kind)| {
                Either::from(PoolTx::validate(
                    last_commit,
                    self.chain_id,
                    chain_params,
                    execution_params,
                    tx,
                    kind,
                ))
                .flip()
            });

        for (tx, drop_reason) in invalid_txs {
            event_tracker.drop(*tx.tx_hash(), drop_reason);
        }

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup at N-k. As a fix, we add 1 so the seqnum is on the edge of
        // the range at N-k+1.
        let block_seq_num = block_policy.get_last_commit() + SeqNum(1);

        let account_balance_addresses = txs.iter().map(PoolTx::signer).collect_vec();

        let account_balances = match block_policy.compute_account_base_balances(
            block_seq_num,
            state_backend,
            chain_config,
            None,
            account_balance_addresses.iter(),
        ) {
            Ok(account_balances) => account_balances,
            Err(err) => {
                warn!(
                    ?err,
                    "failed to insert transactions at account_balance lookups"
                );
                event_tracker.drop_all(
                    txs.into_iter().map(PoolTx::into_raw),
                    EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError),
                );
                return;
            }
        };

        let last_commit_base_fee = last_commit.execution_inputs.base_fee_per_gas;

        let txs = txs
            .into_iter()
            .filter(|tx| {
                if account_balances
                    .get(tx.signer_ref())
                    .is_none_or(|account_balance_state| {
                        account_balance_state.balance
                            < compute_txn_max_gas_cost(tx.raw(), last_commit_base_fee)
                    })
                {
                    event_tracker.drop(tx.hash(), EthTxPoolDropReason::InsufficientBalance);
                    return false;
                }

                true
            })
            .into_group_map_by(|tx| tx.signer());

        let account_nonce_addresses = txs.keys().cloned().collect_vec();

        let mut account_nonces = match block_policy.get_account_base_nonces(
            block_seq_num,
            state_backend,
            &vec![],
            account_nonce_addresses.iter(),
        ) {
            Ok(account_nonces) => account_nonces,
            Err(err) => {
                warn!(
                    ?err,
                    "failed to insert transactions at account_nonce lookups"
                );
                event_tracker.drop_all(
                    txs.into_values().flatten().map(PoolTx::into_raw),
                    EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError),
                );
                return;
            }
        };

        for (address, txs) in txs {
            let Some(account_nonce) = account_nonces.remove(&address) else {
                event_tracker.drop_all(
                    txs.into_iter().map(PoolTx::into_raw),
                    EthTxPoolDropReason::Internal(EthTxPoolInternalDropReason::StateBackendError),
                );
                continue;
            };

            self.tracked.try_insert_txs(
                event_tracker,
                last_commit,
                address,
                txs,
                account_nonce,
                &mut on_insert,
            );
        }

        self.update_aggregate_metrics(event_tracker);
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

        {
            let chain_id = chain_config.chain_id();

            if self.chain_id != chain_id {
                panic!(
                    "txpool chain id changed from {} to {} in create_proposal",
                    self.chain_id, chain_id
                );
            }

            let chain_revision = chain_config.get_chain_revision(round);
            let execution_revision = chain_config.get_execution_chain_revision(timestamp_seconds);

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

        let user_transactions = self.sequence_user_transactions(
            event_tracker,
            proposed_seq_num,
            base_fee,
            tx_limit - system_transactions.len(),
            proposal_gas_limit,
            proposal_byte_limit - system_txs_size,
            extending_blocks.iter().collect(),
            block_policy,
            state_backend,
            chain_config,
        )?;

        let body = EthBlockBody {
            transactions: system_transactions
                .into_iter()
                .chain(user_transactions)
                .map(|tx| tx.into_inner())
                .collect(),
            ommers: Vec::new(),
            withdrawals: Vec::new(),
        };

        // Monad does not use request hashes yet
        // It is hardcoded to zero hash for prague compatibility
        let maybe_request_hash = if self
            .execution_revision
            .execution_chain_params()
            .prague_enabled
        {
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
        {
            let seqnum = committed_block.get_seq_num();
            debug!(?seqnum, "txpool updating committed block");
        }

        if let Some(last_commit) = self.last_commit.as_ref() {
            assert_eq!(
                committed_block.get_seq_num(),
                last_commit.seq_num + SeqNum(1),
                "txpool received out of order committed block"
            );
        }
        self.last_commit = Some(committed_block.header().clone());

        let execution_revision = chain_config
            .get_execution_chain_revision(committed_block.header().execution_inputs.timestamp);

        if self.execution_revision != execution_revision {
            self.execution_revision = execution_revision;
            info!(execution_revision =? self.execution_revision, "updating execution revision");

            self.static_validate_all_txs(event_tracker);
        }

        self.tracked
            .update_committed_nonce_usages(event_tracker, committed_block.nonce_usages);

        self.tracked.evict_expired_txs(event_tracker);

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn reset(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        chain_config: &impl ChainConfig<CRT>,
        last_delay_committed_blocks: Vec<EthValidatedBlock<ST, SCT>>,
    ) {
        self.last_commit = last_delay_committed_blocks
            .last()
            .map(|block| block.header().clone());

        let execution_revision = chain_config.get_execution_chain_revision(
            last_delay_committed_blocks
                .last()
                .map_or(0, |committed_block| {
                    committed_block.header().execution_inputs.timestamp
                }),
        );

        if self.execution_revision != execution_revision {
            self.execution_revision = execution_revision;
            info!(execution_revision =? self.execution_revision, "updating execution revision");

            self.static_validate_all_txs(event_tracker);
        }

        self.tracked.reset();

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn static_validate_all_txs(&mut self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        self.tracked.static_validate_all_txs(
            event_tracker,
            self.chain_id,
            &self.chain_revision,
            &self.execution_revision,
        );
    }

    pub fn get_forwardable_txs<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        &mut self,
    ) -> Option<impl Iterator<Item = &TxEnvelope>> {
        let last_commit = self.last_commit.as_ref()?;

        let last_commit_seq_num = last_commit.seq_num;
        let last_commit_base_fee = last_commit.execution_inputs.base_fee_per_gas;

        Some(self.tracked.iter_mut_txs().filter_map(move |tx| {
            tx.get_if_forwardable::<MIN_SEQNUM_DIFF, MAX_RETRIES>(
                last_commit_seq_num,
                last_commit_base_fee,
            )
        }))
    }

    fn update_aggregate_metrics(&self, event_tracker: &mut EthTxPoolEventTracker<'_>) {
        event_tracker.update_aggregate_metrics(
            self.tracked.num_addresses() as u64,
            self.tracked.num_txs() as u64,
        );
    }

    pub fn generate_snapshot(&self) -> EthTxPoolSnapshot {
        EthTxPoolSnapshot {
            txs: self.tracked.iter_txs().map(PoolTx::hash).collect(),
        }
    }

    pub fn generate_sender_snapshot(&self) -> Vec<Address> {
        self.tracked
            .iter_txs()
            .map(PoolTx::signer)
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

    pub fn sequence_user_transactions(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        proposed_seq_num: SeqNum,
        base_fee: u64,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_backend: &SBT,
        chain_config: &CCT,
    ) -> Result<Vec<Recovered<TxEnvelope>>, BlockPolicyError> {
        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, "txpool create_proposal");
        });

        let Some(last_commit) = self.last_commit.as_ref() else {
            error!("txpool create_proposal called before last committed block set");
            return Ok(Vec::default());
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
                "txpool last commit update does not match block policy last commit"
            );
            return Ok(Vec::default());
        }

        if tx_limit == 0 {
            warn!("txpool create_proposal called with zero tx_limit");
            return Ok(Vec::default());
        }

        let sequencer =
            ProposalSequencer::new(self.tracked.iter(), &extending_blocks, base_fee, tx_limit);
        let sequencer_len = sequencer.len();

        if sequencer.is_empty() {
            return Ok(Vec::default());
        }

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
            addresses = self.tracked.num_addresses(),
            num_txs = self.tracked.num_txs(),
            sequencer_len,
            account_balances = account_balances.len(),
            ?state_backend_lookups,
            "txpool sequencing transactions"
        );

        let validator = EthBlockPolicyBlockValidator::new(
            proposed_seq_num,
            block_policy.get_execution_delay(),
            base_fee,
            &self.chain_revision,
            &self.execution_revision,
        )?;

        let proposal = sequencer.build_proposal(
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            chain_config,
            account_balances,
            validator,
        );

        let proposal_num_txs = proposal.txs.len();

        event_tracker.record_create_proposal(
            self.tracked.num_addresses(),
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
            EthTxPoolConfig {
                limits: TrackedTxLimitsConfig::new(
                    None,
                    None,
                    None,
                    None,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                ),
            },
            MockChainConfig::DEFAULT.chain_id(),
            MockChainRevision::DEFAULT,
            MonadExecutionRevision::LATEST,
        )
    }
}
