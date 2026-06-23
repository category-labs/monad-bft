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
    collections::{BTreeMap, HashSet, VecDeque},
    time::Duration,
};

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS,
    transaction::{Recovered, SignerRecoverable},
    Transaction, EMPTY_OMMER_ROOT_HASH,
};
use alloy_primitives::{Address, U256};
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
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_eth_block_policy::{
    compute_txn_max_gas_cost, timestamp_ns_to_secs, EthBlockPolicy, EthBlockPolicyBlockValidator,
    EthValidatedBlock,
};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason, EthTxPoolSnapshot};
use monad_eth_types::{
    AccountKey, EthBlockBody, EthExecutionProtocol, EthTxEnvelope, ExtractEthAddress,
    NamespaceTransactionBatch, NamespacedTx, ProposedEthHeader,
};
use monad_execution_state_read::{ExecutionStateRead, ExecutionStateReadError};
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
use self::{
    sequencer::{Proposal, ProposalSequencer},
    tracked::TrackedTxMap,
    transaction::PoolTx,
};
use crate::EthTxPoolEventTracker;

mod config;
mod sequencer;
mod tracked;
mod transaction;

#[derive(Clone, Debug)]
pub struct ProposalWithSenderGas<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub proposed_execution_inputs: ProposedExecutionInputs<EthExecutionProtocol>,
    pub sender_gas: BTreeMap<NodeId<CertificateSignaturePubKey<ST>>, u64>,
}

#[derive(Clone, Debug)]
pub struct EthTxPool<ST, SCT, ESRT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ESRT: ExecutionStateRead<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    tracked: TrackedTxMap<ST, SCT, ESRT, CCT, CRT>,
    namespace_batches: VecDeque<PoolNamespaceBatch<CertificateSignaturePubKey<ST>>>,

    last_commit: Option<ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>>,

    chain_id: u64,
    chain_revision: CRT,
    execution_revision: MonadExecutionRevision,
}

#[derive(Clone, Debug)]
struct PoolNamespaceBatch<PT: PubKey> {
    batch: NamespaceTransactionBatch,
    txs: Vec<PoolTx<PT>>,
    total_gas: u64,
    total_size: u64,
}

impl<ST, SCT, ESRT, CCT, CRT> EthTxPool<ST, SCT, ESRT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ESRT: ExecutionStateRead<ST, SCT>,
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
            namespace_batches: VecDeque::new(),

            last_commit: None,

            chain_id,
            chain_revision,
            execution_revision,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tracked.is_empty() && self.namespace_batches.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.tracked.num_txs()
            + self
                .namespace_batches
                .iter()
                .map(|batch| batch.txs.len())
                .sum::<usize>()
    }

    pub fn current_revision(&self) -> (&CRT, &MonadExecutionRevision) {
        (&self.chain_revision, &self.execution_revision)
    }

    pub fn insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_read: &mut ESRT,
        chain_config: &CCT,
        txs: Vec<(
            Recovered<EthTxEnvelope>,
            PoolTxKind<CertificateSignaturePubKey<ST>>,
        )>,
        mut on_insert: impl FnMut(&PoolTx<CertificateSignaturePubKey<ST>>),
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

        let account_balance_keys = txs.iter().map(PoolTx::account_key).collect_vec();

        let account_balances = match block_policy.compute_account_base_balances(
            block_seq_num,
            state_read,
            chain_config,
            None,
            account_balance_keys.iter(),
        ) {
            Ok(account_balances) => account_balances,
            Err(err) => {
                warn!(
                    ?err,
                    "failed to insert transactions at account_balance lookups"
                );
                event_tracker.drop_all(
                    txs.into_iter().map(PoolTx::into_raw),
                    EthTxPoolDropReason::Internal(
                        EthTxPoolInternalDropReason::ExecutionStateReadError,
                    ),
                );
                return;
            }
        };

        let last_commit_base_fee = last_commit.execution_inputs.base_fee_per_gas;

        let txs = txs
            .into_iter()
            .filter(|tx| {
                if account_balances
                    .get(&tx.account_key())
                    .is_none_or(|account_balance_state| {
                        account_balance_state.balance
                            < compute_txn_max_gas_cost(tx.raw().inner(), last_commit_base_fee)
                    })
                {
                    event_tracker.drop(tx.hash(), EthTxPoolDropReason::InsufficientBalance);
                    return false;
                }

                true
            })
            .into_group_map_by(|tx| tx.account_key());

        let account_nonce_keys = txs.keys().cloned().collect_vec();

        let mut account_nonces = match block_policy.get_account_base_nonces(
            block_seq_num,
            state_read,
            &vec![],
            account_nonce_keys.iter(),
        ) {
            Ok(account_nonces) => account_nonces,
            Err(err) => {
                warn!(
                    ?err,
                    "failed to insert transactions at account_nonce lookups"
                );
                event_tracker.drop_all(
                    txs.into_values().flatten().map(PoolTx::into_raw),
                    EthTxPoolDropReason::Internal(
                        EthTxPoolInternalDropReason::ExecutionStateReadError,
                    ),
                );
                return;
            }
        };

        for (account_key, txs) in txs {
            let Some(account_nonce) = account_nonces.remove(&account_key) else {
                event_tracker.drop_all(
                    txs.into_iter().map(PoolTx::into_raw),
                    EthTxPoolDropReason::Internal(
                        EthTxPoolInternalDropReason::ExecutionStateReadError,
                    ),
                );
                continue;
            };

            self.tracked.try_insert_txs(
                event_tracker,
                last_commit,
                account_key,
                txs,
                account_nonce,
                &mut on_insert,
            );
        }

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn insert_namespace_batches(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_read: &mut ESRT,
        chain_config: &CCT,
        batches: Vec<(NamespaceTransactionBatch, U256, Vec<u8>)>,
        mut on_insert: impl FnMut(&PoolTx<CertificateSignaturePubKey<ST>>),
    ) {
        let Some(last_commit) = self.last_commit.as_ref() else {
            for (batch, _, _) in batches {
                for tx in batch.transactions.iter() {
                    event_tracker.drop(*tx.tx_hash(), EthTxPoolDropReason::PoolNotReady);
                }
            }
            return;
        };

        let chain_params = self.chain_revision.chain_params();
        let execution_params = self.execution_revision.execution_chain_params();

        'batch: for (batch, priority, extra_data) in batches {
            if batch.transactions.is_empty() {
                continue;
            }

            if batch.recover_signer().is_err() {
                for tx in batch.transactions.iter() {
                    event_tracker.drop(*tx.tx_hash(), EthTxPoolDropReason::InvalidSignature);
                }
                continue;
            }

            let mut namespace = None;
            for tx in batch.transactions.iter() {
                let tx_namespace = match tx.namespace(self.chain_id) {
                    Ok(Some(namespace)) => namespace,
                    Ok(None) => {
                        for tx in batch.transactions.iter() {
                            event_tracker.drop(
                                *tx.tx_hash(),
                                EthTxPoolDropReason::NotWellFormed(
                                    monad_eth_block_policy::validation::StaticValidationError::InvalidChainId {
                                        tx_chain_id: tx.chain_id().unwrap_or(self.chain_id),
                                    },
                                ),
                            );
                        }
                        continue 'batch;
                    }
                    Err(monad_eth_types::WrongChainId::InvalidNamespaceSuffix {
                        tx_chain_id,
                        ..
                    }) => {
                        for tx in batch.transactions.iter() {
                            event_tracker.drop(
                                *tx.tx_hash(),
                                EthTxPoolDropReason::NotWellFormed(
                                    monad_eth_block_policy::validation::StaticValidationError::InvalidChainId {
                                        tx_chain_id,
                                    },
                                ),
                            );
                        }
                        continue 'batch;
                    }
                };

                if namespace.is_some_and(|namespace| namespace != tx_namespace) {
                    for tx in batch.transactions.iter() {
                        event_tracker.drop(
                            *tx.tx_hash(),
                            EthTxPoolDropReason::NotWellFormed(
                                monad_eth_block_policy::validation::StaticValidationError::InvalidChainId {
                                    tx_chain_id: tx.chain_id().unwrap_or(self.chain_id),
                                },
                            ),
                        );
                    }
                    continue 'batch;
                }
                namespace = Some(tx_namespace);
            }

            let mut pool_txs = Vec::with_capacity(batch.transactions.len());
            let mut total_gas = 0_u64;
            let mut total_size = 0_u64;

            for tx in batch.transactions.iter().cloned() {
                let recovered_tx = match tx.recover_signer() {
                    Ok(signer) => Recovered::new_unchecked(tx, signer),
                    Err(_) => {
                        for tx in batch.transactions.iter() {
                            event_tracker
                                .drop(*tx.tx_hash(), EthTxPoolDropReason::InvalidSignature);
                        }
                        continue 'batch;
                    }
                };

                let pool_tx = match PoolTx::validate(
                    last_commit,
                    self.chain_id,
                    chain_params,
                    execution_params,
                    recovered_tx,
                    PoolTxKind::Owned {
                        priority,
                        extra_data: extra_data.clone(),
                    },
                ) {
                    Ok(pool_tx) => pool_tx,
                    Err((_tx, drop_reason)) => {
                        for tx in batch.transactions.iter() {
                            event_tracker.drop(*tx.tx_hash(), drop_reason);
                        }
                        continue 'batch;
                    }
                };

                total_gas = total_gas.saturating_add(pool_tx.gas_limit());
                total_size = total_size.saturating_add(pool_tx.size());
                pool_txs.push(pool_tx);
            }

            let block_seq_num = block_policy.get_last_commit() + SeqNum(1);
            let account_balance_keys = pool_txs.iter().map(PoolTx::account_key).collect_vec();
            let account_balances = match block_policy.compute_account_base_balances(
                block_seq_num,
                state_read,
                chain_config,
                None,
                account_balance_keys.iter(),
            ) {
                Ok(account_balances) => account_balances,
                Err(err) => {
                    warn!(
                        ?err,
                        "failed to insert namespace batch at account_balance lookups"
                    );
                    for tx in batch.transactions.iter() {
                        event_tracker.drop(
                            *tx.tx_hash(),
                            EthTxPoolDropReason::Internal(
                                EthTxPoolInternalDropReason::ExecutionStateReadError,
                            ),
                        );
                    }
                    continue;
                }
            };

            let last_commit_base_fee = last_commit.execution_inputs.base_fee_per_gas;
            if pool_txs.iter().any(|tx| {
                account_balances
                    .get(&tx.account_key())
                    .is_none_or(|account_balance_state| {
                        account_balance_state.balance
                            < compute_txn_max_gas_cost(tx.raw().inner(), last_commit_base_fee)
                    })
            }) {
                for tx in batch.transactions.iter() {
                    event_tracker.drop(*tx.tx_hash(), EthTxPoolDropReason::InsufficientBalance);
                }
                continue;
            }

            for tx in pool_txs.iter() {
                on_insert(tx);
                event_tracker.insert(tx.raw(), true);
            }

            self.namespace_batches.push_back(PoolNamespaceBatch {
                batch,
                txs: pool_txs,
                total_gas,
                total_size,
            });
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
        state_read: &mut ESRT,
        chain_config: &CCT,
    ) -> Result<ProposalWithSenderGas<ST>, BlockPolicyError> {
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

        let extending_block_refs = extending_blocks.iter().collect_vec();

        let self_eth_address = node_id.pubkey().get_eth_address();
        let system_transactions = self.get_system_transactions(
            epoch,
            round,
            proposed_seq_num,
            self_eth_address,
            &extending_block_refs,
            block_policy,
            state_read,
            chain_config,
        )?;
        let system_txs_size: u64 = system_transactions
            .iter()
            .map(|tx| tx.length() as u64)
            .sum();

        let user_proposal = self.sequence_user_transactions(
            event_tracker,
            proposed_seq_num,
            base_fee,
            tx_limit - system_transactions.len(),
            proposal_gas_limit,
            proposal_byte_limit - system_txs_size,
            extending_block_refs.clone(),
            block_policy,
            state_read,
            chain_config,
        )?;
        let Proposal {
            sender_gas,
            txs: user_transactions,
            total_gas: user_total_gas,
            total_size: user_total_size,
        } = user_proposal;

        let namespace_transaction_batches = self.select_namespace_batches(
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            system_transactions.len() + user_transactions.len(),
            user_total_gas,
            system_txs_size + user_total_size,
            &extending_block_refs,
        );

        let body = EthBlockBody {
            transactions: system_transactions
                .into_iter()
                .chain(user_transactions)
                .map(|tx| tx.into_inner())
                .collect(),
            ommers: Default::default(),
            withdrawals: Default::default(),
            namespace_transaction_batches: namespace_transaction_batches.into(),
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
                &body.flattened_transactions(),
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

        Ok(ProposalWithSenderGas {
            proposed_execution_inputs: ProposedExecutionInputs { header, body },
            sender_gas,
        })
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

        let committed_hashes = committed_block
            .get_validated_txn_hashes()
            .into_iter()
            .collect::<HashSet<_>>();

        self.tracked
            .update_committed_nonce_usages(event_tracker, committed_block.nonce_usages);

        self.namespace_batches.retain(|batch| {
            if batch
                .txs
                .iter()
                .any(|tx| committed_hashes.contains(tx.hash_ref()))
            {
                event_tracker.tracked_commit(false, batch.txs.iter().map(PoolTx::hash));
                false
            } else {
                true
            }
        });

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
        self.namespace_batches.clear();

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
    ) -> Option<impl Iterator<Item = &EthTxEnvelope>> {
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
            txs: self
                .tracked
                .iter_txs()
                .map(PoolTx::hash)
                .chain(
                    self.namespace_batches
                        .iter()
                        .flat_map(|batch| batch.txs.iter().map(PoolTx::hash)),
                )
                .collect(),
        }
    }

    pub fn generate_sender_snapshot(&self) -> Vec<AccountKey> {
        self.tracked
            .iter_txs()
            .map(PoolTx::account_key)
            .unique()
            .collect()
    }

    fn get_system_transactions(
        &self,
        proposed_epoch: Epoch,
        proposed_round: Round,
        proposed_seq_num: SeqNum,
        block_author: Address,
        extending_blocks: &Vec<&EthValidatedBlock<ST, SCT>>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_read: &mut ESRT,
        chain_config: &impl ChainConfig<CRT>,
    ) -> Result<Vec<Recovered<EthTxEnvelope>>, ExecutionStateReadError> {
        // TODO this should be inside SystemTransactionGenerator to prevent
        // exposing SYSTEM_SENDER_ETH_ADDRESS outside the crate
        let system_sender_key = AccountKey::global(SYSTEM_SENDER_ETH_ADDRESS);
        let next_system_txn_nonce = *block_policy
            .get_account_base_nonces(
                proposed_seq_num,
                state_read,
                extending_blocks,
                [system_sender_key].iter(),
            )?
            .get(&system_sender_key)
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
            proposed_round,
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

    fn sequence_user_transactions(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        proposed_seq_num: SeqNum,
        base_fee: u64,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        block_policy: &EthBlockPolicy<ST, SCT, CCT, CRT>,
        state_read: &mut ESRT,
        chain_config: &CCT,
    ) -> Result<Proposal<ST>, BlockPolicyError> {
        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, "txpool create_proposal");
        });

        let Some(last_commit) = self.last_commit.as_ref() else {
            error!("txpool create_proposal called before last committed block set");
            return Ok(Proposal::default());
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
            return Ok(Proposal::default());
        }

        if tx_limit == 0 {
            warn!("txpool create_proposal called with zero tx_limit");
            return Ok(Proposal::default());
        }

        let sequencer =
            ProposalSequencer::new(self.tracked.iter(), &extending_blocks, base_fee, tx_limit);
        let sequencer_len = sequencer.len();

        if sequencer.is_empty() {
            return Ok(Proposal::default());
        }

        let (account_balances, state_read_lookups) = {
            let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
                debug!(
                    ?elapsed,
                    "txpool create_proposal compute account base balances"
                );
            });

            let total_db_lookups_before = state_read.total_db_lookups();

            (
                block_policy.compute_account_base_balances(
                    proposed_seq_num,
                    state_read,
                    chain_config,
                    Some(&extending_blocks),
                    sequencer.addresses(),
                )?,
                state_read.total_db_lookups() - total_db_lookups_before,
            )
        };

        info!(
            addresses = self.tracked.num_addresses(),
            num_txs = self.tracked.num_txs(),
            sequencer_len,
            account_balances = account_balances.len(),
            ?state_read_lookups,
            "txpool sequencing transactions"
        );

        let validator = EthBlockPolicyBlockValidator::new(
            proposed_seq_num,
            block_policy.get_execution_delay(),
            base_fee,
            &self.chain_revision,
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
            state_read_lookups,
            proposal_num_txs,
        );

        info!(
            ?proposed_seq_num,
            ?proposal_num_txs,
            proposal_total_gas = proposal.total_gas,
            "created proposal"
        );

        Ok(proposal)
    }

    fn select_namespace_batches(
        &self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        mut current_tx_count: usize,
        mut current_gas: u64,
        mut current_size: u64,
        extending_blocks: &[&EthValidatedBlock<ST, SCT>],
    ) -> Vec<NamespaceTransactionBatch> {
        let mut selected = Vec::new();
        let extending_hashes = extending_blocks
            .iter()
            .flat_map(|block| block.get_validated_txn_hashes())
            .collect::<HashSet<_>>();

        for batch in self.namespace_batches.iter() {
            if batch
                .txs
                .iter()
                .any(|tx| extending_hashes.contains(tx.hash_ref()))
            {
                continue;
            }
            if current_tx_count
                .checked_add(batch.txs.len())
                .is_none_or(|tx_count| tx_count > tx_limit)
            {
                continue;
            }
            if current_gas
                .checked_add(batch.total_gas)
                .is_none_or(|gas| gas > proposal_gas_limit)
            {
                continue;
            }
            if current_size
                .checked_add(batch.total_size)
                .is_none_or(|size| size > proposal_byte_limit)
            {
                continue;
            }

            current_tx_count += batch.txs.len();
            current_gas += batch.total_gas;
            current_size += batch.total_size;
            selected.push(batch.batch.clone());
        }

        selected
    }
}

impl<ST, SCT, ESRT> EthTxPool<ST, SCT, ESRT, MockChainConfig, MockChainRevision>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    ESRT: ExecutionStateRead<ST, SCT>,
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
