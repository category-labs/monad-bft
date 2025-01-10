use std::time::Duration;

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, transaction::Recovered, TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::{Either, Itertools};
use monad_consensus_types::{
    block::ProposedExecutionInputs,
    metrics::TxPoolEvents,
    payload::RoundSignature,
    signature_collection::SignatureCollection,
    txpool::{TxPool, TxPoolInsertionError},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_types::{
    Balance, EthBlockBody, EthExecutionProtocol, ProposedEthHeader, BASE_FEE_PER_GAS,
};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::warn;

use crate::{pending::PendingTxMap, tracked::TrackedTxMap, transaction::ValidEthTransaction};

mod pending;
mod tracked;
mod transaction;

const MAX_PROPOSAL_SIZE: usize = 10_000;

// These constants control how many txs will get promoted from the pending map to the tracked map
// during other tx insertions. They were set based on intuition and should be changed once we have
// more data on txpool performance.
const INSERT_TXS_MIN_PROMOTE: usize = 32;
const INSERT_TXS_MAX_PROMOTE: usize = 128;

#[derive(Clone, Debug)]
pub struct EthTxPool<ST, SCT, SBT> {
    do_local_insert: bool,
    pending: PendingTxMap,
    tracked: TrackedTxMap<ST, SCT, SBT>,
}

impl<ST, SCT, SBT> EthTxPool<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn new(do_local_insert: bool, tx_expiry: Duration) -> Self {
        Self {
            do_local_insert,
            pending: PendingTxMap::default(),
            tracked: TrackedTxMap::new(tx_expiry),
        }
    }

    pub fn default_testing() -> Self {
        Self::new(true, Duration::from_secs(60))
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

    fn validate_and_insert_tx(
        &mut self,
        tx: Recovered<TxEnvelope>,
        block_policy: &EthBlockPolicy<ST, SCT>,
        account_balance: &Balance,
    ) -> Result<(), TxPoolInsertionError> {
        if !self.do_local_insert {
            return Ok(());
        }

        let tx = ValidEthTransaction::validate(tx, block_policy)?;
        tx.apply_max_value(account_balance)?;

        // TODO(andr-dev): Should any additional tx validation occur before inserting into mempool

        match self.tracked.try_add_tx(tx) {
            Either::Left(tx) => self.pending.try_add_tx(tx),
            Either::Right(result) => result,
        }
    }

    fn validate_and_insert_txs(
        &mut self,
        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &impl StateBackend,
        txs: Vec<Recovered<TxEnvelope>>,
    ) -> Result<Vec<Result<(), TxPoolInsertionError>>, StateBackendError> {
        let senders = txs.iter().map(|tx| tx.signer()).collect_vec();

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup outside that range. As a fix, we add 1 so the seqnum is on
        // the edge of the range.
        let block_seq_num = block_policy.get_last_commit() + SeqNum(1);

        let sender_account_balances = block_policy.compute_account_base_balances(
            block_seq_num,
            state_backend,
            None,
            senders.iter(),
        )?;

        let results = txs
            .into_iter()
            .zip(senders.iter())
            .map(|(tx, sender)| {
                self.validate_and_insert_tx(
                    tx,
                    block_policy,
                    &sender_account_balances
                        .get(&sender)
                        .cloned()
                        .unwrap_or_default(),
                )
            })
            .collect();
        Ok(results)
    }

    fn update_aggregate_metrics(&self, metrics: &mut TxPoolEvents) {
        metrics.pending_addresses = self.pending.num_addresses() as u64;
        metrics.pending_txs = self.pending.num_txs() as u64;
        metrics.tracked_addresses = self.tracked.num_addresses() as u64;
        metrics.tracked_txs = self.tracked.num_txs() as u64;
    }
}

impl<ST, SCT, SBT> TxPool<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
    for EthTxPool<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &SBT,
        metrics: &mut TxPoolEvents,
    ) -> Vec<Bytes> {
        if let Err(state_backend_error) = self.tracked.promote_pending(
            block_policy,
            state_backend,
            &mut self.pending,
            txns.len()
                .min(INSERT_TXS_MIN_PROMOTE)
                .max(INSERT_TXS_MAX_PROMOTE),
            metrics,
        ) {
            if self.pending.is_at_promote_txs_watermark() {
                warn!(
                    ?state_backend_error,
                    "txpool failed to promote at pending promote txs watermark"
                );
            }
        }

        let incoming_num_txs = txns.len();

        // TODO(rene): sender recovery is done inline here
        let (decoded_txs, raw_txs): (Vec<_>, Vec<_>) = txns
            .into_par_iter()
            .filter_map(|raw_tx| {
                let tx = TxEnvelope::decode(&mut raw_tx.as_ref()).ok()?;
                let signer = tx.recover_signer().ok()?;
                Some((Recovered::new_unchecked(tx, signer), raw_tx))
            })
            .unzip();

        metrics.drop_invalid_bytes += incoming_num_txs.saturating_sub(decoded_txs.len()) as u64;

        let Ok(insertion_results) =
            self.validate_and_insert_txs(block_policy, state_backend, decoded_txs)
        else {
            // can't insert, state backend is delayed
            return Vec::new();
        };

        let results = insertion_results
            .into_iter()
            .zip(raw_txs)
            .filter_map(|(result, b)| {
                let Some(error) = result.err() else {
                    return Some(b);
                };

                match error {
                    TxPoolInsertionError::NotWellFormed => metrics.drop_not_well_formed += 1,
                    TxPoolInsertionError::NonceTooLow => metrics.drop_nonce_too_low += 1,
                    TxPoolInsertionError::FeeTooLow => metrics.drop_fee_too_low += 1,
                    TxPoolInsertionError::InsufficientBalance => {
                        metrics.drop_insufficient_balance += 1
                    }
                    TxPoolInsertionError::PoolFull => metrics.drop_pool_full += 1,
                    TxPoolInsertionError::ExistingHigherPriority => {
                        metrics.drop_existing_higher_priority += 1
                    }
                }

                None
            })
            .collect::<Vec<_>>();

        self.update_aggregate_metrics(metrics);

        results
    }

    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        beneficiary: [u8; 20],
        timestamp_ns: u128,
        round_signature: &RoundSignature<SCT::SignatureType>,

        block_policy: &EthBlockPolicy<ST, SCT>,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        state_backend: &SBT,
        metrics: &mut TxPoolEvents,
    ) -> Result<ProposedExecutionInputs<EthExecutionProtocol>, StateBackendError> {
        self.tracked.evict_expired_txs(metrics);

        let timestamp_seconds = timestamp_ns / 1_000_000_000;
        // u64::MAX seconds is ~500 Billion years
        assert!(timestamp_seconds < u64::MAX.into());

        let transactions = self.tracked.create_proposal(
            proposed_seq_num,
            tx_limit.min(MAX_PROPOSAL_SIZE),
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
            &mut self.pending,
            metrics,
        )?;

        let body = EthBlockBody {
            transactions: transactions.into_iter().map(|tx| tx.into_tx()).collect(),
            ommers: Vec::new(),
            withdrawals: Vec::new(),
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
            timestamp: timestamp_seconds as u64,
            mix_hash: round_signature.get_hash().0,
            nonce: [0_u8; 8],
            extra_data: [0_u8; 32],
            base_fee_per_gas: BASE_FEE_PER_GAS,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
        };

        self.update_aggregate_metrics(metrics);

        Ok(ProposedExecutionInputs { header, body })
    }

    fn update_committed_block(
        &mut self,
        committed_block: &EthValidatedBlock<ST, SCT>,
        metrics: &mut TxPoolEvents,
    ) {
        self.tracked
            .update_committed_block(committed_block, &mut self.pending, metrics);

        self.tracked.evict_expired_txs(metrics);

        self.update_aggregate_metrics(metrics);
    }

    fn reset(
        &mut self,
        last_delay_committed_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        metrics: &mut TxPoolEvents,
    ) {
        self.tracked.reset(last_delay_committed_blocks);

        self.update_aggregate_metrics(metrics);
    }
}
