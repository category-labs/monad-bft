use std::u64;

use alloy_consensus::{constants::EMPTY_WITHDRAWALS, EMPTY_OMMER_ROOT_HASH};
use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::{Either, Itertools};
use monad_consensus_types::{
    block::ProposedExecutionInputs,
    payload::{
        EthBlockBody, EthExecutionProtocol, ProposedEthHeader, RoundSignature, BASE_FEE_PER_GAS,
        PROPOSAL_GAS_LIMIT,
    },
    signature_collection::SignatureCollection,
    txpool::{TxPool, TxPoolInsertionError},
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_types::{Balance, EthAddress, TxEnvelopeWithSigner};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::warn;

use crate::{pending::PendingTxMap, tracked::TrackedTxMap, transaction::ValidEthTransaction};

mod pending;
mod tracked;
mod transaction;

const MAX_PROPOSAL_SIZE: usize = 10_000;
const INSERT_TXS_MIN_PROMOTE: usize = 32;
const INSERT_TXS_MAX_PROMOTE: usize = 128;

#[derive(Clone, Debug)]
pub struct EthTxPool<ST, SCT, SBT> {
    do_local_insert: bool,
    pending: PendingTxMap,
    tracked: TrackedTxMap<ST, SCT, SBT>,
}

impl<ST, SCT, SBT> Default for EthTxPool<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    fn default() -> Self {
        Self::new(true)
    }
}

impl<ST, SCT, SBT> EthTxPool<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn new(do_local_insert: bool) -> Self {
        Self {
            do_local_insert,
            pending: PendingTxMap::default(),
            tracked: TrackedTxMap::default(),
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

    pub fn num_pending_txns(&self) -> usize {
        self.pending.num_txs()
    }

    pub fn num_tracked_txns(&self) -> usize {
        self.tracked.num_txs()
    }

    pub fn promote_pending(
        &mut self,
        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &SBT,
        max_promotable: usize,
    ) -> Result<(), StateBackendError> {
        self.tracked.promote_pending(
            block_policy,
            state_backend,
            &mut self.pending,
            max_promotable,
        )
    }

    fn validate_and_insert_tx(
        &mut self,
        tx: TxEnvelopeWithSigner,
        block_policy: &EthBlockPolicy<ST, SCT>,
        account_balance: &Balance,
    ) -> Result<(), TxPoolInsertionError> {
        if !self.do_local_insert {
            return Ok(());
        }

        let tx = ValidEthTransaction::validate(tx, block_policy)?;
        tx.apply_txn_fee(account_balance)?;

        // TODO(andr-dev): Should any additional tx validation occur before inserting into mempool

        match self.tracked.try_add_tx(tx) {
            Either::Left(tx) => self.pending.try_add_tx(tx),
            Either::Right(result) => result,
        }
    }

    fn validate_and_insert_txs(
        &mut self,
        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &SBT,
        txs: Vec<TxEnvelopeWithSigner>,
    ) -> Result<Vec<Result<(), TxPoolInsertionError>>, StateBackendError> {
        let senders = txs.iter().map(|tx| EthAddress(tx.signer)).collect_vec();

        let block_seq_num = block_policy.get_last_commit() + SeqNum(1); // ?????
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
    ) -> Vec<Bytes> {
        if let Err(state_backend_error) = self.promote_pending(
            block_policy,
            state_backend,
            txns.len()
                .min(INSERT_TXS_MIN_PROMOTE)
                .max(INSERT_TXS_MAX_PROMOTE),
        ) {
            if self.pending.is_at_promote_txs_watermark() {
                warn!(
                    ?state_backend_error,
                    "txpool failed to promote at pending promote txs watermark"
                );
            }
        }

        // TODO(rene): sender recovery is done inline here
        let (decoded_txs, raw_txs): (Vec<_>, Vec<_>) = txns
            .into_par_iter()
            .filter_map(|b| {
                TxEnvelopeWithSigner::decode(&mut b.as_ref())
                    .ok()
                    .map(|valid_tx| (valid_tx, b))
            })
            .unzip();

        let Ok(insertion_results) =
            self.validate_and_insert_txs(block_policy, state_backend, decoded_txs)
        else {
            // can't insert, state backend is delayed
            return Vec::new();
        };

        insertion_results
            .into_iter()
            .zip(raw_txs)
            .filter_map(|(insertion_result, b)| match insertion_result {
                Ok(()) => Some(b),
                Err(_) => None,
            })
            .collect::<Vec<_>>()
    }

    fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        beneficiary: &EthAddress,
        timestamp_ns: u128,
        round_signature: &RoundSignature<SCT::SignatureType>,

        block_policy: &EthBlockPolicy<ST, SCT>,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        state_backend: &SBT,
    ) -> Result<ProposedExecutionInputs<EthExecutionProtocol>, StateBackendError> {
        self.tracked.evict_expired_txs();

        let timestamp_seconds = timestamp_ns / 1_000_000_000;
        // u64::MAX seconds is ~500 Billion years
        assert!(timestamp_seconds < u64::MAX.into());

        let transactions = self.tracked.create_proposal(
            proposed_seq_num,
            tx_limit.min(MAX_PROPOSAL_SIZE),
            block_policy,
            extending_blocks,
            state_backend,
            &mut self.pending,
        )?;

        let body = EthBlockBody {
            transactions: transactions
                .into_iter()
                .map(|tx_with_signer| tx_with_signer.transaction)
                .collect(),
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

            beneficiary: beneficiary.0,
            difficulty: 0,
            number: proposed_seq_num.0,
            gas_limit: PROPOSAL_GAS_LIMIT,
            timestamp: timestamp_seconds as u64,
            mix_hash: round_signature.get_hash().0,
            nonce: [0_u8; 8],
            extra_data: [0_u8; 32],
            base_fee_per_gas: BASE_FEE_PER_GAS,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
        };

        Ok(ProposedExecutionInputs { header, body })
    }

    fn update_committed_block(&mut self, committed_block: &EthValidatedBlock<ST, SCT>) {
        self.tracked
            .update_committed_block(committed_block, &mut self.pending);

        self.tracked.evict_expired_txs();
    }

    fn clear(&mut self) {
        self.tracked.evict_expired_txs();
    }

    fn reset(&mut self, last_delay_committed_blocks: Vec<&EthValidatedBlock<ST, SCT>>) {
        self.tracked.reset(last_delay_committed_blocks);
    }
}
