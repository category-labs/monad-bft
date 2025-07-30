use std::time::Duration;

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS, transaction::Recovered, SignableTransaction, TxEnvelope,
    TxLegacy, EMPTY_OMMER_ROOT_HASH,
};
use alloy_primitives::{hex, Address, FixedBytes, TxKind, B256};
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use itertools::Itertools;
use monad_consensus_types::{block::ProposedExecutionInputs, payload::RoundSignature};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_txpool_types::{EthTxPoolDropReason, EthTxPoolInternalDropReason, EthTxPoolSnapshot};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ProposedEthHeader, BASE_FEE_PER_GAS};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{Epoch, SeqNum};
use monad_validator::signature_collection::SignatureCollection;
use tracing::{info, warn};

use self::{pending::PendingTxMap, tracked::TrackedTxMap, transaction::ValidEthTransaction};
use crate::EthTxPoolEventTracker;

mod pending;
mod tracked;
mod transaction;

// Key used by all consensus nodes to sign and verify system transactions
const SYSTEM_TRANSACTIONS_PRIV_KEY: B256 = B256::new(hex!(
    "b0358e6d701a955d9926676f227e40172763296b317ff554e49cdf2c2c35f8a7"
));
const SYSTEM_TRANSACTIONS_ETH_ADDRESS: Address =
    Address::new(hex!("0x6f49a8F621353f12378d0046E7d7e4b9B249DC9e"));

// System transactions related to staking precompile
const STAKING_CONTRACT_ADDRESS: Address =
    Address::new(hex!("0x0000000000000000000000000000000000001000"));
const EPOCH_CHANGE_IDENTIFIER: FixedBytes<4> = FixedBytes::new(hex!("0x00000000"));
const SNAPSHOT_IDENTIFIER: FixedBytes<4> = FixedBytes::new(hex!("0x00000001"));
const REWARD_IDENTIFIER: FixedBytes<4> = FixedBytes::new(hex!("0x00000002"));

// This constants controls the maximum number of addresses that get promoted during the tx insertion
// process. It was set based on intuition and should be changed once we have more data on txpool
// performance.
// Each account lookup takes about 30us so this should block the thread for at most roughly 8ms.
const INSERT_TXS_MAX_PROMOTE: usize = 256;
const PENDING_MAX_PROMOTE: usize = 128;

#[derive(Clone, Debug)]
pub struct EthTxPool<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    do_local_insert: bool,
    pending: PendingTxMap,
    tracked: TrackedTxMap<ST, SCT, SBT>,
    // current proposal_gas_limit. On insert_tx, we validate that tx.gas_limit
    // <= proposal_gas_limit to reject anything that can't possibly fit in a
    // block. Create proposal doesn't rely on this value
    proposal_gas_limit: u64,
    val_set_update_interval: SeqNum,
    chain_id: u64,

    max_code_size: usize,
}

impl<ST, SCT, SBT> EthTxPool<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    pub fn new(
        do_local_insert: bool,
        soft_tx_expiry: Duration,
        hard_tx_expiry: Duration,
        proposal_gas_limit: u64,
        val_set_update_interval: SeqNum,
        chain_id: u64,
        max_code_size: usize,
    ) -> Self {
        Self {
            do_local_insert,
            pending: PendingTxMap::default(),
            tracked: TrackedTxMap::new(soft_tx_expiry, hard_tx_expiry),
            proposal_gas_limit,
            val_set_update_interval,
            chain_id,
            max_code_size,
        }
    }

    pub fn default_testing() -> Self {
        const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
        const VAL_SET_UPDATE_INTERVAL: SeqNum = SeqNum(100);
        const CHAIN_ID: u64 = 1337;
        const MAX_CODE_SIZE: usize = 0x6000;
        Self::new(
            true,
            Duration::from_secs(60),
            Duration::from_secs(60),
            PROPOSAL_GAS_LIMIT,
            VAL_SET_UPDATE_INTERVAL,
            CHAIN_ID,
            MAX_CODE_SIZE,
        )
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

    pub fn set_tx_gas_limit(&mut self, proposal_gas_limit: u64) {
        self.proposal_gas_limit = proposal_gas_limit
    }

    pub fn set_max_code_size(&mut self, max_code_size: usize) {
        self.max_code_size = max_code_size
    }

    pub fn insert_txs(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &SBT,
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
                    block_policy,
                    self.proposal_gas_limit,
                    self.max_code_size,
                    tx,
                    owned,
                    last_commit,
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

        for tx in txs {
            let account_balance = account_balances
                .get(tx.signer_ref())
                .cloned()
                .unwrap_or_default();

            let Some(_new_account_balance) = tx.apply_max_value(account_balance) else {
                event_tracker.drop(tx.hash(), EthTxPoolDropReason::InsufficientBalance);
                continue;
            };

            let Some(tx) = self
                .tracked
                .try_insert_tx(event_tracker, tx)
                .unwrap_or_else(|tx| self.pending.try_insert_tx(event_tracker, tx))
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
        block_policy: &EthBlockPolicy<ST, SCT>,
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
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        beneficiary: [u8; 20],
        timestamp_ns: u128,
        epoch: Epoch,
        round_signature: RoundSignature<SCT::SignatureType>,
        extending_blocks: Vec<EthValidatedBlock<ST, SCT>>,

        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &SBT,
    ) -> Result<ProposedExecutionInputs<EthExecutionProtocol>, StateBackendError> {
        info!(
            ?proposed_seq_num,
            ?tx_limit,
            ?proposal_gas_limit,
            ?proposal_byte_limit,
            "txpool creating proposal"
        );

        self.tracked.evict_expired_txs(event_tracker);

        let timestamp_seconds = timestamp_ns / 1_000_000_000;
        // u64::MAX seconds is ~500 Billion years
        assert!(timestamp_seconds < u64::MAX.into());

        let mut system_transactions = Vec::new();
        if proposed_seq_num.is_epoch_end(self.val_set_update_interval) {
            system_transactions.push(self.generate_snapshot_tx());
        } else if extending_blocks
            .last()
            .is_some_and(|parent_block| parent_block.block.get_epoch() != epoch)
        {
            system_transactions.push(self.generate_epoch_change_tx(epoch));
        }
        system_transactions.push(self.generate_reward_tx(&beneficiary));

        let transactions = self.tracked.create_proposal(
            event_tracker,
            proposed_seq_num,
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            block_policy,
            extending_blocks.iter().collect(),
            system_transactions,
            state_backend,
            &mut self.pending,
        )?;

        let body = EthBlockBody {
            transactions: transactions.into_iter().map(|tx| tx.into_tx()).collect(),
            ommers: Vec::new(),
            withdrawals: Vec::new(),
        };
        let extra_data = if proposed_seq_num.is_epoch_end(self.val_set_update_interval) {
            [u8::MAX; 32]
        } else {
            [0_u8; 32]
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
            extra_data,
            base_fee_per_gas: BASE_FEE_PER_GAS,
            blob_gas_used: 0,
            excess_blob_gas: 0,
            parent_beacon_block_root: [0_u8; 32],
        };

        self.update_aggregate_metrics(event_tracker);

        Ok(ProposedExecutionInputs { header, body })
    }

    pub fn update_committed_block(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        committed_block: EthValidatedBlock<ST, SCT>,
    ) {
        self.tracked
            .update_committed_block(event_tracker, committed_block, &mut self.pending);

        self.tracked.evict_expired_txs(event_tracker);

        self.update_aggregate_metrics(event_tracker);
    }

    pub fn get_forwardable_txs<const MIN_SEQNUM_DIFF: u64, const MAX_RETRIES: usize>(
        &mut self,
    ) -> Option<impl Iterator<Item = &TxEnvelope>> {
        let last_commit_seq_num = self.tracked.last_commit()?.seq_num;

        Some(
            self.pending
                .iter_mut_txs()
                .chain(self.tracked.iter_mut_txs())
                .filter_map(move |tx| {
                    tx.get_if_forwardable::<MIN_SEQNUM_DIFF, MAX_RETRIES>(last_commit_seq_num)
                }),
        )
    }

    pub fn reset(
        &mut self,
        event_tracker: &mut EthTxPoolEventTracker<'_>,
        last_delay_committed_blocks: Vec<EthValidatedBlock<ST, SCT>>,
    ) {
        self.tracked.reset(last_delay_committed_blocks);

        self.update_aggregate_metrics(event_tracker);
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

    fn sign_system_tx(tx: TxLegacy) -> Recovered<TxEnvelope> {
        let signer = PrivateKeySigner::from_bytes(&SYSTEM_TRANSACTIONS_PRIV_KEY).unwrap();
        let signature = signer
            .sign_hash_sync(&tx.signature_hash())
            .unwrap();
        let signed = tx.into_signed(signature);

        Recovered::new_unchecked(TxEnvelope::Legacy(signed), SYSTEM_TRANSACTIONS_ETH_ADDRESS)
    }

    fn generate_epoch_change_tx(&self, new_epoch: Epoch) -> Recovered<TxEnvelope> {
        let mut input = [0_u8; 12];
        input[0..4].copy_from_slice(EPOCH_CHANGE_IDENTIFIER.as_slice());
        input[5..12].copy_from_slice(&new_epoch.0.to_be_bytes());

        let transaction = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value: Default::default(),
            input: input.into(),
        };

        Self::sign_system_tx(transaction)
    }

    fn generate_snapshot_tx(&self) -> Recovered<TxEnvelope> {
        let transaction = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value: Default::default(),
            input: SNAPSHOT_IDENTIFIER.into(),
        };

        Self::sign_system_tx(transaction)
    }

    fn generate_reward_tx(&self, beneficiary: &[u8; 20]) -> Recovered<TxEnvelope> {
        let mut input = [0_u8; 24];
        input[0..4].copy_from_slice(REWARD_IDENTIFIER.as_slice());
        input[5..24].copy_from_slice(beneficiary);

        let transaction = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce: 0,
            gas_price: 0,
            gas_limit: 0,
            to: TxKind::Call(STAKING_CONTRACT_ADDRESS),
            value: Default::default(),
            input: input.into(),
        };

        Self::sign_system_tx(transaction)
    }
}
