use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_primitives::Address;
use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use itertools::{Either, Itertools};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{DropTimer, SeqNum};
use tracing::{debug, error, info, trace};
use tx_heap::TrackedTxHeapDrainAction;

use self::{list::TrackedTxList, tx_heap::TrackedTxHeap};
use super::{
    error::TxPoolInsertionError,
    pending::{PendingTxList, PendingTxMap},
    transaction::ValidEthTransaction,
};
use crate::metrics::TxPoolMetrics;

mod list;
mod tx_heap;

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
pub struct TrackedTxMap<ST, SCT, SBT> {
    last_commit_seq_num: Option<SeqNum>,
    soft_tx_expiry: Duration,
    hard_tx_expiry: Duration,

    // By using IndexMap, we can iterate through the map with Vec-like performance and are able to
    // evict expired txs through the entry API.
    txs: IndexMap<Address, TrackedTxList>,

    _phantom: PhantomData<(ST, SCT, SBT)>,
}

impl<ST, SCT, SBT> TrackedTxMap<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn new(soft_tx_expiry: Duration, hard_tx_expiry: Duration) -> Self {
        Self {
            last_commit_seq_num: None,
            soft_tx_expiry,
            hard_tx_expiry,

            txs: IndexMap::with_capacity(MAX_ADDRESSES),

            _phantom: PhantomData,
        }
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

    pub fn try_add_tx(
        &mut self,
        tx: ValidEthTransaction,
    ) -> Either<ValidEthTransaction, Result<(), TxPoolInsertionError>> {
        if self.last_commit_seq_num.is_none() {
            return Either::Left(tx);
        }

        match self.txs.entry(tx.sender()) {
            IndexMapEntry::Vacant(_) => Either::Left(tx),
            IndexMapEntry::Occupied(mut o) => {
                Either::Right(o.get_mut().try_add_tx(tx, self.hard_tx_expiry))
            }
        }
    }

    pub fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        block_policy: &EthBlockPolicy<ST, SCT>,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
        metrics: &mut TxPoolMetrics,
    ) -> Result<Vec<Recovered<TxEnvelope>>, StateBackendError> {
        let Some(last_commit_seq_num) = self.last_commit_seq_num else {
            return Ok(Vec::new());
        };

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

        self.promote_pending(
            block_policy,
            state_backend,
            pending,
            MAX_PROMOTABLE_ON_CREATE_PROPOSAL,
            metrics,
        )?;

        if self.txs.is_empty() || tx_limit == 0 {
            return Ok(Vec::new());
        }

        let tx_heap = TrackedTxHeap::new(&self.txs, &extending_blocks);

        let account_balances = block_policy.compute_account_base_balances(
            proposed_seq_num,
            state_backend,
            Some(&extending_blocks),
            tx_heap.addresses(),
        )?;

        info!(
            addresses = self.txs.len(),
            num_txs = self.num_txs(),
            tx_heap_len = tx_heap.len(),
            account_balances = account_balances.len(),
            "txpool sequencing transactions"
        );

        let (proposal_total_gas, proposal_tx_list) = self.create_proposal_tx_list(
            tx_limit,
            proposal_gas_limit,
            proposal_byte_limit,
            tx_heap,
            account_balances,
        )?;

        let proposal_num_tx = proposal_tx_list.len();

        info!(
            ?proposed_seq_num,
            ?proposal_num_tx,
            proposal_total_gas,
            "created proposal"
        );

        Ok(proposal_tx_list)
    }

    pub fn promote_pending(
        &mut self,
        block_policy: &EthBlockPolicy<ST, SCT>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
        max_promotable: usize,
        metrics: &mut TxPoolMetrics,
    ) -> Result<(), StateBackendError> {
        let Some(last_commit_seq_num) = self.last_commit_seq_num else {
            return Ok(());
        };

        let Some(insertable) = MAX_ADDRESSES.checked_sub(self.txs.len()) else {
            return Ok(());
        };

        let insertable = insertable.min(max_promotable);

        if insertable == 0 {
            return Ok(());
        }

        let to_insert = pending.split_off(insertable);

        if to_insert.is_empty() {
            return Ok(());
        }

        let addresses = to_insert.len();
        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, addresses, "txpool promote_pending")
        });

        let addresses = to_insert.keys().cloned().collect_vec();

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup outside that range. As a fix, we add 1 so the seqnum is on
        // the edge of the range.
        let account_nonces = block_policy.get_account_base_nonces(
            last_commit_seq_num + SeqNum(1),
            state_backend,
            &Vec::default(),
            addresses.iter(),
        )?;

        for (address, pending_tx_list) in to_insert {
            let pending_num_txs = pending_tx_list.num_txs();

            let Some(account_nonce) = account_nonces.get(&address) else {
                error!("txpool address missing from state backend");

                metrics.pending_drop_unknown_addresses += 1;
                metrics.pending_drop_unknown_txs += pending_num_txs as u64;

                continue;
            };

            match self.txs.entry(address) {
                IndexMapEntry::Occupied(_) => {
                    unreachable!("pending address present in tracked map")
                }
                IndexMapEntry::Vacant(v) => {
                    Self::finalize_promotion(v, *account_nonce, pending_tx_list, metrics);
                }
            }
        }

        Ok(())
    }

    fn create_proposal_tx_list(
        &self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        proposal_byte_limit: u64,
        tx_heap: TrackedTxHeap<'_>,
        mut account_balances: BTreeMap<&Address, u128>,
    ) -> Result<(u64, Vec<Recovered<TxEnvelope>>), StateBackendError> {
        assert!(tx_limit > 0);

        let mut txs = Vec::new();
        let mut total_gas = 0u64;
        let mut total_size = 0u64;

        tx_heap.drain_in_order_while(|sender, tx| {
            if total_gas
                .checked_add(tx.gas_limit())
                .map_or(true, |new_total_gas| new_total_gas > proposal_gas_limit)
            {
                return TrackedTxHeapDrainAction::Skip;
            }

            let tx_size = tx.size();
            if total_size
                .checked_add(tx_size)
                .map_or(true, |new_total_size| new_total_size > proposal_byte_limit)
            {
                return TrackedTxHeapDrainAction::Skip;
            }

            let Some(account_balance) = account_balances.get_mut(sender) else {
                error!(
                    ?sender,
                    "txpool create_proposal account_balances lookup failed"
                );
                return TrackedTxHeapDrainAction::Skip;
            };

            match tx.apply_max_value(account_balance) {
                Ok(new_account_balance) => {
                    *account_balance = new_account_balance;

                    total_gas += tx.gas_limit();
                    total_size += tx_size;
                    trace!(txn_hash = ?tx.hash(), "txn included in proposal");
                    txs.push(tx.raw().to_owned());

                    if txs.len() < tx_limit {
                        TrackedTxHeapDrainAction::Continue
                    } else {
                        TrackedTxHeapDrainAction::Stop
                    }
                }
                Err(_) => TrackedTxHeapDrainAction::Skip,
            }
        });

        Ok((total_gas, txs))
    }

    pub fn update_committed_block(
        &mut self,
        committed_block: EthValidatedBlock<ST, SCT>,
        pending: &mut PendingTxMap,
        metrics: &mut TxPoolMetrics,
    ) {
        {
            let seqnum = committed_block.get_seq_num();
            debug!(?seqnum, "txpool updating committed block");
        }

        if let Some(last_commit_seq_num) = self.last_commit_seq_num {
            assert_eq!(
                committed_block.get_seq_num(),
                last_commit_seq_num + SeqNum(1),
                "txpool received out of order committed block"
            );
        }
        self.last_commit_seq_num = Some(committed_block.get_seq_num());

        let mut insertable = MAX_ADDRESSES.saturating_sub(self.txs.len());

        for (address, highest_tx_nonce) in committed_block.get_nonces() {
            let account_nonce = highest_tx_nonce
                .checked_add(1)
                .expect("nonce does not overflow");

            match self.txs.entry(*address) {
                IndexMapEntry::Occupied(tx_list) => {
                    let num_txs = tx_list.get().num_txs();

                    let Some(tx_list) = TrackedTxList::update_account_nonce(tx_list, account_nonce)
                    else {
                        metrics.tracked_remove_committed_addresses += 1;
                        metrics.tracked_remove_committed_txs += num_txs as u64;
                        continue;
                    };

                    metrics.tracked_remove_committed_txs +=
                        num_txs.saturating_sub(tx_list.get().num_txs()) as u64;
                }
                IndexMapEntry::Vacant(v) => {
                    if insertable == 0 {
                        continue;
                    }

                    let Some(pending_tx_list) = pending.remove(address) else {
                        continue;
                    };

                    insertable -= 1;

                    Self::finalize_promotion(v, account_nonce, pending_tx_list, metrics);
                }
            }
        }
    }

    pub fn evict_expired_txs(&mut self, metrics: &mut TxPoolMetrics) {
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

            let entry_num_txs = entry.get().num_txs();

            let Some(entry) = TrackedTxList::evict_expired_txs(entry, tx_expiry) else {
                metrics.tracked_evict_expired_addresses += 1;
                metrics.tracked_evict_expired_txs += entry_num_txs as u64;
                continue;
            };

            metrics.tracked_evict_expired_txs +=
                entry_num_txs.saturating_sub(entry.get().num_txs()) as u64;

            idx += 1;
        }
    }

    pub fn reset(&mut self, last_delay_committed_blocks: Vec<EthValidatedBlock<ST, SCT>>) {
        self.txs.clear();
        self.last_commit_seq_num = last_delay_committed_blocks
            .last()
            .map(|block| block.get_seq_num())
    }

    fn finalize_promotion(
        v: indexmap::map::VacantEntry<'_, Address, TrackedTxList>,
        account_nonce: u64,
        pending_tx_list: PendingTxList,
        metrics: &mut TxPoolMetrics,
    ) {
        let pending_num_txs = pending_tx_list.num_txs();

        let Some(tracked_tx_list) =
            TrackedTxList::new_from_account_nonce_and_pending(account_nonce, pending_tx_list)
        else {
            metrics.pending_drop_low_nonce_addresses += 1;
            metrics.pending_drop_low_nonce_txs += pending_num_txs as u64;

            return;
        };

        let tracked_num_txs = tracked_tx_list.num_txs();

        metrics.pending_drop_low_nonce_txs +=
            pending_num_txs.saturating_sub(tracked_num_txs) as u64;

        v.insert(tracked_tx_list);

        metrics.pending_promote_addresses += 1;
        metrics.pending_promote_txs += tracked_num_txs as u64;
    }
}
