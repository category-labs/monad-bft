use alloy_rlp::Decodable;
use bytes::Bytes;
use itertools::{Either, Itertools};
use monad_consensus_types::{
    payload::FullTransactionList,
    signature_collection::SignatureCollection,
    txpool::{TxPool, TxPoolInsertionError},
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::EthTransaction;
use monad_eth_types::{Balance, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::SeqNum;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::{pending::PendingTxMap, tracked::TrackedTxMap, transaction::ValidEthTransaction};

mod pending;
mod tracked;
mod transaction;

const MAX_PROPOSAL_SIZE: usize = 10_000;

#[derive(Clone, Debug)]
pub struct EthTxPool<SCT, SBT> {
    do_local_insert: bool,
    pending: PendingTxMap,
    tracked: TrackedTxMap<SCT, SBT>,
}

impl<SCT, SBT> Default for EthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn default() -> Self {
        Self::new(true)
    }
}

impl<SCT, SBT> EthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
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

    pub fn promote_pending(
        &mut self,
        block_policy: &EthBlockPolicy,
        state_backend: &SBT,
        max_promotable: usize,
    ) -> Result<(), StateBackendError>
    where
        SCT: SignatureCollection,
        SBT: StateBackend,
    {
        self.tracked.promote_pending(
            block_policy,
            state_backend,
            &mut self.pending,
            max_promotable,
        )
    }

    fn validate_and_insert_tx(
        &mut self,
        tx: EthTransaction,
        block_policy: &EthBlockPolicy,
        account_balance: &Balance,
    ) -> Result<(), TxPoolInsertionError> {
        let Some(tx_chain_id) = tx.chain_id() else {
            return Err(TxPoolInsertionError::NotWellFormed);
        };

        if tx_chain_id != block_policy.get_chain_id() {
            return Err(TxPoolInsertionError::NotWellFormed);
        }

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
        block_policy: &EthBlockPolicy,
        state_backend: &impl StateBackend,
        txs: Vec<EthTransaction>,
    ) -> Result<Vec<Result<(), TxPoolInsertionError>>, StateBackendError> {
        let senders = txs.iter().map(|tx| EthAddress(tx.signer())).collect_vec();

        // BlockPolicy only guarantees that data is available for seqnum (N-k, N] for some execution
        // delay k. Since block_policy looks up seqnum - execution_delay, passing the last commit
        // seqnum will result in a lookup outside that range. As a fix, we add 1 so the seqnum is on
        // the edge of the range.
        let block_seq_num = block_policy.get_last_commit() + SeqNum(1);

        let sender_account_balances = block_policy.compute_account_base_balances::<SCT>(
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

impl<SCT, SBT> TxPool<SCT, EthBlockPolicy, SBT> for EthTxPool<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    fn insert_tx(
        &mut self,
        txns: Vec<Bytes>,
        block_policy: &EthBlockPolicy,
        state_backend: &SBT,
    ) -> Vec<Bytes> {
        // TODO: unwrap can be removed when this is made generic over the actual
        // tx type rather than Bytes and decoding won't be necessary
        // TODO(rene): sender recovery is done inline here
        let (decoded_txs, raw_txs): (Vec<_>, Vec<_>) = txns
            .into_par_iter()
            .filter_map(|b| {
                EthTransaction::decode(&mut b.as_ref())
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
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
    ) -> Result<FullTransactionList, StateBackendError> {
        self.tracked.create_proposal(
            proposed_seq_num,
            tx_limit.min(MAX_PROPOSAL_SIZE),
            proposal_gas_limit,
            block_policy,
            extending_blocks,
            state_backend,
            &mut self.pending,
        )
    }

    fn update_committed_block(&mut self, committed_block: &EthValidatedBlock<SCT>) {
        self.tracked
            .update_committed_block(committed_block, &mut self.pending);
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.tracked.clear()
    }

    fn reset(&mut self, last_delay_committed_blocks: Vec<&EthValidatedBlock<SCT>>) {
        self.tracked.reset(last_delay_committed_blocks);
    }
}
