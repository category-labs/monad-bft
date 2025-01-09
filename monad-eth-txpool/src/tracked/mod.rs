use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use itertools::{Either, Itertools};
use monad_consensus_types::{
    block::BlockType, payload::FullTransactionList, signature_collection::SignatureCollection,
    txpool::TxPoolInsertionError,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_tx::{EthFullTransactionList, EthTransaction};
use monad_eth_types::EthAddress;
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{DropTimer, SeqNum};
use tracing::{debug, error, info, trace};
use tx_heap::TrackedTxHeapDrainAction;

use self::{list::TrackedTxList, tx_heap::TrackedTxHeap};
use crate::{pending::PendingTxMap, transaction::ValidEthTransaction};

mod list;
mod tx_heap;

// To produce 10k tx blocks, we need the tracked tx map to hold at least 20k addresses so that if
// the block in the pending blocktree has 10k txs with 10k unique addresses that are also in the
// tracked tx map then we still have 10k other addresses to use when creating the next block.
const MAX_ADDRESSES: usize = 20 * 1024;

// TODO(andr-dev): This currently limits the number of unique addresses in a
// proposal. This will be removed once we move the txpool into its own thread.
const MAX_PROMOTABLE_ON_CREATE_PROPOSAL: usize = 1024 * 10;

/// Stores transactions using a "snapshot" system by which each address has an associated
/// account_nonce stored in the TrackedTxList which is guaranteed to be the correct
/// account_nonce for the seqnum stored in last_commit_seq_num.
#[derive(Clone, Debug)]
pub struct TrackedTxMap<SCT, SBT> {
    last_commit_seq_num: Option<SeqNum>,
    tx_expiry: Duration,

    // By using IndexMap, we can iterate through the map with Vec-like performance and are able to
    // evict expired txs through the entry API.
    txs: IndexMap<EthAddress, TrackedTxList>,

    _phantom: PhantomData<(SCT, SBT)>,
}

impl<SCT, SBT> TrackedTxMap<SCT, SBT>
where
    SCT: SignatureCollection,
    SBT: StateBackend,
{
    pub fn new(tx_expiry: Duration) -> Self {
        Self {
            last_commit_seq_num: None,
            tx_expiry,

            txs: IndexMap::with_capacity(MAX_ADDRESSES),

            _phantom: PhantomData,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
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
                Either::Right(o.get_mut().try_add_tx(tx, self.tx_expiry))
            }
        }
    }

    pub fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        proposal_gas_limit: u64,
        block_policy: &EthBlockPolicy,
        extending_blocks: Vec<&EthValidatedBlock<SCT>>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
    ) -> Result<FullTransactionList, StateBackendError> {
        let Some(last_commit_seq_num) = self.last_commit_seq_num else {
            return Ok(FullTransactionList::empty());
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

            return Ok(FullTransactionList::empty());
        }

        let _timer = DropTimer::start(Duration::ZERO, |elapsed| {
            debug!(?elapsed, "txpool create_proposal");
        });

        self.promote_pending(
            block_policy,
            state_backend,
            pending,
            MAX_PROMOTABLE_ON_CREATE_PROPOSAL,
        )?;

        if self.txs.is_empty() || tx_limit == 0 {
            return Ok(FullTransactionList::empty());
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

        let (proposal_total_gas, proposal_tx_list) =
            self.create_proposal_tx_list(tx_limit, proposal_gas_limit, tx_heap, account_balances)?;

        let proposal_num_tx = proposal_tx_list.len();
        let proposal_rlp_tx_list = EthFullTransactionList(proposal_tx_list).rlp_encode();

        info!(
            ?proposed_seq_num,
            ?proposal_num_tx,
            proposal_total_gas,
            proposal_tx_bytes = proposal_rlp_tx_list.len(),
            "created proposal"
        );

        Ok(FullTransactionList::new(proposal_rlp_tx_list))
    }

    pub fn promote_pending(
        &mut self,
        block_policy: &EthBlockPolicy,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
        max_promotable: usize,
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
        DropTimer::start(Duration::ZERO, |elapsed| {
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
            &Vec::<&EthValidatedBlock<SCT>>::default(),
            addresses.iter(),
        )?;

        for (address, tx_list) in to_insert {
            let Some(account_nonce) = account_nonces.get(&address) else {
                error!("txpool address missing from state backend");
                continue;
            };

            match self.txs.entry(address) {
                IndexMapEntry::Occupied(_) => {
                    unreachable!("pending address present in tracked map")
                }
                IndexMapEntry::Vacant(v) => {
                    if let Some(tx_list) =
                        TrackedTxList::new_from_account_nonce_and_pending(*account_nonce, tx_list)
                    {
                        v.insert(tx_list);
                    }
                }
            }
        }

        Ok(())
    }

    fn create_proposal_tx_list(
        &self,
        tx_limit: usize,
        proposal_gas_limit: u64,
        tx_heap: TrackedTxHeap<'_>,
        mut account_balances: BTreeMap<&EthAddress, u128>,
    ) -> Result<(u64, Vec<EthTransaction>), StateBackendError> {
        assert!(tx_limit > 0);

        let mut txs = Vec::new();
        let mut total_gas = 0u64;

        tx_heap.drain_in_order_while(|sender, tx| {
            if total_gas
                .checked_add(tx.gas_limit())
                .map(|total_gas| total_gas > proposal_gas_limit)
                .unwrap_or(true)
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

            match tx.apply_txn_fee(account_balance) {
                Ok(new_account_balance) => {
                    *account_balance = new_account_balance;

                    total_gas += tx.gas_limit();
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
        committed_block: &EthValidatedBlock<SCT>,
        pending: &mut PendingTxMap,
    ) {
        if committed_block.is_empty_block() {
            // TODO this is error-prone, easy to forget
            // TODO write tests that fail if this doesn't exist
            return;
        }

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

        for (address, highest_tx_nonce) in committed_block.get_nonces() {
            let account_nonce = highest_tx_nonce
                .checked_add(1)
                .expect("nonce does not overflow");

            match self.txs.entry(*address) {
                IndexMapEntry::Occupied(tx_list) => {
                    TrackedTxList::update_account_nonce(tx_list, account_nonce)
                }
                IndexMapEntry::Vacant(v) => {
                    let Some(tx_list) = pending.remove(address) else {
                        continue;
                    };

                    if let Some(tx_list) =
                        TrackedTxList::new_from_account_nonce_and_pending(account_nonce, tx_list)
                    {
                        v.insert(tx_list);
                    }
                }
            }
        }
    }

    pub fn evict_expired_txs(&mut self) {
        let mut idx = 0;

        while let Some(entry) = self.txs.get_index_entry(idx) {
            let Some(_) = TrackedTxList::evict_expired_txs(entry, self.tx_expiry) else {
                continue;
            };

            idx += 1;
        }
    }

    pub fn reset(&mut self, last_delay_committed_blocks: Vec<&EthValidatedBlock<SCT>>) {
        self.txs.clear();
        self.last_commit_seq_num = last_delay_committed_blocks
            .last()
            .map(|block| block.get_seq_num())
    }
}
