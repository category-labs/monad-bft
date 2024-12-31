use std::{collections::BTreeMap, marker::PhantomData, time::Duration};

use indexmap::{map::Entry as IndexMapEntry, IndexMap};
use itertools::{Either, Itertools};
use monad_consensus_types::{
    payload::{PROPOSAL_GAS_LIMIT, PROPOSAL_SIZE_LIMIT},
    signature_collection::SignatureCollection,
    txpool::TxPoolInsertionError,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{EthBlockPolicy, EthValidatedBlock};
use monad_eth_types::{EthAddress, TxEnvelopeWithSigner};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{DropTimer, SeqNum};
use tracing::{debug, error, info, trace};
use tx_heap::TrackedTxHeapDrainAction;

use self::{list::TrackedTxList, tx_heap::TrackedTxHeap};
use crate::{pending::PendingTxMap, transaction::ValidEthTransaction};

mod list;
mod tx_heap;

const MAX_ADDRESSES: usize = 16 * 1024;

// TODO(andr-dev): This currently limits the number of unique addresses in a
// proposal. This will be removed once we move the txpool into its own thread.
const MAX_PROMOTABLE_ON_CREATE_PROPOSAL: usize = 1024 * 10;

/// Stores transactions using a "snapshot" system by which each address has an associated
/// account_nonce stored in the TrackedTxList which is guaranteed to be the correct
/// account_nonce for the seqnum stored in last_commit_seq_num.
#[derive(Clone, Debug)]
pub struct TrackedTxMap<ST, SCT, SBT> {
    last_commit_seq_num: SeqNum,
    txs: IndexMap<EthAddress, TrackedTxList>,

    _phantom: PhantomData<(ST, SCT, SBT)>,
}

impl<ST, SCT, SBT> Default for TrackedTxMap<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    fn default() -> Self {
        Self {
            last_commit_seq_num: SeqNum::MIN,
            txs: IndexMap::with_capacity(MAX_ADDRESSES),

            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, SBT> TrackedTxMap<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
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
        match self.txs.entry(tx.sender()) {
            IndexMapEntry::Vacant(_) => Either::Left(tx),
            IndexMapEntry::Occupied(mut o) => Either::Right(o.get_mut().try_add_tx(tx)),
        }
    }

    pub fn create_proposal(
        &mut self,
        proposed_seq_num: SeqNum,
        tx_limit: usize,
        block_policy: &EthBlockPolicy<ST, SCT>,
        extending_blocks: Vec<&EthValidatedBlock<ST, SCT>>,
        state_backend: &SBT,
        pending: &mut PendingTxMap,
    ) -> Result<Vec<TxEnvelopeWithSigner>, StateBackendError> {
        assert!(
            block_policy.get_last_commit().ge(&self.last_commit_seq_num),
            "txpool received block policy with lower committed seq num"
        );

        if self.last_commit_seq_num != block_policy.get_last_commit() {
            error!(
                block_policy_last_commit = block_policy.get_last_commit().0,
                txpool_last_commit = self.last_commit_seq_num.0,
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
        )?;

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
            self.create_proposal_tx_list(tx_limit, tx_heap, account_balances)?;

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
    ) -> Result<(), StateBackendError> {
        if self.last_commit_seq_num.0 == 0 {
            return Ok(());
        }

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

        let account_nonces = block_policy.get_account_base_nonces(
            self.last_commit_seq_num + SeqNum(1),
            state_backend,
            &Vec::default(),
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
        tx_heap: TrackedTxHeap<'_>,
        mut account_balances: BTreeMap<&EthAddress, u128>,
    ) -> Result<(u64, Vec<TxEnvelopeWithSigner>), StateBackendError> {
        assert!(tx_limit > 0);

        let mut txs = Vec::new();
        let mut total_gas = 0u64;
        let mut total_size = 0u64;

        tx_heap.drain_in_order_while(|sender, tx| {
            if total_gas
                .checked_add(tx.gas_limit())
                .map_or(true, |new_total_gas| new_total_gas > PROPOSAL_GAS_LIMIT)
            {
                return TrackedTxHeapDrainAction::Skip;
            }

            let tx_size = tx.size();
            if total_size
                .checked_add(tx_size)
                .map_or(true, |new_total_size| new_total_size > PROPOSAL_SIZE_LIMIT)
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
        committed_block: &EthValidatedBlock<ST, SCT>,
        pending: &mut PendingTxMap,
    ) where
        SCT: SignatureCollection,
    {
        {
            let seqnum = committed_block.get_seq_num();
            debug!(?seqnum, "txpool updating committed block");
        }

        if self.last_commit_seq_num != SeqNum::MIN {
            assert_eq!(
                committed_block.get_seq_num(),
                self.last_commit_seq_num + SeqNum(1),
                "txpool received out of order committed block"
            );
        }
        self.last_commit_seq_num = committed_block.get_seq_num();

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
            let Some(_) = TrackedTxList::evict_expired_txs(entry) else {
                continue;
            };

            idx += 1;
        }
    }

    pub fn reset(&mut self, last_delay_committed_blocks: Vec<&EthValidatedBlock<ST, SCT>>) {
        self.txs.clear();
        self.last_commit_seq_num = last_delay_committed_blocks
            .last()
            .map(|block| block.get_seq_num())
            .unwrap_or(SeqNum(0));
    }
}
