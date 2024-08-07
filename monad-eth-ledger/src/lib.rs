use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use alloy_rlp::Decodable;
use futures::Stream;
use monad_consensus::messages::message::BlockSyncResponseMessage;
use monad_consensus_types::{
    block::{Block, BlockType},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{compute_txn_carriage_cost, nonce::{InMemoryAccount, InMemoryState, PreviousTxn}};
use monad_eth_tx::EthTransaction;
use monad_eth_types::EthAddress;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::StateBackend;
use monad_types::{BlockId, SeqNum};
use monad_updaters::ledger::MockableLedger;

/// A ledger for commited Monad Blocks
/// Purpose of the ledger is to have retrievable committed blocks to
/// respond the BlockSync requests
/// MockEthLedger stores the ledger in memory and is only expected to be used in testing
pub struct MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    blocks: BTreeMap<SeqNum, Block<SCT>>,
    block_ids: HashMap<BlockId, SeqNum>,
    events: VecDeque<BlockSyncEvent<SCT>>,

    state: Arc<Mutex<InMemoryState>>,

    waker: Option<Waker>,
    _phantom: PhantomData<ST>,
}

impl<ST, SCT> MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(state: Arc<Mutex<InMemoryState>>) -> Self {
        MockEthLedger {
            blocks: Default::default(),
            block_ids: Default::default(),
            events: Default::default(),

            state,

            waker: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<ST, SCT> Executor for MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = LedgerCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(blocks) => {
                    for block in blocks {
                        match self.blocks.entry(block.get_seq_num()) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                let block_id = block.get_id();
                                let seq_num = block.get_seq_num();
                                entry.insert(block.clone());
                                self.block_ids.insert(block_id, seq_num);
                            }
                            std::collections::btree_map::Entry::Occupied(entry) => {
                                assert_eq!(entry.get(), &block, "two conflicting blocks committed")
                            }
                        }
                        // generate eth block and update the state backend with committed nonces
                        let mut state = self.state.lock().unwrap();
                        let new_accounts =
                            Vec::<EthTransaction>::decode(&mut block.payload.txns.bytes().as_ref())
                                .expect("invalid eth tx in block")
                                .into_iter()
                                .map(|tx| {
                                    let original_balance = state.raw_read_account(SeqNum(block.get_seq_num().0 - 1), &EthAddress(tx.signer())).map_or_else(|| 0, |a| a.balance);
                                    (
                                        EthAddress(tx.signer()),
                                        (
                                            InMemoryAccount {
                                                balance: original_balance - compute_txn_carriage_cost(&tx),
                                                nonce: tx.nonce() + 1
                                            },
                                            PreviousTxn {
                                                seq_num: block.get_seq_num(),
                                                address: EthAddress(tx.signer()),
                                                reserve_balance: compute_txn_carriage_cost(&tx)
                                            }
                                        )
                                    )
                                })
                                // collecting into a map will handle a sender sending multiple
                                // transactions gracefully
                                //
                                // this is because nonces are always increasing per account
                                .collect();
                        state.update_committed_accounts(block.get_seq_num(), new_accounts);
                    }
                }
                LedgerCommand::LedgerFetch(block_id) => {
                    self.events.push_back(BlockSyncEvent::SelfResponse {
                        response: match self.block_ids.get(&block_id) {
                            Some(seq_num) => BlockSyncResponseMessage::BlockFound(
                                self.blocks
                                    .get(seq_num)
                                    .expect("block_id mapping inconsistent")
                                    .clone(),
                            ),
                            None => BlockSyncResponseMessage::NotAvailable(block_id),
                        },
                    });
                    if let Some(waker) = self.waker.take() {
                        waker.wake()
                    }
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}

impl<ST, SCT> Stream for MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(event) = this.events.pop_front() {
            return Poll::Ready(Some(MonadEvent::BlockSyncEvent(event)));
        }

        if this.waker.is_none() {
            this.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<ST, SCT> MockableLedger for MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type SignatureCollection = SCT;
    type Event = MonadEvent<ST, SCT>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn get_blocks(&self) -> &BTreeMap<SeqNum, Block<SCT>> {
        &self.blocks
    }
}
