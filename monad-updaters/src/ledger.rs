use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus::messages::message::BlockSyncResponseMessage;
use monad_consensus_types::{
    block::{BlockType, FullBlock},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::InMemoryState;
use monad_types::{BlockId, Round};

pub trait MockableLedger:
    Executor<Command = LedgerCommand<Self::SignatureCollection>> + Stream<Item = Self::Event> + Unpin
{
    type SignatureCollection: SignatureCollection;
    type Event;

    fn ready(&self) -> bool;
    fn get_blocks(&self) -> &BTreeMap<Round, FullBlock<Self::SignatureCollection>>;
}

impl<T: MockableLedger + ?Sized> MockableLedger for Box<T> {
    type SignatureCollection = T::SignatureCollection;
    type Event = T::Event;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn get_blocks(&self) -> &BTreeMap<Round, FullBlock<Self::SignatureCollection>> {
        (**self).get_blocks()
    }
}

pub struct MockLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    blocks: BTreeMap<Round, FullBlock<SCT>>,
    block_ids: HashMap<BlockId, Round>,
    events: VecDeque<BlockSyncEvent<SCT>>,

    state_backend: InMemoryState,

    waker: Option<Waker>,
    _phantom: PhantomData<ST>,
}

impl<ST, SCT> MockLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(state_backend: InMemoryState) -> Self {
        Self {
            blocks: Default::default(),
            block_ids: Default::default(),
            events: Default::default(),

            state_backend,

            waker: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<ST, SCT> Executor for MockLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = LedgerCommand<SCT>;

    fn exec(&mut self, cmds: Vec<Self::Command>) {
        for cmd in cmds {
            match cmd {
                LedgerCommand::LedgerCommit(blocks) => {
                    for block in blocks {
                        if !block.is_empty_block() {
                            // mock ledger isn't used with real txs
                            self.state_backend
                                .lock()
                                .unwrap()
                                .ledger_commit(block.get_seq_num(), BTreeMap::default());
                        }

                        match self.blocks.entry(block.get_round()) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                let block_id = block.get_id();
                                let round = block.get_round();
                                entry.insert(block);
                                self.block_ids.insert(block_id, round);
                            }
                            std::collections::btree_map::Entry::Occupied(entry) => {
                                assert_eq!(entry.get(), &block, "two conflicting blocks committed")
                            }
                        }
                    }
                }
                LedgerCommand::LedgerFetchHeaders(block_id_range) => {
                    // self.events.push_back(BlockSyncEvent::SelfResponse {
                    //     response: match self.block_ids.get(&block_id) {
                    //         Some(round) => BlockSyncResponseMessage::BlockFound(
                    //             self.blocks
                    //                 .get(round)
                    //                 .expect("block_id mapping inconsistent")
                    //                 .clone(),
                    //         ),
                    //         None => BlockSyncResponseMessage::NotAvailable(block_id),
                    //     },
                    // });
                    // if let Some(waker) = self.waker.take() {
                    //     waker.wake()
                    // }
                }
                LedgerCommand::LedgerFetchPayload(payload_id) => {

                }
            }
        }
    }
    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}

impl<ST, SCT> Stream for MockLedger<ST, SCT>
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

impl<ST, SCT> MockableLedger for MockLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type SignatureCollection = SCT;
    type Event = MonadEvent<ST, SCT>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn get_blocks(&self) -> &BTreeMap<Round, FullBlock<SCT>> {
        &self.blocks
    }
}
