use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_blocksync::messages::message::{
    BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockRange, BlockType, FullBlock},
    payload::PayloadId,
    quorum_certificate::GENESIS_BLOCK_ID,
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

    fn get_headers(&self, block_range: BlockRange) -> BlockSyncHeadersResponse<SCT> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        loop {
            // TODO add max number of headers to read
            let Some(block_round) = self.block_ids.get(&next_block_id) else {
                return BlockSyncHeadersResponse::NotAvailable(block_range);
            };

            let block_header = self
                .blocks
                .get(block_round)
                .expect("round to blockid invariant")
                .get_unvalidated_block_ref();

            if block_header.get_seq_num() < block_range.root_seq_num {
                // if headers is empty here, then block range is invalid
                break;
            }

            headers.push_front(block_header.clone());
            next_block_id = block_header.get_parent_id();

            if next_block_id == GENESIS_BLOCK_ID {
                // don't try fetching genesis block
                break;
            }
        }

        BlockSyncHeadersResponse::Found((block_range, headers.into()))
    }

    fn get_payload(&self, payload_id: PayloadId) -> BlockSyncPayloadResponse {
        // TODO: all payloads are stored in memory, facilitate blocksync for only the blocksyncable_range
        if let Some((_, full_block)) = self
            .blocks
            .iter()
            .find(|(_, full_block)| full_block.get_payload_id() == payload_id)
        {
            BlockSyncPayloadResponse::Found(full_block.payload.clone())
        } else {
            BlockSyncPayloadResponse::NotAvailable(payload_id)
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
                LedgerCommand::LedgerFetchHeaders(block_range) => {
                    self.events.push_back(BlockSyncEvent::SelfResponse {
                        response: BlockSyncResponseMessage::HeadersResponse(
                            self.get_headers(block_range),
                        ),
                    });
                }
                LedgerCommand::LedgerFetchPayload(payload_id) => {
                    self.events.push_back(BlockSyncEvent::SelfResponse {
                        response: BlockSyncResponseMessage::PayloadResponse(
                            self.get_payload(payload_id),
                        ),
                    });
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
