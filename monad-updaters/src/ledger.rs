use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_blocksync::messages::message::{
    BlockSyncBodyResponse, BlockSyncHeadersResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockRange, ConsensusFullBlock, OptimisticCommit},
    payload::ConsensusBlockBodyId,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::InMemoryState;
use monad_types::{BlockId, ExecutionProtocol, Round, SeqNum};

pub trait MockableLedger:
    Executor<
        Command = LedgerCommand<
            Self::Signature,
            Self::SignatureCollection,
            Self::ExecutionProtocol,
        >,
    > + Stream<Item = Self::Event>
    + Unpin
{
    type Signature: CertificateSignatureRecoverable;
    type SignatureCollection: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::Signature>,
    >;
    type ExecutionProtocol: ExecutionProtocol;

    type Event;

    fn ready(&self) -> bool;
    fn get_finalized_blocks(
        &self,
    ) -> &BTreeMap<
        SeqNum,
        ConsensusFullBlock<Self::Signature, Self::SignatureCollection, Self::ExecutionProtocol>,
    >;
}

impl<T: MockableLedger + ?Sized> MockableLedger for Box<T> {
    type Signature = T::Signature;
    type SignatureCollection = T::SignatureCollection;
    type ExecutionProtocol = T::ExecutionProtocol;

    type Event = T::Event;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn get_finalized_blocks(
        &self,
    ) -> &BTreeMap<
        SeqNum,
        ConsensusFullBlock<Self::Signature, Self::SignatureCollection, Self::ExecutionProtocol>,
    > {
        (**self).get_finalized_blocks()
    }
}

pub struct MockLedger<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    blocks: BTreeMap<Round, ConsensusFullBlock<ST, SCT, EPT>>,
    block_ids: HashMap<BlockId, Round>,
    committed_blocks: BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EPT>>,

    events: VecDeque<BlockSyncEvent<ST, SCT, EPT>>,

    state_backend: InMemoryState,

    waker: Option<Waker>,
    _phantom: PhantomData<ST>,
}

impl<ST, SCT, EPT> MockLedger<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(state_backend: InMemoryState) -> Self {
        Self {
            blocks: Default::default(),
            block_ids: Default::default(),
            committed_blocks: Default::default(),
            events: Default::default(),

            state_backend,

            waker: Default::default(),
            _phantom: Default::default(),
        }
    }

    fn get_headers(&self, block_range: BlockRange) -> BlockSyncHeadersResponse<ST, SCT, EPT> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        while (headers.len() as u64) < block_range.num_blocks.0 {
            // TODO add max number of headers to read
            let Some(block_round) = self.block_ids.get(&next_block_id) else {
                return BlockSyncHeadersResponse::NotAvailable(block_range);
            };
            let block_header = self
                .blocks
                .get(block_round)
                .expect("round to blockid invariant")
                .header();

            headers.push_front(block_header.clone());
            next_block_id = block_header.get_parent_id();
        }

        BlockSyncHeadersResponse::Found((block_range, headers.into()))
    }

    fn get_payload(&self, payload_id: ConsensusBlockBodyId) -> BlockSyncBodyResponse<EPT> {
        // TODO: all payloads are stored in memory, facilitate blocksync for only the blocksyncable_range
        if let Some((_, full_block)) = self
            .blocks
            .iter()
            .find(|(_, full_block)| full_block.get_body_id() == payload_id)
        {
            BlockSyncBodyResponse::Found(full_block.body().clone())
        } else {
            BlockSyncBodyResponse::NotAvailable(payload_id)
        }
    }
}

impl<ST, SCT, EPT> Executor for MockLedger<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = LedgerCommand<ST, SCT, EPT>;

    fn exec(&mut self, cmds: Vec<Self::Command>) {
        for cmd in cmds {
            match cmd {
                LedgerCommand::LedgerClearWal => {}
                LedgerCommand::LedgerCommit(OptimisticCommit::Proposed(block)) => {
                    self.state_backend.lock().unwrap().ledger_propose(
                        block.get_id(),
                        block.get_seq_num(),
                        block.get_round(),
                        block.get_parent_round(),
                        BTreeMap::default(), // TODO parse out txs
                    );
                    match self.blocks.entry(block.get_round()) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            let block_id = block.get_id();
                            let round = block.get_round();
                            entry.insert(block);
                            self.block_ids.insert(block_id, round);
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            if entry.get() != &block {
                                self.block_ids.remove(&entry.get().get_id());

                                let block_id = block.get_id();
                                let round = block.get_round();
                                entry.insert(block);
                                self.block_ids.insert(block_id, round);
                            }
                        }
                    }
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::Finalized(block)) => {
                    self.committed_blocks
                        .insert(block.get_seq_num(), block.clone());
                    self.state_backend
                        .lock()
                        .unwrap()
                        .ledger_commit(&block.get_id());
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

impl<ST, SCT, EPT> Stream for MockLedger<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;
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

impl<ST, SCT, EPT> MockableLedger for MockLedger<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EPT;
    type Event = MonadEvent<ST, SCT, EPT>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn get_finalized_blocks(&self) -> &BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EPT>> {
        &self.committed_blocks
    }
}
