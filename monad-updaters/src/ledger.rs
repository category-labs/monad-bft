// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::{InMemoryState, StateBackendTest};
use monad_types::{BlockId, ExecutionProtocol, SeqNum};
use monad_validator::signature_collection::SignatureCollection;

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
    blocks: HashMap<BlockId, ConsensusFullBlock<ST, SCT, EPT>>,
    committed_blocks: BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EPT>>,

    events: VecDeque<BlockSyncEvent<ST, SCT, EPT>>,

    finalization_delay: SeqNum,
    state_backend: InMemoryState<ST, SCT>,

    waker: Option<Waker>,
    _phantom: PhantomData<ST>,
}

impl<ST, SCT, EPT> MockLedger<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(state_backend: InMemoryState<ST, SCT>) -> Self {
        Self {
            blocks: Default::default(),
            committed_blocks: Default::default(),
            events: Default::default(),

            finalization_delay: SeqNum(0),
            state_backend,

            waker: Default::default(),
            _phantom: Default::default(),
        }
    }

    pub fn with_blocks(mut self, old_ledger: &Self) -> Self {
        self.blocks = old_ledger.blocks.clone();
        self.committed_blocks = old_ledger.committed_blocks.clone();
        self
    }

    pub fn with_finalization_delay(mut self, finalization_delay: SeqNum) -> Self {
        self.finalization_delay = finalization_delay;
        self
    }

    fn get_headers(&self, block_range: BlockRange) -> BlockSyncHeadersResponse<ST, SCT, EPT> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        while (headers.len() as u64) < block_range.num_blocks.0 {
            // TODO add max number of headers to read
            let Some(block) = self.blocks.get(&next_block_id) else {
                return BlockSyncHeadersResponse::NotAvailable(block_range);
            };
            let block_header = block.header();

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
                LedgerCommand::LedgerCommit(
                    OptimisticCommit::Proposed(block) | OptimisticCommit::UpdateProposedHead(block),
                ) => {
                    self.state_backend.lock().unwrap().ledger_propose(
                        block.get_id(),
                        block.get_seq_num(),
                        block.get_block_round(),
                        block.get_parent_id(),
                        BTreeMap::default(), // TODO parse out txs
                    );
                    self.blocks.insert(block.get_id(), block);
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::UpdateVotedHead(block)) => {
                    self.blocks.insert(block.get_id(), block);
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::UpdateFinalizedHead(block)) => {
                    if block.get_seq_num() <= self.finalization_delay {
                        continue;
                    }
                    let finalize_seq_num = block.get_seq_num() - self.finalization_delay;
                    let mut block = block;
                    loop {
                        if block.get_seq_num() == finalize_seq_num {
                            self.committed_blocks
                                .insert(block.get_seq_num(), block.clone());
                            let mut state = self.state_backend.lock().unwrap();
                            state.ledger_commit(&block.get_id(), &block.get_seq_num());
                            break;
                        }
                        block = match self.blocks.get(&block.get_parent_id()) {
                            None => break,
                            Some(next_block) => next_block.clone(),
                        };
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
    fn metrics(&self) -> ExecutorMetricsChain<'_> {
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
