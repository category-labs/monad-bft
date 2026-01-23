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
    collections::{BTreeMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloy_consensus::Transaction as _;
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
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::{InMemoryState, StateBackendTest};
use monad_types::{BlockId, SeqNum};
use monad_updaters::ledger::MockableLedger;
use monad_validator::signature_collection::SignatureCollection;
use tracing::debug_span;

/// A ledger for commited Monad Blocks
/// Purpose of the ledger is to have retrievable committed blocks to
/// respond the BlockSync requests
/// MockEthLedger stores the ledger in memory and is only expected to be used in testing
pub struct MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    blocks: BTreeMap<BlockId, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    finalized: BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    events: VecDeque<BlockSyncEvent<ST, SCT, EthExecutionProtocol>>,

    state: InMemoryState<ST, SCT>,

    waker: Option<Waker>,
    _phantom: PhantomData<ST>,
}

impl<ST, SCT> MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(state: InMemoryState<ST, SCT>) -> Self {
        MockEthLedger {
            blocks: Default::default(),
            finalized: Default::default(),
            events: Default::default(),

            state,

            waker: Default::default(),
            _phantom: Default::default(),
        }
    }

    fn get_headers(
        &self,
        block_range: BlockRange,
    ) -> BlockSyncHeadersResponse<ST, SCT, EthExecutionProtocol> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        while (headers.len() as u64) < block_range.num_blocks.0 {
            // TODO add max number of headers to read
            let Some(next_block) = self.blocks.get(&next_block_id) else {
                return BlockSyncHeadersResponse::NotAvailable(block_range);
            };

            headers.push_front(next_block.header().clone());
            next_block_id = next_block.get_parent_id();
        }

        BlockSyncHeadersResponse::Found((block_range, headers.into()))
    }

    fn get_payload(
        &self,
        body_id: ConsensusBlockBodyId,
    ) -> BlockSyncBodyResponse<EthExecutionProtocol> {
        // TODO: all payloads are stored in memory, facilitate blocksync for only the blocksyncable_range
        if let Some((_, full_block)) = self
            .blocks
            .iter()
            .find(|(_, full_block)| full_block.get_body_id() == body_id)
        {
            return BlockSyncBodyResponse::Found(full_block.body().clone());
        }

        BlockSyncBodyResponse::NotAvailable(body_id)
    }
}

impl<ST, SCT> Executor for MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = LedgerCommand<ST, SCT, EthExecutionProtocol>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(OptimisticCommit::Proposed(block)) => {
                    let _span = debug_span!("optimistic commit proposed").entered();
                    // generate eth block and update the state backend with committed nonces
                    let new_account_nonces = block
                        .body()
                        .execution_body
                        .transactions
                        .iter()
                        .map(|tx| {
                            (
                                tx.recover_signer().expect("invalid eth tx in block"),
                                tx.nonce() + 1,
                            )
                        })
                        // collecting into a map will handle a sender sending multiple
                        // transactions gracefully
                        //
                        // this is because nonces are always increasing per account
                        .collect();
                    let mut state = self.state.lock().unwrap();
                    state.ledger_propose(
                        block.get_id(),
                        block.get_seq_num(),
                        block.get_block_round(),
                        block.get_parent_id(),
                        new_account_nonces,
                    );

                    self.blocks.insert(block.get_id(), block);
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::Finalized(block)) => {
                    let _span = debug_span!("optimistic commit finalized").entered();
                    self.finalized.insert(block.get_seq_num(), block.clone());
                    let mut state = self.state.lock().unwrap();
                    state.ledger_commit(&block.get_id(), &block.get_seq_num());
                }
                LedgerCommand::LedgerFetchHeaders(block_range) => {
                    let _span = debug_span!("ledger fetch header").entered();
                    self.events.push_back(BlockSyncEvent::SelfResponse {
                        response: BlockSyncResponseMessage::HeadersResponse(
                            self.get_headers(block_range),
                        ),
                    });
                }
                LedgerCommand::LedgerFetchPayload(payload_id) => {
                    let _span = debug_span!("ledger fetch payload").entered();
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

impl<ST, SCT> Stream for MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;
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
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EthExecutionProtocol;
    type Event = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn get_finalized_blocks(
        &self,
    ) -> &BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>> {
        &self.finalized
    }
}
