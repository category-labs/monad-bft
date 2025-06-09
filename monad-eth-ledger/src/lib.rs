use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::Executor;
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::{InMemoryState, StateBackendTest};
use monad_types::{BlockId, Round, SeqNum};
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
    blocks: BTreeMap<Round, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    finalized: BTreeMap<SeqNum, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    block_ids: HashMap<BlockId, Round>,
    events: VecDeque<BlockSyncEvent<ST, SCT, EthExecutionProtocol>>,

    state: InMemoryState,

    waker: Option<Waker>,
    _phantom: PhantomData<ST>,
}

impl<ST, SCT> MockEthLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(state: InMemoryState) -> Self {
        MockEthLedger {
            blocks: Default::default(),
            finalized: Default::default(),
            block_ids: Default::default(),
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
    type Metrics = ();

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerClearWal => {}
                LedgerCommand::LedgerCommit(OptimisticCommit::Proposed(block)) => {
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
                        block.get_round(),
                        block.get_parent_round(),
                        new_account_nonces,
                    );

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
                LedgerCommand::LedgerCommit(OptimisticCommit::Finalized(block)) => {
                    self.finalized.insert(block.get_seq_num(), block.clone());
                    let mut state = self.state.lock().unwrap();
                    state.ledger_commit(&block.get_id());
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

    fn metrics(&self) -> &Self::Metrics {
        &()
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
