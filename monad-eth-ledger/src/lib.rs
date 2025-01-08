use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloy_consensus::Transaction;
use futures::Stream;
use monad_blocksync::messages::message::{
    BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockRange, BlockType, FullBlock},
    payload::{PayloadId, TransactionPayload},
    quorum_certificate::GENESIS_BLOCK_ID,
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_tx::EthFullTransactionList;
use monad_eth_types::EthAddress;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_state_backend::InMemoryState;
use monad_types::{BlockId, Round};
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
    blocks: BTreeMap<Round, FullBlock<SCT>>,
    block_ids: HashMap<BlockId, Round>,
    events: VecDeque<BlockSyncEvent<SCT>>,

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
            block_ids: Default::default(),
            events: Default::default(),

            state,

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
            return BlockSyncPayloadResponse::Found(full_block.payload.clone());
        }

        BlockSyncPayloadResponse::NotAvailable(payload_id)
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
                        if let TransactionPayload::List(eth_txns_rlp) = &block.payload.txns {
                            // generate eth block and update the state backend with committed nonces
                            let new_account_nonces =
                                EthFullTransactionList::rlp_decode(eth_txns_rlp.bytes().clone())
                                    .expect("invalid eth tx in block")
                                    .0
                                    .into_iter()
                                    .map(|tx| (EthAddress(tx.signer()), tx.nonce() + 1))
                                    // collecting into a map will handle a sender sending multiple
                                    // transactions gracefully
                                    //
                                    // this is because nonces are always increasing per account
                                    .collect();
                            let mut state = self.state.lock().unwrap();
                            state.ledger_commit(block.get_seq_num(), new_account_nonces);
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

    fn get_blocks(&self) -> &BTreeMap<Round, FullBlock<SCT>> {
        &self.blocks
    }
}
