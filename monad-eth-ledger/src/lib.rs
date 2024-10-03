use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloy_rlp::Decodable;
use futures::Stream;
use monad_consensus::messages::message::{
    BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockIdRange, BlockType, FullBlock},
    payload::{PayloadId, TransactionPayload},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_tx::EthTransaction;
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
    blocksyncable_range: Round,
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
            // TODO: make it configurable
            blocksyncable_range: Round(100),
            events: Default::default(),

            state,

            waker: Default::default(),
            _phantom: Default::default(),
        }
    }

    fn get_headers(&self, block_id_range: BlockIdRange) -> BlockSyncHeadersResponse<SCT> {
        // all blocks are stored in memory, facilitate blocksync for only the blocksyncable_range
        let latest_round = self
            .blocks
            .last_key_value()
            .map(|(r, _)| *r)
            .unwrap_or(Round(0));
        let last_blocksyncable_round =
            Round(latest_round.0.saturating_sub(self.blocksyncable_range.0));

        let final_block_id = block_id_range.from;
        let next_block_id = block_id_range.to;
        let mut headers = Vec::new();
        while next_block_id != final_block_id {
            let Some(block_round) = self.block_ids.get(&next_block_id) else {
                return BlockSyncHeadersResponse::NotAvailable(block_id_range);
            };

            if block_round < &last_blocksyncable_round {
                return BlockSyncHeadersResponse::NotAvailable(block_id_range);
            }

            let full_block = self
                .blocks
                .get(block_round)
                .expect("round to blockid invariant");

            headers.push(&full_block.block);
        }

        let headers = headers.into_iter().cloned().collect();
        BlockSyncHeadersResponse::Found((block_id_range, headers))
    }

    fn get_payload(&self, payload_id: PayloadId) -> BlockSyncPayloadResponse {
        // all payloads are stored in memory, facilitate blocksync for only the blocksyncable_range
        let latest_round = self
            .blocks
            .last_key_value()
            .map(|(r, _)| *r)
            .unwrap_or(Round(0));
        let last_blocksyncable_round =
            Round(latest_round.0.saturating_sub(self.blocksyncable_range.0));

        if let Some((_, full_block)) = self.blocks.iter().find(|(_, full_block)| {
            full_block.get_round() >= last_blocksyncable_round
                && full_block.get_payload_id() == payload_id
        }) {
            return BlockSyncPayloadResponse::Found((payload_id, full_block.payload.clone()));
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
                                Vec::<EthTransaction>::decode(&mut eth_txns_rlp.bytes().as_ref())
                                    .expect("invalid eth tx in block")
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
                LedgerCommand::LedgerFetchHeaders(block_id_range) => {
                    self.events.push_back(BlockSyncEvent::SelfResponse {
                        response: BlockSyncResponseMessage::HeadersResponse(
                            self.get_headers(block_id_range),
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
