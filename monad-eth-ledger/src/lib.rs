use std::{
    collections::HashMap,
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::{Block, BlockType},
    block_validator::BlockValidator,
    signature_collection::SignatureCollection,
};
use monad_eth_block_policy::{nonce::InMemoryState, EthBlockPolicy};
use monad_eth_block_validator::EthValidator;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::LedgerCommand;
use monad_types::{BlockId, NodeId};
use monad_updaters::ledger::MockableLedger;
use tracing::warn;

/// A ledger for commited Monad Blocks
/// Purpose of the ledger is to have retrievable committed blocks to
/// respond the BlockSync requests
/// MockEthLedger stores the ledger in memory and is only expected to be used in testing
pub struct MockEthLedger<SCT: SignatureCollection, E> {
    blockchain: Vec<Block<SCT>>,
    block_index: HashMap<BlockId, usize>,
    ledger_fetches: HashMap<
        (NodeId<SCT::NodeIdPubKey>, BlockId),
        Box<dyn (FnOnce(Option<Block<SCT>>) -> E) + Send + Sync>,
    >,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    state: Arc<Mutex<InMemoryState>>,
}
const GAUGE_LEDGER_NUM_COMMITS: &str = "monad.ledger.num_commits";

impl<SCT: SignatureCollection, E> MockEthLedger<SCT, E> {
    pub fn new(state: Arc<Mutex<InMemoryState>>) -> Self {
        MockEthLedger {
            blockchain: Vec::new(),
            block_index: HashMap::new(),
            ledger_fetches: HashMap::default(),
            waker: None,
            metrics: Default::default(),
            state,
        }
    }
}

impl<SCT: SignatureCollection, E> Executor for MockEthLedger<SCT, E> {
    type Command = LedgerCommand<SCT::NodeIdPubKey, Block<SCT>, E>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(blocks) => {
                    for block in blocks {
                        self.metrics[GAUGE_LEDGER_NUM_COMMITS] += 1;
                        self.block_index
                            .insert(block.get_id(), self.blockchain.len());
                        self.blockchain.push(block.clone());
                        // generate eth block and update the state backend with committed nonces
                        let eth_block = <EthValidator as BlockValidator<
                            SCT,
                            InMemoryState,
                            EthBlockPolicy,
                        >>::validate(
                            &EthValidator {
                                tx_limit: usize::MAX,
                                block_gas_limit: u64::MAX,
                            },
                            block,
                        )
                        .expect("committed block is valid with max limits");

                        let mut state = self.state.lock().unwrap();
                        state.update_committed_nonces(eth_block);
                    }
                }
                LedgerCommand::LedgerFetch(node_id, block_id, cb) => {
                    if self
                        .ledger_fetches
                        .insert((node_id, block_id), cb)
                        .is_some()
                    {
                        warn!(
                            "MockEthLedger received duplicate fetch from {:?} for block {:?}",
                            node_id, block_id
                        );
                    }
                }
            }
        }
        if self.ready() {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<SCT: SignatureCollection, E> Stream for MockEthLedger<SCT, E>
where
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some((node_id, block_id)) = this.ledger_fetches.keys().next().cloned() {
            let cb = this.ledger_fetches.remove(&(node_id, block_id)).unwrap();

            return Poll::Ready(Some(cb({
                this.block_index
                    .get(&block_id)
                    .map(|idx| this.blockchain[*idx].clone())
            })));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<SCT: SignatureCollection, E> MockableLedger<Block<SCT>> for MockEthLedger<SCT, E> {
    type SignatureCollection = SCT;
    type Event = E;

    fn ready(&self) -> bool {
        !self.ledger_fetches.is_empty()
    }
    fn get_blocks(&self) -> &Vec<Block<SCT>> {
        &self.blockchain
    }
}
