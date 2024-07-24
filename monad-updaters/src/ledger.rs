use std::{
    collections::{HashMap, VecDeque},
    marker::{PhantomData, Unpin},
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
use monad_crypto::certificate_signature::PubKey;
use monad_eth_block_policy::{nonce::InMemoryState, EthBlockPolicy};
use monad_eth_block_validator::EthValidator;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::LedgerCommand;
use monad_types::{BlockId, NodeId};
use tracing::warn;

/// A ledger for commited Monad Blocks
/// Purpose of the ledger is to have retrievable committed blocks to
/// respond the BlockSync requests
/// MockLedger stores the ledger in memory and is only expected to be used in testing
pub struct MockLedger<SCT: SignatureCollection, PT: PubKey, E> {
    blockchain: Vec<Block<SCT>>,
    block_index: HashMap<BlockId, usize>,
    ledger_fetches:
        HashMap<(NodeId<PT>, BlockId), Box<dyn (FnOnce(Option<Block<SCT>>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    state: Option<Arc<Mutex<InMemoryState>>>,
    _pd: PhantomData<SCT>,
}
const GAUGE_LEDGER_NUM_COMMITS: &str = "monad.ledger.num_commits";

impl<SCT: SignatureCollection, PT: PubKey, E> Default for MockLedger<SCT, PT, E> {
    fn default() -> Self {
        Self {
            blockchain: Vec::new(),
            block_index: HashMap::new(),
            ledger_fetches: HashMap::default(),
            waker: None,
            metrics: Default::default(),
            state: None,
            _pd: PhantomData,
        }
    }
}

impl<SCT: SignatureCollection, PT: PubKey, E> MockLedger<SCT, PT, E> {
    pub fn with_state(state: Arc<Mutex<InMemoryState>>) -> Self {
        MockLedger {
            state: Some(state),
            ..Default::default()
        }
    }
}

impl<SCT: SignatureCollection, PT: PubKey, E> Executor for MockLedger<SCT, PT, E> {
    type Command = LedgerCommand<PT, Block<SCT>, E>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(blocks) => {
                    for block in blocks {
                        self.metrics[GAUGE_LEDGER_NUM_COMMITS] += 1;
                        self.block_index
                            .insert(block.get_id(), self.blockchain.len());
                        self.blockchain.push(block.clone());
                        if let Some(state) = &self.state {
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

                            let mut state = state.lock().unwrap();
                            state.update_committed_nonces(eth_block);
                        }
                    }
                }
                LedgerCommand::LedgerFetch(node_id, block_id, cb) => {
                    if self
                        .ledger_fetches
                        .insert((node_id, block_id), cb)
                        .is_some()
                    {
                        warn!(
                            "MockLedger received duplicate fetch from {:?} for block {:?}",
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

impl<SCT: SignatureCollection, PT: PubKey, E> Stream for MockLedger<SCT, PT, E>
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

impl<SCT: SignatureCollection, PT: PubKey, E> MockLedger<SCT, PT, E> {
    pub fn ready(&self) -> bool {
        !self.ledger_fetches.is_empty()
    }
    pub fn get_blocks(&self) -> &Vec<Block<SCT>> {
        &self.blockchain
    }
}

pub struct BoundedLedger<SCT: SignatureCollection, PT: PubKey, O: BlockType<SCT>, E> {
    recent_blocks: VecDeque<O>,
    max_blocks: usize,
    num_commits: usize,
    ledger_fetches: HashMap<(NodeId<PT>, BlockId), Box<dyn (FnOnce(Option<O>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
    metrics: ExecutorMetrics,
    _pd: PhantomData<SCT>,
}

impl<SCT: SignatureCollection, PT: PubKey, O: BlockType<SCT>, E> BoundedLedger<SCT, PT, O, E> {
    pub fn new(max_blocks: usize) -> Self {
        Self {
            recent_blocks: VecDeque::new(),
            max_blocks,
            num_commits: 0,
            ledger_fetches: HashMap::default(),
            waker: None,
            metrics: Default::default(),
            _pd: PhantomData,
        }
    }
}

impl<SCT: SignatureCollection, PT: PubKey, O: BlockType<SCT>, E> Executor
    for BoundedLedger<SCT, PT, O, E>
{
    type Command = LedgerCommand<PT, O, E>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(blocks) => {
                    self.num_commits += blocks.len();
                    for block in blocks {
                        if self.recent_blocks.len() >= self.max_blocks {
                            self.recent_blocks.pop_back();
                        }
                        self.recent_blocks.push_front(block);
                    }

                    debug_assert!(self.recent_blocks.len() <= self.max_blocks);
                }
                LedgerCommand::LedgerFetch(node_id, block_id, cb) => {
                    if self
                        .ledger_fetches
                        .insert((node_id, block_id), cb)
                        .is_some()
                    {
                        warn!(
                            "MockLedger received duplicate fetch from {:?} for block {:?}",
                            node_id, block_id
                        );
                    }
                }
            }
        }
        if !self.ledger_fetches.is_empty() {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<SCT: SignatureCollection, PT: PubKey, O: BlockType<SCT>, E> Stream
    for BoundedLedger<SCT, PT, O, E>
where
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some((node_id, block_id)) = this.ledger_fetches.keys().next().cloned() {
            let cb = this.ledger_fetches.remove(&(node_id, block_id)).unwrap();

            if let Some(fetched_block) = this.recent_blocks.iter().find(|&b| b.get_id() == block_id)
            {
                return Poll::Ready(Some(cb(Some(fetched_block.clone()))));
            }
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<SCT: SignatureCollection, PT: PubKey, O: BlockType<SCT>, E> BoundedLedger<SCT, PT, O, E> {
    pub fn get_num_commits(&self) -> usize {
        self.num_commits
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        env,
    };

    use futures::{FutureExt, StreamExt};
    use monad_consensus_types::{
        block::{Block, BlockType},
        payload::{ExecutionArtifacts, FullTransactionList},
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey, PubKey},
        hasher::Hash,
        NopSignature,
    };
    use monad_executor::Executor;
    use monad_executor_glue::LedgerCommand;
    use monad_multi_sig::MultiSig;
    use monad_testutil::{
        block::setup_block,
        signing::{get_key, node_id},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, NodeId, Round};
    use monad_validator::validator_set::ValidatorSetFactory;
    use rand::{seq::SliceRandom, Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use test_case::test_case;

    use crate::ledger::MockLedger;

    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;

    #[derive(Debug, PartialEq, Eq)]
    struct MockLedgerEvent<PT: PubKey> {
        pub requester: NodeId<PT>,
        pub bid: BlockId,
        pub block: Option<Block<SignatureCollectionType>>,
    }

    fn create_block(parent_id: BlockId) -> Block<SignatureCollectionType> {
        let (_, certkeys, _, vmap) = create_keys_w_validators::<
            SignatureType,
            SignatureCollectionType,
            _,
        >(1, ValidatorSetFactory::default());
        let author = node_id::<SignatureType>();
        let block_round = Round(5);
        let qc_round = Round(4);
        let txns = FullTransactionList::empty();
        let executor_header = ExecutionArtifacts::zero();

        setup_block::<SignatureType, SignatureCollectionType>(
            author,
            block_round,
            qc_round,
            parent_id,
            txns,
            executor_header,
            &certkeys,
            &vmap,
        )
    }

    #[test]
    #[ignore = "cron_test"]
    fn test_ledger_command_arbitrary_order_cron() {
        let round = match env::var("LEDGER_COMMAND_ARBITRARY_ORDER_ROUND") {
            Ok(v) => v.parse().unwrap(),
            Err(_e) => panic!("LEDGER_COMMAND_ARBITRARY_ORDER_ROUND is not set"),
        };

        match env::var("RANDOM_TEST_SEED") {
            Ok(v) => {
                let mut seed = v.parse().unwrap();
                let mut generator = ChaCha20Rng::seed_from_u64(seed);
                for _ in 0..round {
                    seed = generator.gen();
                    let nodes_req_range = generator.gen_range(1..1000);
                    println!(
                        "test_ledger_command_arbitrary_order, seed: {}, range: {} ",
                        seed, nodes_req_range
                    );
                    test_ledger_command_arbitrary_order(seed, nodes_req_range);
                }
            }
            Err(_e) => panic!("RANDOM_TEST_SEED is not set"),
        };
    }

    #[test]
    fn test_basic_stream_functionality() {
        let mut mock_ledger = MockLedger::<
            SignatureCollectionType,
            PubKeyType,
            MockLedgerEvent<PubKeyType>,
        >::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        let block = create_block(BlockId(Hash([0x01_u8; 32])));
        let bid = block.get_id();

        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![block.clone()])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            bid,
            Box::new(
                move |block: Option<Block<SignatureCollectionType>>| MockLedgerEvent {
                    requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                    bid,
                    block,
                },
            ),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let retrieved_block = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(retrieved_block, block);
    }

    #[test]
    fn test_seeking_exist() {
        let mut mock_ledger = MockLedger::<
            SignatureCollectionType,
            PubKeyType,
            MockLedgerEvent<PubKeyType>,
        >::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        let b1 = create_block(BlockId(Hash([0x01_u8; 32])));
        let b2_base = create_block(b1.get_id());
        let b2 = b2_base.clone();
        let b3 = create_block(b2.get_id());
        let b4 = create_block(b3.get_id());

        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![b1, b2, b3, b4])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        let b2_id = b2_base.get_id();
        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            b2_id,
            Box::new(move |block| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: b2_id,
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let retrieved_block = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(retrieved_block, b2_base);

        let b2_id = b2_base.get_id();
        // similarly, calling retrieve again always be viable
        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            b2_id,
            Box::new(move |block| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: b2_id,
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let retrieved_block = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(retrieved_block, b2_base);
    }
    #[test]
    fn test_seeking_non_exist() {
        let mut mock_ledger = MockLedger::<
            SignatureCollectionType,
            PubKeyType,
            MockLedgerEvent<PubKeyType>,
        >::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        let b1 = create_block(BlockId(Hash([0x01_u8; 32])));
        let b2 = create_block(b1.get_id());
        let b3 = create_block(b2.get_id());
        let b4 = create_block(b3.get_id());

        let committed_blocks = vec![b1, b2, b3, b4];
        let fetch_id = BlockId(Hash([0x10_u8; 32]));
        assert!(!committed_blocks
            .iter()
            .map(|b| b.get_id())
            .collect::<Vec<_>>()
            .contains(&fetch_id));

        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(committed_blocks)]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            fetch_id,
            Box::new(move |block| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: fetch_id,
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let retrived_block = retrieved.unwrap().unwrap().block;

        assert_eq!(retrived_block, None);
    }

    /**
     *  Fuzz testing on a set of request that is being shuffled into arbitrary order
     */
    #[test_case(123123, 12; "test 1")]
    #[test_case(345345345, 23; "test 2")]
    #[test_case(346346353, 100; "test 3")]
    #[test_case(343413452, 100; "test 4")]
    #[test_case(23452345245, 64; "test 5")]
    #[test_case(342342, 67; "test 6")]
    #[test_case(346243632, 97; "test 7")]
    #[test_case(23534366, 44; "test 8")]
    #[test_case(457643563, 2; "test 9")]
    #[test_case(965864809, 1; "test 10")]
    fn test_ledger_command_arbitrary_order(seed: u64, node_req_range: i32) {
        assert!(node_req_range > 0);
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        let poll_pref = rng.gen_range(0.0..1.0);
        let mut mock_ledger = MockLedger::<
            SignatureCollectionType,
            PubKeyType,
            MockLedgerEvent<PubKeyType>,
        >::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        let blocks: Vec<_> = (1..40_u8)
            .map(|seed| create_block(BlockId(Hash([seed; 32]))))
            .collect();

        let pub_keys: Vec<_> = (0..100)
            .map(|i| get_key::<SignatureType>(i).pubkey())
            .collect();
        let mut callback_map = HashMap::new();
        let mut inserted_block = HashSet::new();
        let mut requests = pub_keys.into_iter().fold(vec![], |mut reqs, key| {
            for _ in 0..rng.gen_range(1..node_req_range + 1) {
                let bid = blocks
                    .choose(&mut rng)
                    .expect("at least 1 element is within blocks")
                    .get_id();
                let node_id = monad_types::NodeId::new(key);
                reqs.push(LedgerCommand::LedgerFetch(
                    node_id,
                    bid,
                    Box::new(move |block| MockLedgerEvent {
                        requester: node_id,
                        bid,
                        block,
                    }),
                ));
            }

            reqs
        });
        requests.extend(
            blocks
                .clone()
                .into_iter()
                .map(|b| LedgerCommand::LedgerCommit(vec![b])),
        );
        requests.shuffle(&mut rng);

        while !requests.is_empty() || mock_ledger.ready() {
            if rng.gen_bool(poll_pref) && !requests.is_empty() {
                let request = requests.remove(0);
                match request {
                    LedgerCommand::LedgerFetch(id, bid, callback) => {
                        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(id, bid, callback)]);
                        *callback_map.entry((id, bid)).or_insert(0) = 1;
                    }
                    LedgerCommand::LedgerCommit(blocks) => {
                        for b in blocks.iter() {
                            inserted_block.insert(b.get_id());
                        }
                        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(blocks)]);
                    }
                };
            } else if let Some(retrieved) = mock_ledger.next().now_or_never() {
                let result = retrieved.unwrap();
                let block_id = result.bid;
                let requester = result.requester;

                if result.block.is_none() {
                    assert!(!inserted_block.contains(&block_id))
                }

                if let Entry::Occupied(mut entry) = callback_map.entry((requester, block_id)) {
                    *entry.get_mut() = 0;
                } else {
                    panic!("requesting a block that was not planed")
                }
            }
        }
        for (_, v) in callback_map {
            // its possible that same block is requested twice by the same user
            // but only 1 reset is needed since the duplicate request should be ignored
            assert!(v == 0)
        }
        assert!(requests.is_empty());
        assert!(mock_ledger.blockchain.len() == blocks.len());
        assert!(!mock_ledger.ready());
    }
}
