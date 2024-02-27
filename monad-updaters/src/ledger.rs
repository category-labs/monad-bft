use std::{
    collections::{HashMap, VecDeque},
    marker::Unpin,
    ops::DerefMut,
    path::Path,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use heed::{Env as LmdbEnv, EnvOpenOptions};
use monad_blockdb::{
    BftLedgerTableType, BFT_LEDGER_TABLE_NAME, BLOCK_DB_MAP_SIZE, BLOCK_DB_NUM_DBS,
};
use monad_consensus_types::{
    block::{Block, BlockType},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::PubKey;
use monad_executor::Executor;
use monad_executor_glue::LedgerCommand;
use monad_proto::proto::block::ProtoBlock;
use monad_types::{BlockId, NodeId};
use prost::Message;
use tracing::warn;

/// A ledger for commited Monad Blocks
/// Purpose of the ledger is to have retrievable committed blocks to
/// respond the BlockSync requests
/// MockLedger stores the ledger in memory and is only expected to be used in testing
pub struct MockLedger<PT: PubKey, O: BlockType, E> {
    blockchain: Vec<O>,
    block_index: HashMap<BlockId, usize>,
    ledger_fetches: HashMap<(NodeId<PT>, BlockId), Box<dyn (FnOnce(Option<O>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<PT: PubKey, O: BlockType, E> Default for MockLedger<PT, O, E> {
    fn default() -> Self {
        Self {
            blockchain: Vec::new(),
            block_index: HashMap::new(),
            ledger_fetches: HashMap::default(),
            waker: None,
        }
    }
}

impl<PT: PubKey, O: BlockType, E> Executor for MockLedger<PT, O, E> {
    type Command = LedgerCommand<PT, O, E>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            LedgerCommand::LedgerFetch(..) => false,
            LedgerCommand::LedgerCommit(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(blocks) => {
                    for block in blocks {
                        self.block_index
                            .insert(block.get_id(), self.blockchain.len());
                        self.blockchain.push(block);
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
}

impl<PT: PubKey, O: BlockType, E> Stream for MockLedger<PT, O, E>
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

impl<PT: PubKey, O: BlockType, E> MockLedger<PT, O, E> {
    pub fn ready(&self) -> bool {
        !self.ledger_fetches.is_empty()
    }
    pub fn get_blocks(&self) -> &Vec<O> {
        &self.blockchain
    }
}

pub struct BlockDbLedger<SCT: SignatureCollection, PT: PubKey, E> {
    blockdb_env: LmdbEnv,
    recent_blocks: VecDeque<Block<SCT>>,
    max_size_recent_blocks: usize,
    ledger_fetches:
        HashMap<(NodeId<PT>, BlockId), Box<dyn (FnOnce(Option<Block<SCT>>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<SCT: SignatureCollection, PT: PubKey, E> BlockDbLedger<SCT, PT, E> {
    pub fn new(blockdb_path: &Path, max_size_recent_blocks: usize) -> Self {
        let blockdb_env = blockdb_init(blockdb_path).expect("db failed");

        Self {
            blockdb_env,
            recent_blocks: VecDeque::new(),
            max_size_recent_blocks,
            ledger_fetches: HashMap::default(),
            waker: None,
        }
    }
}

pub fn blockdb_store_bftblocks<SCT: SignatureCollection>(
    blockdb_env: LmdbEnv,
    blocks: Vec<Block<SCT>>,
) {
    let bftblock_table: BftLedgerTableType = blockdb_env
        .open_database(Some(BFT_LEDGER_TABLE_NAME))
        .expect("bftblock_table should exist")
        .unwrap();
    let mut bftblock_table_txn = blockdb_env
        .write_txn()
        .expect("bftblock_table txn create failed");

    for block in blocks {
        let pblock: ProtoBlock = (&block).into();
        let data = pblock.encode_to_vec();
        let block_id = block.get_id();

        bftblock_table
            .put(&mut bftblock_table_txn, &block_id.0, &data)
            .expect("bftblock table put failed");
    }
    bftblock_table_txn
        .commit()
        .expect("bftblock_table commit failed");
}

fn blockdb_init(blockdb_path: &Path) -> std::io::Result<LmdbEnv> {
    // TODO this won't work long term...monad-node should make the blockdb and then pass the
    // env around to executors that need it...so the path parameter here should change to a env
    // param
    let blockdb_env = EnvOpenOptions::new()
        .map_size(BLOCK_DB_MAP_SIZE)
        .max_dbs(BLOCK_DB_NUM_DBS)
        .open(blockdb_path)
        .expect("db failed");

    let _: BftLedgerTableType = blockdb_env
        .create_database(Some(BFT_LEDGER_TABLE_NAME))
        .unwrap();

    Ok(blockdb_env)
}

impl<SCT: SignatureCollection, PT: PubKey, E> Executor for BlockDbLedger<SCT, PT, E> {
    type Command = LedgerCommand<PT, Block<SCT>, E>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            LedgerCommand::LedgerFetch(..) => false,
            LedgerCommand::LedgerCommit(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(blocks) => {
                    // not awaiting this thread because
                    // lmdb has locking so multiple writer threads is handled there.
                    // we are only ever adding new blocks so order of writes does not matter
                    let lmdb_env = self.blockdb_env.clone();
                    let blocks_copy = blocks.clone();
                    tokio::task::spawn_blocking(move || {
                        blockdb_store_bftblocks(lmdb_env, blocks_copy);
                    });

                    for block in blocks {
                        if self.recent_blocks.len() >= self.max_size_recent_blocks {
                            self.recent_blocks.pop_back();
                        }
                        self.recent_blocks.push_front(block);
                    }

                    debug_assert!(self.recent_blocks.len() <= self.max_size_recent_blocks);
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
}

impl<SCT: SignatureCollection, PT: PubKey, E> Stream for BlockDbLedger<SCT, PT, E>
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

#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        env,
    };

    use futures::{FutureExt, StreamExt};
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey, PubKey},
        hasher::Hash,
        NopSignature,
    };
    use monad_executor::Executor;
    use monad_executor_glue::LedgerCommand;
    use monad_testutil::{block::MockBlock, signing::get_key};
    use monad_types::{BlockId, NodeId};
    use rand::{seq::SliceRandom, Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use test_case::test_case;

    use crate::ledger::MockLedger;

    type SignatureType = NopSignature;
    type PubKeyType = CertificateSignaturePubKey<SignatureType>;

    #[derive(Debug, PartialEq, Eq)]
    struct MockLedgerEvent<PT: PubKey> {
        pub requester: NodeId<PT>,
        pub bid: BlockId,
        pub block: Option<MockBlock<PT>>,
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
        let mut mock_ledger =
            MockLedger::<PubKeyType, MockBlock<PubKeyType>, MockLedgerEvent<PubKeyType>>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        let block = MockBlock::<PubKeyType>::new(
            monad_types::BlockId(Hash([0x00_u8; 32])),
            monad_types::BlockId(Hash([0x01_u8; 32])),
        );
        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![block])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            monad_types::BlockId(Hash([0x00_u8; 32])),
            Box::new(|block: Option<MockBlock<_>>| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: monad_types::BlockId(Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(Hash([0x00_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(Hash([0x01_u8; 32])),
        );
    }

    #[test]
    fn test_seeking_exist() {
        let mut mock_ledger =
            MockLedger::<PubKeyType, MockBlock<PubKeyType>, MockLedgerEvent<PubKeyType>>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![
            MockBlock::new(
                monad_types::BlockId(Hash([0x01_u8; 32])),
                monad_types::BlockId(Hash([0x00_u8; 32])),
            ),
            MockBlock::new(
                monad_types::BlockId(Hash([0x02_u8; 32])),
                monad_types::BlockId(Hash([0x01_u8; 32])),
            ),
            MockBlock::new(
                monad_types::BlockId(Hash([0x03_u8; 32])),
                monad_types::BlockId(Hash([0x02_u8; 32])),
            ),
            MockBlock::new(
                monad_types::BlockId(Hash([0x04_u8; 32])),
                monad_types::BlockId(Hash([0x03_u8; 32])),
            ),
        ])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            monad_types::BlockId(Hash([0x02_u8; 32])),
            Box::new(|block: Option<MockBlock<_>>| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: monad_types::BlockId(Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(Hash([0x02_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(Hash([0x01_u8; 32])),
        );

        // similarly, calling retrieve again always be viable
        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            monad_types::BlockId(Hash([0x02_u8; 32])),
            Box::new(|block: Option<MockBlock<_>>| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: monad_types::BlockId(Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(Hash([0x02_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(Hash([0x01_u8; 32])),
        );
    }
    #[test]
    fn test_seeking_non_exist() {
        let mut mock_ledger =
            MockLedger::<PubKeyType, MockBlock<PubKeyType>, MockLedgerEvent<PubKeyType>>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![
            MockBlock::new(
                monad_types::BlockId(Hash([0x01_u8; 32])),
                monad_types::BlockId(Hash([0x00_u8; 32])),
            ),
            MockBlock::new(
                monad_types::BlockId(Hash([0x02_u8; 32])),
                monad_types::BlockId(Hash([0x01_u8; 32])),
            ),
            MockBlock::new(
                monad_types::BlockId(Hash([0x03_u8; 32])),
                monad_types::BlockId(Hash([0x02_u8; 32])),
            ),
            MockBlock::new(
                monad_types::BlockId(Hash([0x04_u8; 32])),
                monad_types::BlockId(Hash([0x03_u8; 32])),
            ),
        ])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
            monad_types::BlockId(Hash([0x10_u8; 32])),
            Box::new(|block: Option<MockBlock<_>>| MockLedgerEvent {
                requester: monad_types::NodeId::new(get_key::<SignatureType>(0).pubkey()),
                bid: monad_types::BlockId(Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block;

        assert_eq!(mock_ledger_event, None);
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
        let mut mock_ledger =
            MockLedger::<PubKeyType, MockBlock<PubKeyType>, MockLedgerEvent<PubKeyType>>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        let blocks: Vec<_> = (1..40_u8)
            .map(|seed| {
                MockBlock::<PubKeyType>::new(
                    monad_types::BlockId(Hash([seed; 32])),
                    monad_types::BlockId(Hash([seed - 1; 32])),
                )
            })
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
                    .block_id;
                let node_id = monad_types::NodeId::new(key);
                reqs.push(LedgerCommand::LedgerFetch(
                    node_id,
                    bid,
                    Box::new(move |block: Option<MockBlock<_>>| MockLedgerEvent {
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
                            inserted_block.insert(b.block_id);
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
