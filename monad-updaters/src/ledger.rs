use std::{
    collections::HashMap,
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::block::BlockType;
use monad_executor::Executor;
use monad_executor_glue::LedgerCommand;
use monad_types::{BlockId, NodeId};
use tracing::warn;

pub struct MockLedger<O: BlockType, E> {
    blockchain: Vec<O>,
    block_index: HashMap<BlockId, usize>,
    ledger_fetches: HashMap<(NodeId, BlockId), Box<dyn (FnOnce(Option<O>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<O: BlockType, E> Default for MockLedger<O, E> {
    fn default() -> Self {
        Self {
            blockchain: Vec::new(),
            block_index: HashMap::new(),
            ledger_fetches: HashMap::default(),
            waker: None,
        }
    }
}

impl<O: BlockType, E> Executor for MockLedger<O, E> {
    type Command = LedgerCommand<O, E>;

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
                LedgerCommand::LedgerFetchReset(node_id, block_id) => {
                    self.ledger_fetches.remove(&(node_id, block_id));
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

impl<O: BlockType, E> Stream for MockLedger<O, E>
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

impl<O: BlockType, E> MockLedger<O, E> {
    pub fn ready(&self) -> bool {
        !self.ledger_fetches.is_empty()
    }
    pub fn get_blocks(&self) -> &Vec<O> {
        &self.blockchain
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        env,
    };

    use futures::{FutureExt, StreamExt};
    use monad_executor::Executor;
    use monad_executor_glue::LedgerCommand;
    use monad_testutil::{block::MockBlock, signing::get_key};
    use monad_types::{BlockId, NodeId};
    use rand::{seq::SliceRandom, Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use test_case::test_case;

    use crate::ledger::MockLedger;

    #[derive(Debug, PartialEq, Eq)]
    struct MockLedgerEvent {
        pub requester: NodeId,
        pub bid: BlockId,
        pub block: Option<MockBlock>,
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
                    let nodes_req_range = generator.gen_range(0..1000);
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
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        let block = MockBlock {
            block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        };
        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![block])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId(get_key(0).pubkey()),
            monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent {
                requester: monad_types::NodeId(get_key(0).pubkey()),
                bid: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        );
    }

    #[test]
    fn test_seeking_exist() {
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline
        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            },
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
            },
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            },
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x04_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
            },
        ])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId(get_key(0).pubkey()),
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent {
                requester: monad_types::NodeId(get_key(0).pubkey()),
                bid: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        );

        // similarly, calling retrieve again always be viable
        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId(get_key(0).pubkey()),
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent {
                requester: monad_types::NodeId(get_key(0).pubkey()),
                bid: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
                block,
            }),
        )]);
        let retrieved = mock_ledger.next().now_or_never();
        assert_ne!(retrieved, None); // there should be a response
        let mock_ledger_event = retrieved.unwrap().unwrap().block.unwrap();
        assert_eq!(
            mock_ledger_event.block_id,
            monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
        );
        assert_eq!(
            mock_ledger_event.parent_block_id,
            monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
        );
    }
    #[test]
    fn test_seeking_non_exist() {
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        mock_ledger.exec(vec![LedgerCommand::LedgerCommit(vec![
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
            },
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x01_u8; 32])),
            },
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x02_u8; 32])),
            },
            MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([0x04_u8; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([0x03_u8; 32])),
            },
        ])]);
        assert_eq!(mock_ledger.next().now_or_never(), None); // ledger commit shouldn't cause any event

        mock_ledger.exec(vec![LedgerCommand::LedgerFetch(
            monad_types::NodeId(get_key(0).pubkey()),
            monad_types::BlockId(monad_types::Hash([0x10_u8; 32])),
            Box::new(|block: Option<MockBlock>| MockLedgerEvent {
                requester: monad_types::NodeId(get_key(0).pubkey()),
                bid: monad_types::BlockId(monad_types::Hash([0x00_u8; 32])),
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
        let mut mock_ledger = MockLedger::<MockBlock, MockLedgerEvent>::default();
        assert_eq!(mock_ledger.next().now_or_never(), None); // nothing should be within the pipeline

        let blocks: Vec<_> = (1..40_u8)
            .map(|seed| MockBlock {
                block_id: monad_types::BlockId(monad_types::Hash([seed; 32])),
                parent_block_id: monad_types::BlockId(monad_types::Hash([seed - 1; 32])),
            })
            .collect();

        let pub_keys: Vec<_> = (0..100).map(|i| get_key(i).pubkey()).collect();
        let mut callback_map = HashMap::new();
        let mut inserted_block = HashSet::new();
        let mut requests = pub_keys.into_iter().fold(vec![], |mut reqs, key| {
            for _ in 0..rng.gen_range(1..node_req_range + 1) {
                let bid = blocks
                    .choose(&mut rng)
                    .expect("at least 1 element is within blocks")
                    .block_id;
                let node_id = monad_types::NodeId(key);
                reqs.push(LedgerCommand::LedgerFetch(
                    node_id,
                    bid,
                    Box::new(move |block: Option<MockBlock>| MockLedgerEvent {
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
                    LedgerCommand::LedgerFetchReset(id, bid) => {
                        mock_ledger.exec(vec![LedgerCommand::LedgerFetchReset(id, bid)]);
                        if let Entry::Occupied(mut entry) = callback_map.entry((id, bid)) {
                            *entry.get_mut() = 0;
                        } else {
                            panic!("requesting a block that was not planed")
                        }
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
                // now also insert the fetch reset
                if requests.is_empty() {
                    requests.push(LedgerCommand::LedgerFetchReset(requester, block_id))
                } else {
                    requests.insert(
                        rng.gen_range(0..requests.len()),
                        LedgerCommand::LedgerFetchReset(requester, block_id),
                    )
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
