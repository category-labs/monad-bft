use rand::prelude::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use std::{collections::BTreeMap, collections::HashSet, time::Duration};

use monad_consensus::{
    signatures::aggregate_signature::AggregateSignatures,
    types::{quorum_certificate::genesis_vote_info, signature::SignatureCollection},
    validation::hashing::Sha256Hash,
};
use monad_crypto::{secp256k1::KeyPair, secp256k1::PubKey, NopSignature, Signature};
use monad_executor::{
    executor::mock::MockExecutor,
    mock_swarm::{LinkMessage, Nodes, Transformer},
    PeerId, State,
};
use monad_state::{MonadConfig, MonadEvent, MonadMessage, MonadState};
use monad_testutil::signing::{create_keys, get_genesis_config};
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
use monad_wal::PersistenceLogger;

type SignatureType = NopSignature;
type SignatureCollectionType = AggregateSignatures<SignatureType>;
type MS = MonadState<SignatureType, SignatureCollectionType>;
type MC = MonadConfig<SignatureCollectionType>;
type MM = <MS as State>::Message;
type PersistenceLoggerType = MockWALogger<MonadEvent<SignatureType, SignatureCollectionType>>;

pub enum TransformerReplayOrder {
    Forward,
    Reverse,
    Random(u64),
}

pub struct PartitionThenReplayTransformer<
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST>,
> {
    pub peers: HashSet<PeerId>,
    pub filtered_msgs: Vec<LinkMessage<MonadMessage<ST, SCT>>>,
    pub cnt: u32,
    pub cnt_limit: u32,
    pub order: TransformerReplayOrder,
}

impl<ST: Signature, SCT: SignatureCollection<SignatureType = ST>>
    PartitionThenReplayTransformer<ST, SCT>
{
    pub fn new(peers: HashSet<PeerId>, cnt_limit: u32, order: TransformerReplayOrder) -> Self {
        PartitionThenReplayTransformer {
            peers,
            filtered_msgs: Vec::new(),
            cnt: 0,
            cnt_limit,
            order,
        }
    }
}

impl<ST: Signature, SCT: SignatureCollection<SignatureType = ST>> Transformer<MonadMessage<ST, SCT>>
    for PartitionThenReplayTransformer<ST, SCT>
{
    fn transform(
        &mut self,
        message: LinkMessage<MonadMessage<ST, SCT>>,
    ) -> Vec<(Duration, LinkMessage<MonadMessage<ST, SCT>>)> {
        if self.cnt > self.cnt_limit {
            return vec![(Duration::ZERO, message)];
        }

        self.cnt += 1;
        let mut output = Vec::new();
        if !self.peers.contains(&message.from) && !self.peers.contains(&message.to) {
            output.push((Duration::ZERO, message))
        } else {
            self.filtered_msgs.push(message);
        }

        if self.cnt > self.cnt_limit {
            let msgs = match self.order {
                TransformerReplayOrder::Forward => {
                    self.filtered_msgs.clone().into_iter().collect::<Vec<_>>()
                }
                TransformerReplayOrder::Reverse => self
                    .filtered_msgs
                    .clone()
                    .into_iter()
                    .rev()
                    .collect::<Vec<_>>(),
                TransformerReplayOrder::Random(seed) => {
                    let mut gen = ChaChaRng::seed_from_u64(seed);
                    let mut s = self.filtered_msgs.clone().into_iter().collect::<Vec<_>>();
                    s.shuffle(&mut gen);
                    s
                }
            };

            output.extend(std::iter::repeat(Duration::ZERO).zip(msgs));
        }

        output
    }
}

pub fn get_configs<SCT: SignatureCollection>(
    num_nodes: u16,
    delta: Duration,
) -> (Vec<PubKey>, Vec<MonadConfig<SCT>>) {
    let keys = create_keys(num_nodes as u32);
    let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
    let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SCT>(keys.iter());

    let state_configs = keys
        .into_iter()
        .zip(std::iter::repeat(pubkeys.clone()))
        .map(|(key, pubkeys)| MonadConfig {
            key,
            validators: pubkeys,

            delta,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_sigs.clone(),
        })
        .collect::<Vec<_>>();

    (pubkeys, state_configs)
}

pub fn node_ledger_verification<
    ST: Signature,
    SCT: SignatureCollection<SignatureType = ST> + PartialEq,
    PL: PersistenceLogger,
>(
    states: &BTreeMap<PeerId, (MockExecutor<MonadState<ST, SCT>>, MonadState<ST, SCT>, PL)>,
) {
    let num_b = states
        .values()
        .map(|v| v.0.ledger().get_blocks().len())
        .min()
        .unwrap();

    for n in states {
        let a = n.1 .0.ledger().get_blocks();
        let b = states.values().next().unwrap().0.ledger().get_blocks();

        assert!(!b.is_empty());
        assert!((a.len() as i32).abs_diff(b.len() as i32) <= 1);
        assert!(a.iter().take(num_b).eq(b.iter().take(num_b)));
    }
}

pub fn run_nodes<T: Transformer<MM>>(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    transformer: T,
) {
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);
    let peers = pubkeys
        .into_iter()
        .zip(state_configs)
        .zip(std::iter::repeat(MockWALoggerConfig {}))
        .map(|((a, b), c)| (a, b, c))
        .collect::<Vec<_>>();
    let mut nodes = Nodes::<MS, T, PersistenceLoggerType>::new(peers, transformer);

    while let Some((duration, id, event)) = nodes.step() {
        if nodes
            .states()
            .values()
            .next()
            .unwrap()
            .0
            .ledger()
            .get_blocks()
            .len()
            > num_blocks
        {
            break;
        }
    }

    node_ledger_verification(nodes.states());
}

pub fn run_one_delayed_node<T: Transformer<MM>>(
    transformer: T,
    pubkeys: Vec<PubKey>,
    state_configs: Vec<MC>,
) {
    let mut nodes = Nodes::<MS, T, PersistenceLoggerType>::new(
        pubkeys
            .into_iter()
            .zip(state_configs)
            .zip(std::iter::repeat(MockWALoggerConfig {}))
            .map(|((a, b), c)| (a, b, c))
            .collect(),
        transformer,
    );

    let mut cnt = 0;
    while let Some((_duration, _id, _event)) = nodes.step() {
        cnt += 1;
        if cnt > 400 {
            break;
        }
    }

    node_ledger_verification(nodes.states());
}
