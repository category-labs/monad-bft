use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use monad_block_sync::{BlockSyncProcess, BlockSyncState};
use monad_consensus_state::{ConsensusProcess, ConsensusState};
use monad_consensus_types::{
    certificate_signature::{CertificateKeyPair, CertificateSignatureRecoverable},
    message_signature::MessageSignature,
    multi_sig::MultiSig,
    quorum_certificate::genesis_vote_info,
    signature_collection::SignatureCollection,
    transaction_validator::MockValidator,
    validation::Sha256Hash,
};
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    NopSignature,
};
use monad_executor::{
    executor::mock::{MockExecutor, NoSerRouterScheduler},
    mock_swarm::{LinkMessage, Nodes, Transformer},
    timed_event::TimedEvent,
    PeerId, State,
};
use monad_state::{MonadConfig, MonadEvent, MonadMessage, MonadState};
use monad_types::NodeId;
use monad_validator::{
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSet, ValidatorSetType},
};
use monad_wal::{
    mock::{MockWALogger, MockWALoggerConfig},
    PersistenceLogger,
};
use rand::{prelude::SliceRandom, SeedableRng};
use rand_chacha::ChaChaRng;

use crate::{signing::get_genesis_config, validators::create_keys_w_validators};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;
type TransactionValidatorType = MockValidator;
type MS = MonadState<
    ConsensusState<SignatureType, SignatureCollectionType, TransactionValidatorType>,
    SignatureType,
    SignatureCollectionType,
    ValidatorSet,
    SimpleRoundRobin,
    BlockSyncState,
>;
type MC = MonadConfig<SignatureCollectionType, TransactionValidatorType>;
type MM = <MS as State>::Message;
type RS = NoSerRouterScheduler<MM>;
type PersistenceLoggerType =
    MockWALogger<TimedEvent<MonadEvent<SignatureType, SignatureCollectionType>>>;

pub enum TransformerReplayOrder {
    Forward,
    Reverse,
    Random(u64),
}

pub struct PartitionThenReplayTransformer<
    ST: MessageSignature + CertificateSignatureRecoverable,
    SCT: SignatureCollection<SignatureType = ST>,
> {
    pub peers: HashSet<PeerId>,
    pub filtered_msgs: Vec<LinkMessage<MonadMessage<ST, SCT>>>,
    pub cnt: u32,
    pub cnt_limit: u32,
    pub order: TransformerReplayOrder,
}

impl<ST, SCT> PartitionThenReplayTransformer<ST, SCT>
where
    ST: MessageSignature + CertificateSignatureRecoverable,
    SCT: SignatureCollection<SignatureType = ST>,
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

impl<ST, SCT> Transformer<MonadMessage<ST, SCT>> for PartitionThenReplayTransformer<ST, SCT>
where
    ST: MessageSignature + CertificateSignatureRecoverable,
    SCT: SignatureCollection<SignatureType = ST>,
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
) -> (Vec<PubKey>, Vec<MonadConfig<SCT, TransactionValidatorType>>) {
    let (keys, cert_keys, _validators, validator_mapping) =
        create_keys_w_validators::<SCT>(num_nodes as u32);
    let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
    let voting_keys = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(cert_keys.iter())
        .collect::<Vec<_>>();

    let (genesis_block, genesis_sigs) =
        get_genesis_config::<Sha256Hash, SCT>(voting_keys.iter(), &validator_mapping);

    let state_configs = keys
        .into_iter()
        .map(|key| MonadConfig {
            transaction_validator: TransactionValidatorType {},
            key,
            validators: voting_keys
                .iter()
                .map(|(node_id, k)| (node_id.0, k.pubkey()))
                .collect::<Vec<_>>(),
            delta,
            genesis_block: genesis_block.clone(),
            genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
            genesis_signatures: genesis_sigs.clone(),
        })
        .collect::<Vec<_>>();

    (pubkeys, state_configs)
}

pub fn node_ledger_verification<
    CT: ConsensusProcess<ST, SCT>,
    ST: MessageSignature + CertificateSignatureRecoverable,
    SCT: SignatureCollection<SignatureType = ST> + PartialEq,
    VT: ValidatorSetType,
    LT: LeaderElection,
    BST: BlockSyncProcess<ST, SCT, VT>,
    PL: PersistenceLogger,
>(
    states: &BTreeMap<
        PeerId,
        (
            MockExecutor<
                MonadState<CT, ST, SCT, VT, LT, BST>,
                NoSerRouterScheduler<MonadMessage<ST, SCT>>,
            >,
            MonadState<CT, ST, SCT, VT, LT, BST>,
            PL,
        ),
    >,
) {
    let num_b = states
        .values()
        .map(|v| v.0.ledger().get_blocks().len())
        .min()
        .unwrap();
    let max_b = states
        .values()
        .map(|v| v.0.ledger().get_blocks().len())
        .max()
        .unwrap();

    assert!(max_b - num_b <= 5); // this 5 block bound is arbitrary... is there a better way to do
                                 // this?

    let b = states.values().next().unwrap().0.ledger().get_blocks();
    for n in states {
        let a = n.1 .0.ledger().get_blocks();

        assert!(!b.is_empty());
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
    let mut nodes = Nodes::<MS, RS, T, PersistenceLoggerType>::new(peers, transformer);

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
    let mut nodes = Nodes::<MS, RS, T, PersistenceLoggerType>::new(
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
