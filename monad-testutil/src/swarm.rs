use std::{collections::BTreeMap, time::Duration};

use monad_block_sync::{BlockSyncProcess, BlockSyncState};
use monad_consensus_state::{ConsensusProcess, ConsensusState};
use monad_consensus_types::{
    block::BlockType, message_signature::MessageSignature, multi_sig::MultiSig,
    quorum_certificate::genesis_vote_info, signature_collection::SignatureCollection,
    transaction_validator::MockValidator, validation::Sha256Hash,
};
use monad_crypto::{
    secp256k1::{KeyPair, PubKey},
    NopSignature,
};
use monad_executor::{
    executor::mock::{MockExecutor, NoSerRouterScheduler},
    mock_swarm::Nodes,
    timed_event::TimedEvent,
    transformer::Pipeline,
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
        .zip(cert_keys.into_iter())
        .map(|(key, certkey)| MonadConfig {
            transaction_validator: TransactionValidatorType {},
            key,
            certkey,
            validators: validator_mapping
                .map
                .iter()
                .map(|(node_id, sctpubkey)| (node_id.0, *sctpubkey))
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
    ST: MessageSignature,
    SCT: SignatureCollection + PartialEq,
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

pub fn run_nodes<T: Pipeline<MM>>(num_nodes: u16, num_blocks: usize, delta: Duration, pipeline: T) {
    let (pubkeys, state_configs) = get_configs(num_nodes, delta);
    let peers = pubkeys
        .into_iter()
        .zip(state_configs)
        .zip(std::iter::repeat(MockWALoggerConfig {}))
        .map(|((a, b), c)| (a, b, c))
        .collect::<Vec<_>>();
    let mut nodes = Nodes::<MS, RS, T, PersistenceLoggerType>::new(peers, pipeline);

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

pub fn run_one_delayed_node<T: Pipeline<MM>>(
    pipeline: T,
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
        pipeline,
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
