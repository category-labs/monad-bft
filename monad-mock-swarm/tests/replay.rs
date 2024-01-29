use std::{
    collections::{BTreeSet, HashMap},
    fs::create_dir_all,
    time::Duration,
};

use monad_consensus_types::{
    block::{Block, BlockType},
    block_validator::MockValidator,
    payload::NopStateRoot,
    txpool::MockTxPool,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    NopSignature,
};
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock_swarm::SwarmBuilder, node::NodeBuilder, swarm_relation::SwarmRelation,
    terminator::UntilTerminator,
};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{
    GenericTransformer, GenericTransformerPipeline, LatencyTransformer, XorLatencyTransformer, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};
use monad_wal::wal::{WALogger, WALoggerConfig};
use tempfile::tempdir;

struct ReplaySwarm;

impl SwarmRelation for ReplaySwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

    type BlockValidator = MockValidator;
    type StateRootValidator = NopStateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::SignatureCollectionType,
    >;
    type TxPool = MockTxPool;

    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type Logger = WALogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

#[test]
fn test_replay() {
    recover_nodes_msg_delays(4, 10, 5, 0, SeqNum(2000), Round(50));
}

pub fn recover_nodes_msg_delays(
    num_nodes: u16,
    num_blocks_before: usize,
    num_block_after: usize,
    proposal_size: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) {
    let state_configs = make_state_configs::<ReplaySwarm>(
        num_nodes, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || NopStateRoot,
        Duration::from_millis(101), // delta
        proposal_size,              // proposal_tx_limit
        val_set_update_interval,    // val_set_update_interval
        epoch_start_delay,          // epoch_start_delay
    );

    // create the log file path
    let mut logger_configs = Vec::new();
    let tmpdir = tempdir().unwrap();
    create_dir_all(tmpdir.path()).unwrap();
    for i in 0..num_nodes {
        logger_configs.push(WALoggerConfig::new(
            tmpdir.path().join(format!("wal{}", i)),
            false, // sync
        ));
    }

    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<ReplaySwarm>(
        state_configs
            .into_iter()
            .zip(logger_configs.clone())
            .enumerate()
            .map(|(seed, (state_builder, logger_config))| {
                let validators = state_builder.validators.clone();
                NodeBuilder::<ReplaySwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    logger_config,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators, val_set_update_interval),
                    vec![GenericTransformer::XorLatency(XorLatencyTransformer::new(
                        Duration::from_millis(u8::MAX as u64),
                    ))],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut nodes = swarm_config.build();
    let mut term = UntilTerminator::new().until_block(num_blocks_before);
    while nodes.step_until(&mut term).is_some() {}

    // can skip this verification so we don't have two cases failing for the same reason
    let node_ledger_before = nodes
        .states()
        .iter()
        .map(|(peerid, node)| {
            (
                *peerid,
                node.executor
                    .ledger()
                    .get_blocks()
                    .iter()
                    .map(|b| b.get_id())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    // drop the nodes -> close the files
    drop(nodes);

    let state_configs_clone = make_state_configs::<ReplaySwarm>(
        num_nodes, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || NopStateRoot,
        Duration::from_millis(2), // delta
        proposal_size,            // proposal_tx_limit
        val_set_update_interval,  // val_set_update_interval
        epoch_start_delay,        // epoch_start_delay
    );

    let swarm_config_clone = SwarmBuilder::<ReplaySwarm>(
        state_configs_clone
            .into_iter()
            .zip(logger_configs)
            .enumerate()
            .map(|(seed, (state_builder, logger_config))| {
                let validators = state_builder.validators.clone();
                NodeBuilder::<ReplaySwarm>::new(
                    ID::new(NodeId::new(state_builder.key.pubkey())),
                    state_builder,
                    logger_config,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators, val_set_update_interval),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(1),
                    ))],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut nodes_recovered = swarm_config_clone.build();

    let node_ledger_recovered = nodes_recovered
        .states()
        .iter()
        .map(|(peerid, node)| {
            (
                *peerid,
                node.executor
                    .ledger()
                    .get_blocks()
                    .iter()
                    .map(|b| b.get_id())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    assert_eq!(node_ledger_before, node_ledger_recovered);
    let mut term = UntilTerminator::new().until_block(num_blocks_before + num_block_after);
    while nodes_recovered.step_until(&mut term).is_some() {}

    swarm_ledger_verification(&nodes_recovered, 1);
}
