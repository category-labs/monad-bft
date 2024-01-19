use std::{collections::HashMap, fs::create_dir_all, time::Duration};

use monad_consensus_types::{
    block::{Block, BlockType},
    block_validator::MockValidator,
    payload::NopStateRoot,
    txpool::MockTxPool,
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
use monad_executor::timed_event::TimedEvent;
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock_swarm::{Nodes, UntilTerminator},
    swarm_relation::SwarmRelation,
};
use monad_multi_sig::MultiSig;
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_testutil::swarm::{get_configs, node_ledger_verification};
use monad_transformer::{
    GenericTransformer, GenericTransformerPipeline, LatencyTransformer, XorLatencyTransformer, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
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
    type ValidatorSet = ValidatorSet<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin;
    type TxPool = MockTxPool;

    type RouterSchedulerConfig = NoSerRouterConfig<CertificateSignaturePubKey<Self::SignatureType>>;
    type RouterScheduler = NoSerRouterScheduler<
        CertificateSignaturePubKey<Self::SignatureType>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = GenericTransformerPipeline<
        CertificateSignaturePubKey<Self::SignatureType>,
        Self::TransportMessage,
    >;

    type LoggerConfig = WALoggerConfig;
    type Logger =
        WALogger<TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

#[test]
fn test_replay() {
    recover_nodes_msg_delays(4, 10, 5, 4, 0, SeqNum(2000), Round(50));
}

pub fn recover_nodes_msg_delays(
    num_nodes: u16,
    num_blocks_before: usize,
    num_block_after: usize,
    state_root_delay: u64,
    proposal_size: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) {
    let (pubkeys, state_configs) = get_configs::<
        <ReplaySwarm as SwarmRelation>::SignatureType,
        <ReplaySwarm as SwarmRelation>::SignatureCollectionType,
        _,
    >(
        MockValidator {},
        num_nodes,
        Duration::from_millis(101),
        state_root_delay,
        proposal_size,
        val_set_update_interval,
        epoch_start_delay,
    );

    // create the log file path
    let mut logger_configs = Vec::new();
    let tmpdir = tempdir().unwrap();
    create_dir_all(tmpdir.path()).unwrap();
    for i in 0..num_nodes {
        logger_configs.push(WALoggerConfig {
            file_path: tmpdir.path().join(format!("wal{}", i)),
            sync: false,
        });
    }

    let peers = pubkeys
        .iter()
        .copied()
        .zip(state_configs)
        .zip(logger_configs.clone())
        .map(|((a, b), c)| {
            (
                ID::new(NodeId::new(a)),
                b,
                c,
                NoSerRouterConfig {
                    all_peers: pubkeys.iter().map(|pubkey| NodeId::new(*pubkey)).collect(),
                },
                vec![GenericTransformer::XorLatency(XorLatencyTransformer::new(
                    Duration::from_millis(u8::MAX as u64),
                ))],
                1,
            )
        })
        .collect::<Vec<_>>();

    let mut nodes = Nodes::<ReplaySwarm>::new(peers);
    let term = UntilTerminator::new().until_block(num_blocks_before);
    while nodes.step_until(&term).is_some() {}

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

    let (pubkeys_clone, state_configs_clone) = get_configs::<
        <ReplaySwarm as SwarmRelation>::SignatureType,
        <ReplaySwarm as SwarmRelation>::SignatureCollectionType,
        _,
    >(
        MockValidator {},
        num_nodes,
        Duration::from_millis(2),
        4,
        proposal_size,
        val_set_update_interval,
        epoch_start_delay,
    );

    let peers_clone = pubkeys_clone
        .iter()
        .copied()
        .zip(state_configs_clone)
        .zip(logger_configs)
        .map(|((a, b), c)| {
            (
                ID::new(NodeId::new(a)),
                b,
                c,
                NoSerRouterConfig {
                    all_peers: pubkeys.iter().map(|pubkey| NodeId::new(*pubkey)).collect(),
                },
                vec![GenericTransformer::Latency(LatencyTransformer::new(
                    Duration::from_millis(1),
                ))],
                1,
            )
        })
        .collect::<Vec<_>>();

    let mut nodes_recovered = Nodes::<ReplaySwarm>::new(peers_clone);

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
    let term = UntilTerminator::new().until_block(num_blocks_before + num_block_after);
    while nodes_recovered.step_until(&term).is_some() {}

    node_ledger_verification(
        &nodes_recovered
            .states()
            .values()
            .map(|node| node.executor.ledger().get_blocks().clone())
            .collect(),
        1,
    );
}
