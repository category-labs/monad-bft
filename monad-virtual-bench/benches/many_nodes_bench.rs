use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use bytes::Bytes;
use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_bls::BlsSignatureCollection;
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, payload::StateRoot,
    txpool::MockTxPool,
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignaturePubKey},
    NopSignature,
};
use monad_gossip::mock::{MockGossip, MockGossipConfig};
use monad_mock_swarm::{
    mock_swarm::SwarmBuilder, node::NodeBuilder, swarm_relation::SwarmRelation,
    terminator::UntilTerminator,
};
use monad_multi_sig::MultiSig;
use monad_quic::{QuicRouterScheduler, QuicRouterSchedulerConfig};
use monad_router_scheduler::RouterSchedulerBuilder;
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_testutil::swarm::make_state_configs;
use monad_transformer::{
    BwTransformer, BytesTransformer, BytesTransformerPipeline, LatencyTransformer, ID,
};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};

type SignatureType = NopSignature;
type NodeIdPubKey = CertificateSignaturePubKey<NopSignature>;

struct NopSwarm;

impl SwarmRelation for NopSwarm {
    type SignatureType = SignatureType;
    type SignatureCollectionType = MultiSig<NopSignature>;
    type BlockPolicyType = PassthruBlockPolicy;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type AsyncStateRootVerify = PeerAsyncStateVerify<
        Self::SignatureCollectionType,
        <Self::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
    >;

    type RouterScheduler = QuicRouterScheduler<
        MockGossip<NodeIdPubKey>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = BytesTransformerPipeline<NodeIdPubKey>;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
}

struct BlsSwarm;

impl SwarmRelation for BlsSwarm {
    type SignatureType = SignatureType;
    type SignatureCollectionType = BlsSignatureCollection<NodeIdPubKey>;
    type BlockPolicyType = PassthruBlockPolicy;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type AsyncStateRootVerify = PeerAsyncStateVerify<
        Self::SignatureCollectionType,
        <Self::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
    >;

    type RouterScheduler = QuicRouterScheduler<
        MockGossip<NodeIdPubKey>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = BytesTransformerPipeline<NodeIdPubKey>;

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
}

fn many_nodes_nop_timeout() -> u128 {
    let zero_instant = Instant::now();
    let state_configs = make_state_configs::<NopSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || {
            StateRoot::new(
                SeqNum(u64::MAX), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        Duration::from_millis(20), // delta
        0,                         // proposal_tx_limit
        SeqNum(2000),              // val_set_update_interval
        Round(50),                 // epoch_start_delay
        majority_threshold,        // state root quorum threshold
        5,                         // max_blocksync_retries
        SeqNum(100),               // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<NopSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                let me = NodeId::new(state_builder.key.pubkey());
                NodeBuilder::<NopSwarm>::new(
                    ID::new(me),
                    state_builder,
                    QuicRouterSchedulerConfig::new(
                        zero_instant,
                        all_peers.iter().cloned().collect(),
                        me,
                        7,
                        MockGossipConfig {
                            all_peers: all_peers.iter().cloned().collect(),
                            me,
                            message_delay: Duration::ZERO,
                        }
                        .build(),
                        Duration::from_millis(100),
                        1000,
                    )
                    .build(),
                    MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(
                            100,
                        ))),
                        BytesTransformer::Bw(BwTransformer::new(4, Duration::from_secs(1))),
                    ],
                    vec![],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    let mut max_tick = Duration::ZERO;
    while let Some((tick, _, _)) =
        swarm.step_until(&mut UntilTerminator::new().until_round(Round(10)))
    {
        max_tick = tick;
    }
    max_tick.as_millis()
}

fn many_nodes_bls_timeout() -> u128 {
    let zero_instant = Instant::now();
    let state_configs = make_state_configs::<BlsSwarm>(
        40, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || {
            StateRoot::new(
                SeqNum(u64::MAX), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        Duration::from_millis(20), // delta
        0,                         // proposal_tx_limit
        SeqNum(2000),              // val_set_update_interval
        Round(50),                 // epoch_start_delay
        majority_threshold,        // state root quorum threshold
        5,                         // max_blocksync_retries
        SeqNum(100),               // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<BlsSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                let me = NodeId::new(state_builder.key.pubkey());
                NodeBuilder::<BlsSwarm>::new(
                    ID::new(me),
                    state_builder,
                    QuicRouterSchedulerConfig::new(
                        zero_instant,
                        all_peers.iter().cloned().collect(),
                        me,
                        7,
                        MockGossipConfig {
                            all_peers: all_peers.iter().cloned().collect(),
                            me,
                            message_delay: Duration::ZERO,
                        }
                        .build(),
                        Duration::from_millis(200),
                        1000,
                    )
                    .build(),
                    MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                    vec![
                        BytesTransformer::Latency(LatencyTransformer::new(Duration::from_millis(
                            100,
                        ))),
                        BytesTransformer::Bw(BwTransformer::new(4, Duration::from_secs(1))),
                    ],
                    vec![],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    let mut max_tick = Duration::ZERO;
    while let Some((tick, _, _)) =
        swarm.step_until(&mut UntilTerminator::new().until_round(Round(10)))
    {
        max_tick = tick;
    }
    max_tick.as_millis()
}

monad_virtual_bench::virtual_bench_main! {many_nodes_nop_timeout, many_nodes_bls_timeout}
