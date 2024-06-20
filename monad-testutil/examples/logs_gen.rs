use std::{collections::BTreeSet, path::PathBuf, time::Duration};

use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, payload::StateRoot,
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
use monad_testutil::swarm::make_state_configs;
use monad_transformer::{GenericTransformer, GenericTransformerPipeline, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};
use monad_wal::wal::{WALogger, WALoggerConfig};

pub struct LogSwarm;

impl SwarmRelation for LogSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;
    type BlockPolicyType = PassthruBlockPolicy;

    type TransportMessage =
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;

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

    type StateRootHashExecutor =
        MockStateRootHashNop<Self::SignatureType, Self::SignatureCollectionType>;
}

pub fn generate_log(
    num_nodes: u16,
    num_blocks: usize,
    delta: Duration,
    state_root_delay: u64,
    proposal_size: usize,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) {
    let state_configs = make_state_configs::<LogSwarm>(
        num_nodes, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || {
            StateRoot::new(
                SeqNum(state_root_delay), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        delta,                   // delta
        proposal_size,           // proposal_tx_limit
        val_set_update_interval, // val_set_update_interval
        epoch_start_delay,       // epoch_start_delay
        majority_threshold,      // state root quorum threshold
        5,                       // max_blocksync_retries
        SeqNum(100),             // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<LogSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let pubkey = state_builder.key.pubkey();
                let validators = state_builder.validators.clone();
                NodeBuilder::<LogSwarm>::new(
                    ID::new(NodeId::new(pubkey)),
                    state_builder,
                    WALoggerConfig::new(
                        PathBuf::from(format!("{:?}.log", pubkey)),
                        false, // sync
                    ),
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators, val_set_update_interval),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(100),
                    ))],
                    vec![],
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    while swarm
        .step_until(&mut UntilTerminator::new().until_block(num_blocks))
        .is_some()
    {}
}

fn main() {
    generate_log(
        4,
        10,
        Duration::from_millis(101),
        4,
        0,
        SeqNum(2000),
        Round(50),
    );
    println!("Logs Generated!");
}
