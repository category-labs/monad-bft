use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use bytes::Bytes;
use monad_async_state_verify::LocalAsyncStateVerify;
use monad_consensus_types::{
    block::Block, block_validator::MockValidator, payload::StateRoot, txpool::MockTxPool,
};
use monad_crypto::{certificate_signature::CertificateSignaturePubKey, NopSignature};
use monad_executor_glue::MonadEvent;
use monad_gossip::mock::MockGossip;
use monad_mock_swarm::swarm_relation::SwarmRelation;
use monad_multi_sig::MultiSig;
use monad_quic::QuicRouterScheduler;
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_tracing_counter::counter::{CounterLayer, MetricFilter};
use monad_transformer::BytesTransformerPipeline;
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSetFactory, ValidatorSetTypeFactory},
};
use monad_wal::mock::MockWALogger;
use tracing_core::LevelFilter;
use tracing_subscriber::{filter::Targets, prelude::*, Layer, Registry};

pub struct QuicSwarm;

impl SwarmRelation for QuicSwarm {
    type SignatureType = NopSignature;
    type SignatureCollectionType = MultiSig<Self::SignatureType>;

    type TransportMessage = Bytes;

    type BlockValidator = MockValidator;
    type StateRootValidator = StateRoot;
    type ValidatorSetTypeFactory =
        ValidatorSetFactory<CertificateSignaturePubKey<Self::SignatureType>>;
    type LeaderElection = SimpleRoundRobin<CertificateSignaturePubKey<Self::SignatureType>>;
    type TxPool = MockTxPool;
    type AsyncStateRootVerify = LocalAsyncStateVerify<
        Self::SignatureCollectionType,
        <Self::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
    >;

    type RouterScheduler = QuicRouterScheduler<
        MockGossip<CertificateSignaturePubKey<Self::SignatureType>>,
        MonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
        VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>,
    >;

    type Pipeline = BytesTransformerPipeline<CertificateSignaturePubKey<Self::SignatureType>>;

    type Logger = MockWALogger<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>;

    type StateRootHashExecutor = MockStateRootHashNop<
        Block<Self::SignatureCollectionType>,
        Self::SignatureType,
        Self::SignatureCollectionType,
    >;
}

pub(crate) fn setup_counter() -> Arc<RwLock<HashMap<String, AtomicUsize>>> {
    let fmt_layer = tracing_subscriber::fmt::layer();
    let counter = Arc::new(RwLock::new(HashMap::new()));
    let counter_layer = CounterLayer::new(Arc::clone(&counter));

    let subscriber = Registry::default()
        .with(
            fmt_layer
                .with_filter(MetricFilter {})
                .with_filter(Targets::new().with_default(LevelFilter::INFO)),
        )
        .with(counter_layer);
    tracing::subscriber::set_global_default(subscriber).expect("unable to set global subscriber");

    counter
}
