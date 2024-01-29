pub mod twin_reader;

use std::collections::BTreeMap;

use monad_consensus_types::{
    block::Block, payload::StateRoot, signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor_glue::MonadEvent;
use monad_mock_swarm::{
    mock_swarm::SwarmBuilder,
    node::NodeBuilder,
    swarm_relation::SwarmRelation,
    transformer::{MonadMessageTransformer, MonadMessageTransformerPipeline, TwinsTransformer},
};
use monad_router_scheduler::{NoSerRouterConfig, NoSerRouterScheduler, RouterSchedulerBuilder};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_transformer::RandLatencyTransformer;
use monad_types::NodeId;
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use twin_reader::TWINS_STATE_ROOT_DELAY;

use crate::twin_reader::{TwinsNodeConfig, TwinsTestCase};

pub fn run_twins_test<ST, SCT, S>(seed: u64, test_case: TwinsTestCase<S>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    S: SwarmRelation<
        SignatureType = ST,
        SignatureCollectionType = SCT,
        Pipeline = MonadMessageTransformerPipeline<CertificateSignaturePubKey<ST>>,
        Logger = MockWALogger<MonadEvent<ST, SCT>>,
        RouterScheduler = NoSerRouterScheduler<
            CertificateSignaturePubKey<ST>,
            MonadMessage<ST, SCT>,
            VerifiedMonadMessage<ST, SCT>,
        >,
        StateRootValidator = StateRoot,
        StateRootHashExecutor = MockStateRootHashNop<Block<SCT>, ST, SCT>,
    >,
{
    let TwinsTestCase {
        description: _,
        allow_block_sync,
        liveness,
        mut terminator,
        delta,
        duplicates,
        nodes,
    } = test_case;

    let ids = nodes.keys().copied().collect::<Vec<_>>();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let mut swarm = SwarmBuilder::<S>(vec![]).build().can_have_duplicate_peer();

    for TwinsNodeConfig {
        id,
        state_config,
        default_partition,
        partition,
    } in nodes.into_values()
    {
        let twins_transformer = TwinsTransformer::new(
            duplicates.clone(),
            partition,
            default_partition,
            !allow_block_sync,
            false, // drop_state_root
        );

        let pipeline = vec![
            MonadMessageTransformer::Twins(twins_transformer),
            MonadMessageTransformer::RandLatency(RandLatencyTransformer::new(rng.gen(), delta)),
        ];
        let validators = state_config.validators.clone();
        swarm.add_state(NodeBuilder::<S>::new(
            id,
            state_config,
            MockWALoggerConfig::default(),
            NoSerRouterConfig::new(
                ids.iter()
                    .map(|id| NodeId::new(id.get_peer_id().pubkey()))
                    .collect(),
            )
            .build(),
            MockStateRootHashNop::new(validators, monad_types::SeqNum(TWINS_STATE_ROOT_DELAY)),
            pipeline,
            seed,
        ));
    }

    while swarm.step_until(&mut terminator.clone()).is_some() {}

    if let Some(liveness_length) = liveness {
        let liveness_transformer = TwinsTransformer::new(
            duplicates,
            BTreeMap::new(),
            ids.clone(),
            false,
            false, // drop_state_root
        );
        let pipeline = vec![
            MonadMessageTransformer::Twins(liveness_transformer),
            MonadMessageTransformer::RandLatency(RandLatencyTransformer::new(rng.gen(), delta)),
        ];

        for id in ids {
            swarm.update_pipeline(&id, pipeline.clone());
        }
        // extend the expected termination condition
        terminator.extend_all(liveness_length);
        // run all nodes all connected until expected_block + liveness block check is achieved
        while swarm.step_until(&mut terminator.clone()).is_some() {}
    }
}
