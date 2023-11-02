pub mod twin_reader;

use std::collections::BTreeMap;

use monad_mock_swarm::{
    mock::MockExecutor,
    mock_swarm::{Node, Nodes},
    swarm_relation::SwarmRelation,
    transformer::{
        monad_test::{MonadMessageTransformer, MonadMessageTransformerPipeline, TwinsTransformer},
        RandLatencyTransformer, ID,
    },
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;

use crate::twin_reader::{TwinsNodeConfig, TwinsTestCase};

pub fn run_twins_test<S, L, R, M>(
    get_logger_config: L,
    get_router_cfg: R,
    get_mempool_cfg: M,
    seed: u64,
    test_case: TwinsTestCase<S>,
) where
    S: SwarmRelation<Pipeline = MonadMessageTransformerPipeline>,
    MockExecutor<S>: Unpin,
    Node<S>: Send,
    L: Fn(&ID, &Vec<ID>) -> S::LoggerConfig,
    R: Fn(&ID, &Vec<ID>) -> S::RouterSchedulerConfig,
    M: Fn(&ID, &Vec<ID>) -> S::MempoolConfig,
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
    let mut swarm = Nodes::<S>::new(vec![]).can_have_duplicate_peer();

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
        );

        let pipeline = vec![
            MonadMessageTransformer::Twins(twins_transformer),
            MonadMessageTransformer::RandLatency(RandLatencyTransformer::new(rng.gen(), delta)),
        ];
        let (lgr_cfg, router_cfg, mempool_cfg) = (
            get_logger_config(&id, &ids),
            get_router_cfg(&id, &ids),
            get_mempool_cfg(&id, &ids),
        );
        swarm.add_state((
            id,
            state_config,
            lgr_cfg,
            router_cfg,
            mempool_cfg,
            pipeline,
            seed,
        ));
    }

    while swarm.step_until(&terminator).is_some() {}

    if let Some(liveness_length) = liveness {
        let liveness_transformer =
            TwinsTransformer::new(duplicates, BTreeMap::new(), ids.clone(), false);
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
        while swarm.step_until(&terminator).is_some() {}
    }
}
