pub mod twin_reader;

use std::collections::BTreeMap;

use monad_mock_swarm::{
    mock::{MockExecutor, RouterScheduler},
    mock_swarm::{Node, Nodes},
    swarm_relation::SwarmRelation,
    transformer::{
        monad_test::{MonadMessageTransformer, MonadMessageTransformerPipeline, TwinsTransformer},
        RandLatencyTransformer, ID,
    },
};
use monad_state::{MonadMessage, VerifiedMonadMessage};
use monad_types::{Deserializable, Serializable};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaChaRng;

use crate::twin_reader::{TwinsNodeConfig, TwinsTestCase};

pub fn run_twins_test<S, LGRC, RSC, MPC>(
    get_logger_config: LGRC,
    get_router_cfg: RSC,
    get_mempool_cfg: MPC,
    seed: u64,
    test_case: TwinsTestCase<S>,
) where
    S: SwarmRelation<P = MonadMessageTransformerPipeline>,
    MockExecutor<S>: Unpin,
    MonadMessage<S::ST, S::SCT>: Deserializable<S::Message>,
    VerifiedMonadMessage<S::ST, S::SCT>: Serializable<<S::RS as RouterScheduler>::M>,
    Node<S>: Send,
    LGRC: Fn(&ID, &Vec<ID>) -> S::LGRCFG,
    RSC: Fn(&ID, &Vec<ID>) -> S::RSCFG,
    MPC: Fn(&ID, &Vec<ID>) -> S::MPCFG,
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
