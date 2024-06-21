use std::{collections::BTreeSet, time::Duration};

use monad_async_state_verify::{majority_threshold, PeerAsyncStateVerify};
use monad_consensus_types::{
    block::PassthruBlockPolicy, block_validator::MockValidator, payload::StateRoot,
    txpool::MockTxPool,
};
use monad_crypto::certificate_signature::CertificateKeyPair;
use monad_eth_reserve_balance::PassthruReserveBalanceCache;
use monad_mock_swarm::{
    mock::TimestamperConfig, mock_swarm::SwarmBuilder, node::NodeBuilder,
    swarm_relation::NoSerSwarm, terminator::UntilTerminator,
};
use monad_router_scheduler::{NoSerRouterConfig, RouterSchedulerBuilder};
use monad_testutil::swarm::{make_state_configs, swarm_ledger_verification};
use monad_transformer::{GenericTransformer, LatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum};
use monad_updaters::state_root_hash::MockStateRootHashNop;
use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSetFactory};

fn two_nodes_virtual() -> u128 {
    let state_configs = make_state_configs::<NoSerSwarm>(
        2, // num_nodes
        ValidatorSetFactory::default,
        SimpleRoundRobin::default,
        MockTxPool::default,
        || MockValidator,
        || PassthruBlockPolicy,
        || PassthruReserveBalanceCache,
        || {
            StateRoot::new(
                SeqNum(4), // state_root_delay
            )
        },
        PeerAsyncStateVerify::new,
        Duration::from_millis(2), // delta
        0,                        // proposal_tx_limit
        SeqNum(2000),             // val_set_update_interval
        Round(50),                // epoch_start_delay
        majority_threshold,       // state root quorum threshold
        5,                        // max_blocksync_retries
        SeqNum(100),              // state_sync_threshold
    );
    let all_peers: BTreeSet<_> = state_configs
        .iter()
        .map(|state_config| NodeId::new(state_config.key.pubkey()))
        .collect();
    let swarm_config = SwarmBuilder::<NoSerSwarm>(
        state_configs
            .into_iter()
            .enumerate()
            .map(|(seed, state_builder)| {
                let validators = state_builder.forkpoint.validator_sets[0].clone();
                let me = NodeId::new(state_builder.key.pubkey());
                NodeBuilder::new(
                    ID::new(me),
                    state_builder,
                    NoSerRouterConfig::new(all_peers.clone()).build(),
                    MockStateRootHashNop::new(validators.validators, SeqNum(2000)),
                    vec![GenericTransformer::Latency(LatencyTransformer::new(
                        Duration::from_millis(1),
                    ))],
                    vec![],
                    TimestamperConfig::default(),
                    seed.try_into().unwrap(),
                )
            })
            .collect(),
    );

    let mut swarm = swarm_config.build();
    let mut max_tick = Duration::ZERO;
    while let Some((tick, _, _)) =
        swarm.step_until(&mut UntilTerminator::new().until_tick(Duration::from_secs(10)))
    {
        max_tick = tick;
    }
    swarm_ledger_verification(&swarm, 1024);
    max_tick.as_millis()
}

monad_virtual_bench::virtual_bench_main! {two_nodes_virtual}
