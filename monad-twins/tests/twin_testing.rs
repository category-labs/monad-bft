#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use monad_block_sync::BlockSyncState;
    use monad_consensus_state::ConsensusState;
    use monad_consensus_types::{
        multi_sig::MultiSig, payload::NopStateRoot, transaction_validator::MockValidator,
    };
    use monad_crypto::NopSignature;
    use monad_executor::timed_event::TimedEvent;
    use monad_executor_glue::MonadEvent;
    use monad_mock_swarm::{
        mock::{MockMempool, MockMempoolConfig, NoSerRouterConfig, NoSerRouterScheduler},
        swarm_relation::SwarmRelation,
        transformer::{monad_test::MonadMessageTransformerPipeline, ID},
    };
    use monad_state::{MonadMessage, MonadState, VerifiedMonadMessage};
    use monad_twins_utils::{run_twins_test, twin_reader::read_twins_test};
    use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
    use monad_wal::mock::{MockWALogger, MockWALoggerConfig};
    use test_case::test_case;

    const TWIN_DEFAULT_SEED: u64 = 1;

    struct TwinsSwarm;

    impl SwarmRelation for TwinsSwarm {
        type State = MonadState<
            ConsensusState<MultiSig<NopSignature>, MockValidator, NopStateRoot>,
            NopSignature,
            MultiSig<NopSignature>,
            ValidatorSet,
            SimpleRoundRobin,
            BlockSyncState,
        >;
        type SignatureType = NopSignature;
        type SignatureCollectionType = MultiSig<Self::SignatureType>;
        type RouterScheduler =
            NoSerRouterScheduler<MonadMessage<Self::SignatureType, Self::SignatureCollectionType>>;
        type Pipeline = MonadMessageTransformerPipeline;
        type Logger = MockWALogger<
            TimedEvent<MonadEvent<Self::SignatureType, Self::SignatureCollectionType>>,
        >;
        type MempoolExecutor = MockMempool<Self::SignatureType, Self::SignatureCollectionType>;
        type TransactionValidator = MockValidator;
        type LoggerConfig = MockWALoggerConfig;
        type RouterSchedulerConfig = NoSerRouterConfig;
        type MempoolConfig = MockMempoolConfig;
        type StateMessage = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
        type OutboundStateMessage =
            VerifiedMonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
        type Message = MonadMessage<Self::SignatureType, Self::SignatureCollectionType>;
    }

    #[test_case("./tests/happy_path.json"; "happy_path")]
    #[test_case("./tests/one_twin.json"; "one_twin")]
    #[test_case("./tests/one_twin_partition.json"; "one_twin_partition")]
    #[test_case("./tests/make_progress.json"; "make_progress")]

    fn twins_testing(path: &str) {
        let test_case = read_twins_test::<TwinsSwarm>(MockValidator, path);

        println!(
            "running twins_testing, description: {:?}",
            test_case.description,
        );

        let get_lgr_cfg = |_: &ID, _: &Vec<ID>| MockWALoggerConfig;
        let get_router_cfg = |_: &ID, ids: &Vec<ID>| NoSerRouterConfig {
            all_peers: ids
                .iter()
                .map(|id| *id.get_peer_id())
                .collect::<BTreeSet<_>>(),
        };
        let get_mempool_cfg = |id: &ID, _: &Vec<ID>| MockMempoolConfig(*id.get_identifier() as u64);

        run_twins_test::<TwinsSwarm, _, _, _>(
            get_lgr_cfg,
            get_router_cfg,
            get_mempool_cfg,
            TWIN_DEFAULT_SEED,
            test_case,
        )
    }

    #[should_panic]
    #[test_case("./tests/too_much_twin.json"; "too_much_twin")]
    #[test_case("./tests/too_much_twin_with_big_delay.json"; "too_much_twin_with_big_delay")]
    #[test_case("./tests/mal_formed.json"; "mal_formed json")]

    fn twins_should_fail_testing(path: &str) {
        let test_case = read_twins_test::<TwinsSwarm>(MockValidator, path);
        println!(
            "running expected fail twins_testing, description: {:?}",
            test_case.description
        );
        let get_lgr_cfg = |_: &ID, _: &Vec<ID>| MockWALoggerConfig;
        let get_router_cfg = |_: &ID, ids: &Vec<ID>| NoSerRouterConfig {
            all_peers: ids
                .iter()
                .map(|id| *id.get_peer_id())
                .collect::<BTreeSet<_>>(),
        };
        let get_mempool_cfg = |id: &ID, _: &Vec<ID>| MockMempoolConfig(*id.get_identifier() as u64);

        run_twins_test::<TwinsSwarm, _, _, _>(
            get_lgr_cfg,
            get_router_cfg,
            get_mempool_cfg,
            TWIN_DEFAULT_SEED,
            test_case,
        )
    }
}
