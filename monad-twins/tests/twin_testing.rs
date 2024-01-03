#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use monad_consensus_types::block_validator::MockValidator;
    use monad_mock_swarm::swarm_relation::MonadMessageNoSerSwarm;
    use monad_router_scheduler::NoSerRouterConfig;
    use monad_transformer::ID;
    use monad_twins_utils::{run_twins_test, twin_reader::read_twins_test};
    use monad_wal::mock::MockWALoggerConfig;
    use test_case::test_case;

    const TWIN_DEFAULT_SEED: u64 = 1;

    #[test_case("./tests/happy_path.json"; "happy_path")]
    #[test_case("./tests/one_twin.json"; "one_twin")]
    #[test_case("./tests/one_twin_partition.json"; "one_twin_partition")]
    #[test_case("./tests/make_progress.json"; "make_progress")]

    fn twins_testing(path: &str) {
        let test_case = read_twins_test::<MonadMessageNoSerSwarm>(MockValidator, path);

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

        run_twins_test::<MonadMessageNoSerSwarm, _, _>(
            get_lgr_cfg,
            get_router_cfg,
            TWIN_DEFAULT_SEED,
            test_case,
        )
    }

    #[should_panic]
    #[test_case("./tests/too_much_twin.json"; "too_much_twin")]
    #[test_case("./tests/too_much_twin_with_big_delay.json"; "too_much_twin_with_big_delay")]
    #[test_case("./tests/mal_formed.json"; "mal_formed json")]

    fn twins_should_fail_testing(path: &str) {
        let test_case = read_twins_test::<MonadMessageNoSerSwarm>(MockValidator, path);
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

        run_twins_test::<MonadMessageNoSerSwarm, _, _>(
            get_lgr_cfg,
            get_router_cfg,
            TWIN_DEFAULT_SEED,
            test_case,
        )
    }
}
