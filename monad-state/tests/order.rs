#[cfg(test)]
mod test {
    use std::{collections::HashSet, env, time::Duration};

    use monad_executor::{
        mock_swarm::{LatencyTransformer, Transformer},
        PeerId,
    };
    use monad_testutil::swarm::{
        get_configs, run_one_delayed_node, PartitionThenReplayTransformer, TransformerReplayOrder,
    };
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use test_case::test_case;

    #[test_case(TransformerReplayOrder::Forward; "in order")]
    #[test_case(TransformerReplayOrder::Reverse; "reverse order")]
    #[test_case(TransformerReplayOrder::Random(1); "random seed 1")]
    #[test_case(TransformerReplayOrder::Random(2); "random seed 2")]
    #[test_case(TransformerReplayOrder::Random(3); "random seed 3")]
    #[test_case(TransformerReplayOrder::Random(4); "random seed 4")]
    #[test_case(TransformerReplayOrder::Random(5); "random seed 5")]
    fn all_messages_delayed(direction: TransformerReplayOrder) {
        let num_nodes = 4;
        let delta = Duration::from_millis(2);
        let (pubkeys, state_configs) = get_configs(num_nodes, delta);

        assert!(num_nodes >= 2, "test requires 2 or more nodes");

        let first_node = PeerId(*pubkeys.first().unwrap());

        let mut filter_peers = HashSet::new();
        filter_peers.insert(first_node);

        println!("delayed node ID: {:?}", first_node);

        run_one_delayed_node(
            vec![
                LatencyTransformer(Duration::from_millis(1)).boxed(),
                PartitionThenReplayTransformer::new(filter_peers, 200, direction).boxed(),
            ],
            pubkeys,
            state_configs,
        );
    }

    #[test]
    fn all_messages_delayed_cron() {
        let round = match env::var("ALL_MESSAGES_DELAYED_ROUND") {
            Ok(v) => v.parse().unwrap(),
            Err(_e) => 1, // default to 1 if not found
        };

        match env::var("RANDOM_TEST_SEED") {
            Ok(v) => {
                let mut seed = v.parse().unwrap();
                let mut generator: StdRng = StdRng::seed_from_u64(seed);
                for _ in 0..round {
                    seed = generator.gen();
                    println!("seed for all_messages_delayed_cron is set to be {}", seed);
                    all_messages_delayed(TransformerReplayOrder::Random(seed));
                }
            }
            Err(_e) => {
                println!("RANDOM_TEST_SEED is not set, default it to inorder");
                all_messages_delayed(TransformerReplayOrder::Forward);
            }
        };
    }
}
