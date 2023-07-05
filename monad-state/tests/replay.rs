#[cfg(test)]
#[cfg(feature = "proto")]
mod test {
    use monad_testutil::swarm::{get_configs, node_ledger_verification};
    use std::collections::HashMap;
    use std::fs::create_dir_all;
    use std::time::Duration;
    use tempfile::tempdir;

    use monad_consensus::signatures::multi_sig::MultiSig;
    use monad_crypto::secp256k1::SecpSignature;
    use monad_executor::mock_swarm::Nodes;
    use monad_executor::mock_swarm::XorLatencyTransformer;
    use monad_state::{MonadEvent, MonadState};
    use monad_wal::wal::{WALogger, WALoggerConfig};

    type SignatureType = SecpSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;

    #[test]
    fn test_replay() {
        recover_nodes_msg_delays(4, 10, 5);
    }

    pub fn recover_nodes_msg_delays(
        num_nodes: u16,
        num_blocks_before: usize,
        num_block_after: usize,
    ) {
        use monad_executor::mock_swarm::LatencyTransformer;

        let (pubkeys, state_configs) = get_configs(num_nodes, Duration::from_millis(101));

        // create the log file path
        let mut logger_configs = Vec::new();
        let tmpdir = tempdir().unwrap();
        create_dir_all(tmpdir.path()).unwrap();
        for i in 0..num_nodes {
            logger_configs.push(WALoggerConfig {
                file_path: tmpdir.path().join(format!("wal{}", i)),
            });
        }

        let peers = pubkeys
            .into_iter()
            .zip(state_configs)
            .zip(logger_configs.clone())
            .map(|((a, b), c)| (a, b, c))
            .collect::<Vec<_>>();

        let mut nodes = Nodes::<
            MonadState<SignatureType, SignatureCollectionType>,
            _,
            WALogger<MonadEvent<SignatureType, SignatureCollectionType>>,
        >::new(
            peers,
            XorLatencyTransformer(Duration::from_millis(u8::MAX as u64)),
        );

        while let Some((_, _, _)) = nodes.step() {
            if nodes
                .states()
                .values()
                .next()
                .unwrap()
                .0
                .ledger()
                .get_blocks()
                .len()
                > num_blocks_before
            {
                break;
            }
        }

        // can skip this verification so we don't have two cases failing for the same reason
        let node_ledger_before = nodes
            .states()
            .iter()
            .map(|(peerid, (exec, _, _))| {
                (
                    *peerid,
                    exec.ledger()
                        .get_blocks()
                        .iter()
                        .map(|b| b.get_id())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();

        // drop the nodes -> close the files
        drop(nodes);

        let (pubkeys_clone, state_configs_clone) =
            get_configs::<SignatureCollectionType>(num_nodes, Duration::from_millis(2));

        let peers_clone = pubkeys_clone
            .into_iter()
            .zip(state_configs_clone)
            .zip(logger_configs)
            .map(|((a, b), c)| (a, b, c))
            .collect::<Vec<_>>();

        let mut nodes_recovered =
            Nodes::<
                MonadState<SignatureType, SignatureCollectionType>,
                _,
                WALogger<MonadEvent<SignatureType, SignatureCollectionType>>,
            >::new(peers_clone, LatencyTransformer(Duration::from_millis(1)));

        let node_ledger_recovered = nodes_recovered
            .states()
            .iter()
            .map(|(peerid, (exec, _, _))| {
                (
                    *peerid,
                    exec.ledger()
                        .get_blocks()
                        .iter()
                        .map(|b| b.get_id())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(node_ledger_before, node_ledger_recovered);

        while let Some((_, _, _)) = nodes_recovered.step() {
            if nodes_recovered
                .states()
                .values()
                .next()
                .unwrap()
                .0
                .ledger()
                .get_blocks()
                .len()
                > num_blocks_before + num_block_after
            {
                break;
            }
        }

        node_ledger_verification(nodes_recovered.states());
    }
}
