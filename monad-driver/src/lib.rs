#[cfg(test)]
mod tests {
    use std::{fs::create_dir_all, time::Duration};

    use futures::StreamExt;
    use monad_consensus_state::ConsensusState;
    use monad_consensus_types::{
        multi_sig::MultiSig, quorum_certificate::genesis_vote_info,
        signature::SignatureCollectionKeyPairType, transaction_validator::MockValidator,
        validation::Sha256Hash,
    };
    use monad_crypto::{
        bls12_381::BlsKeyPair,
        secp256k1::{KeyPair, SecpSignature},
        GenericKeyPair,
    };
    use monad_executor::{
        executor::{ledger::MockLedger, mempool::MockMempool},
        Executor, State,
    };
    use monad_state::{MonadConfig, MonadState};
    use monad_testutil::signing::get_genesis_config;
    use monad_types::NodeId;
    use monad_validator::{
        simple_round_robin::SimpleRoundRobin,
        validator_property::{ValidatorSetProperty, ValidatorSetPropertyType},
        validator_set::ValidatorSet,
    };
    use monad_wal::{
        mock::{MockWALogger, MockWALoggerConfig},
        PersistenceLogger,
    };
    use tempfile::tempdir;

    type SignatureType = SecpSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type TransactionValidatorType = MockValidator;
    type S = MonadState<
        ConsensusState<SignatureType, SignatureCollectionType, TransactionValidatorType>,
        SignatureType,
        SignatureCollectionType,
        ValidatorSet,
        ValidatorSetProperty,
        SimpleRoundRobin,
    >;
    type PersistenceLoggerType = MockWALogger<<S as State>::Event>;

    #[tokio::test]
    async fn libp2p_executor() {
        const NUM_NODES: u32 = 2;

        let log_dir = tempdir().unwrap();
        create_dir_all(log_dir.path()).unwrap();

        let mut node_configs = (0..NUM_NODES)
            .map(|i| {
                let mut k: [u8; 32] = [(i + 1) as u8; 32];
                let (key, key_libp2p) =
                    KeyPair::libp2p_from_bytes(k.clone().as_mut_slice()).unwrap();

                let blskey = BlsKeyPair::from_bytes(k.clone().as_mut_slice()).unwrap();
                let voting_key = <SignatureCollectionKeyPairType<SignatureCollectionType> as GenericKeyPair>::from_bytes(k.as_mut_slice()).expect("voting key");

                let executor = monad_executor::executor::parent::ParentExecutor {
                    router: monad_p2p::Service::without_executor(key_libp2p.into()),
                    mempool: MockMempool::default(),
                    ledger: MockLedger::default(),
                    timer: monad_executor::executor::timer::TokioTimer::default(),
                };

                let logger_config = MockWALoggerConfig {};
                (key, blskey, voting_key, executor, logger_config)
            })
            .collect::<Vec<_>>();

        // set up executors - dial each other
        for i in 0..NUM_NODES {
            let (key, blskey, voting_key, mut executor, logger_config) =
                node_configs.pop().unwrap();
            for (_, _, _, executor_to_dial, _) in &mut node_configs {
                let addresses = executor_to_dial
                    .router
                    .listeners()
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(!addresses.is_empty());
                for address in addresses {
                    executor
                        .router
                        .add_peer(executor_to_dial.router.local_peer_id(), address)
                }
            }

            node_configs.push((key, blskey, voting_key, executor, logger_config));
            node_configs.swap(i as usize, NUM_NODES as usize - 1);
        }

        let pubkeys = node_configs
            .iter()
            .map(|(key, _, _, _, _)| KeyPair::pubkey(key))
            .collect::<Vec<_>>();

        let prop_list = node_configs
            .iter()
            .map(|(key, blskey, _, _, _)| (NodeId(key.pubkey()), blskey.pubkey()))
            .collect::<Vec<_>>();

        let validators_property =
            ValidatorSetProperty::new(prop_list).expect("validator set property");

        let config_validators = node_configs
            .iter()
            .map(|(key, blskey, _, _, _)| (key.pubkey(), blskey.pubkey()))
            .collect::<Vec<_>>();

        let voters = pubkeys
            .iter()
            .map(|pk| NodeId(*pk))
            .zip(node_configs.iter().map(|(_, _, key, _, _)| key));
        let (genesis_block, genesis_sigs) =
            get_genesis_config::<Sha256Hash, SignatureCollectionType>(voters, &validators_property);

        let state_configs = node_configs
            .into_iter()
            .zip(std::iter::repeat(pubkeys.clone()))
            .map(|((key, _, voting_key, exec, logger_config), _)| {
                (
                    exec,
                    MonadConfig {
                        transaction_validator: TransactionValidatorType {},
                        key,
                        voting_key,
                        validators: config_validators.clone(),
                        delta: Duration::from_millis(2),
                        genesis_block: genesis_block.clone(),
                        genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
                        genesis_signatures: genesis_sigs.clone(),
                    },
                    PersistenceLoggerType::new(logger_config).unwrap(),
                )
            })
            .collect::<Vec<_>>();

        let mut states = state_configs
            .into_iter()
            .map(|(mut executor, config, (wal, replay_events))| {
                let (mut state, mut init_commands) = S::init(config);

                for event in replay_events {
                    init_commands.extend(state.update(event));
                }

                executor.exec(init_commands);
                (executor, state, wal)
            })
            .collect::<Vec<_>>();

        while states
            .iter()
            .any(|(exec, _, _)| exec.ledger().get_blocks().len() < 10)
        {
            let ((executor, state, event, wal), _, _) =
                futures::future::select_all(states.iter_mut().map(|(executor, state, wal)| {
                    let fut = async {
                        let event = executor.next().await.unwrap();
                        (executor, state, event, wal)
                    };
                    Box::pin(fut)
                }))
                .await;
            wal.push(&event).unwrap();
            let commands = state.update(event);
            executor.exec(commands);
        }
    }
}
