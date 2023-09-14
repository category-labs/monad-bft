#[cfg(test)]
mod tests {
    use std::{fs::create_dir_all, time::Duration};

    use futures::StreamExt;
    use monad_block_sync::BlockSyncState;
    use monad_consensus_state::{ConsensusConfig, ConsensusState};
    use monad_consensus_types::{
        block::BlockType, certificate_signature::CertificateKeyPair, multi_sig::MultiSig,
        payload::NopStateRoot, quorum_certificate::genesis_vote_info,
        signature_collection::SignatureCollectionKeyPairType, transaction_validator::MockValidator,
        validation::Sha256Hash, voting::ValidatorMapping,
    };
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_executor::{
        executor::{checkpoint::MockCheckpoint, ledger::MockLedger, mock::MockMempool},
        Executor, State,
    };
    use monad_state::{MonadConfig, MonadState};
    use monad_testutil::signing::get_genesis_config;
    use monad_types::NodeId;
    use monad_validator::{simple_round_robin::SimpleRoundRobin, validator_set::ValidatorSet};
    use monad_wal::{
        mock::{MockWALogger, MockWALoggerConfig},
        PersistenceLogger,
    };
    use tempfile::tempdir;

    type SignatureType = SecpSignature;
    type SignatureCollectionType = MultiSig<SignatureType>;
    type TransactionValidatorType = MockValidator;
    type StateRootValidatorType = NopStateRoot;
    type S = MonadState<
        ConsensusState<SignatureCollectionType, TransactionValidatorType, StateRootValidatorType>,
        SignatureType,
        SignatureCollectionType,
        ValidatorSet,
        SimpleRoundRobin,
        BlockSyncState,
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
                    KeyPair::libp2p_from_bytes(k.clone().as_mut_slice()).expect("keypair creation");

                let certificate_key = <SignatureCollectionKeyPairType<SignatureCollectionType> as CertificateKeyPair>::from_bytes(k.as_mut_slice()).expect("certificate keypair creation");

                let executor = monad_executor::executor::parent::ParentExecutor {
                    router: monad_p2p::Service::without_executor(key_libp2p.into()),
                    mempool: MockMempool::default(),
                    ledger: MockLedger::default(),
                    checkpoint: MockCheckpoint::default(),
                    timer: monad_executor::executor::timer::TokioTimer::default(),
                };

                let logger_config = MockWALoggerConfig {};
                (key, certificate_key, executor, logger_config)
            })
            .collect::<Vec<_>>();

        // set up executors - dial each other
        for i in 0..NUM_NODES {
            let (key, cert_key, mut executor, logger_config) = node_configs.pop().unwrap();
            for (_, _, executor_to_dial, _) in &mut node_configs {
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

            node_configs.push((key, cert_key, executor, logger_config));
            node_configs.swap(i as usize, NUM_NODES as usize - 1);
        }

        let pubkeys = node_configs
            .iter()
            .map(|(key, _, _, _)| KeyPair::pubkey(key))
            .collect::<Vec<_>>();

        let voting_keys = node_configs
            .iter()
            .map(|(key, cert_key, _, _)| (NodeId(key.pubkey()), cert_key))
            .collect::<Vec<_>>();

        let voting_identities = node_configs
            .iter()
            .map(|(key, cert_key, _, _)| (NodeId(key.pubkey()), cert_key.pubkey()));

        let validator_mapping = ValidatorMapping::new(voting_identities);

        let config_validators = node_configs
            .iter()
            .map(|(key, certkey, _, _)| (key.pubkey(), certkey.pubkey()))
            .collect::<Vec<_>>();

        let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SignatureCollectionType>(
            voting_keys.iter(),
            &validator_mapping,
        );

        let state_configs = node_configs
            .into_iter()
            .zip(std::iter::repeat(pubkeys.clone()))
            .map(|((key, certkey, exec, logger_config), _)| {
                (
                    exec,
                    MonadConfig {
                        transaction_validator: TransactionValidatorType {},
                        key,
                        certkey,
                        validators: config_validators.clone(),
                        delta: Duration::from_millis(2),
                        consensus_config: ConsensusConfig {
                            proposal_size: 5000,
                            state_root_delay: 0,
                            propose_with_missing_blocks: false,
                        },
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
