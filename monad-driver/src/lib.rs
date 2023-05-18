#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use monad_consensus::{
        signatures::aggregate_signature::AggregateSignatures,
        types::quorum_certificate::genesis_vote_info, validation::hashing::Sha256Hash,
    };
    use monad_crypto::secp256k1::{KeyPair, SecpSignature};
    use monad_executor::{
        executor::mempool::{MempoolExecutor, SimpleMempool},
        Executor, State,
    };
    use monad_state::{MonadConfig, MonadState};
    use monad_testutil::signing::get_genesis_config;

    type SignatureType = SecpSignature;
    type SignatureCollectionType = AggregateSignatures<SignatureType>;
    type S = MonadState<SignatureType, SignatureCollectionType>;

    #[tokio::test]
    async fn libp2p_executor() {
        const NUM_NODES: u32 = 2;

        let mut node_configs = (0..NUM_NODES)
            .map(|i| {
                let mut k: [u8; 32] = [(i + 1) as u8; 32];
                let (key, key_libp2p) = KeyPair::libp2p_from_bytes(&mut k).unwrap();

                let executor = monad_executor::executor::parent::ParentExecutor {
                    router: monad_p2p::Service::without_executor(key_libp2p.into()),
                    mempool: MempoolExecutor::<SimpleMempool, _>::default(),
                    timer: monad_executor::executor::timer::TokioTimer::default(),
                };
                (key, executor)
            })
            .collect::<Vec<_>>();

        // set up executors - dial each other
        for i in 0..NUM_NODES {
            let (key, mut executor) = node_configs.pop().unwrap();
            for (_, executor_to_dial) in &mut node_configs {
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

            node_configs.push((key, executor));
            node_configs.swap(i as usize, NUM_NODES as usize - 1);
        }

        let pubkeys = node_configs
            .iter()
            .map(|(key, _)| KeyPair::pubkey(key))
            .collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SignatureCollectionType>(
            node_configs.iter().map(|(key, _)| key),
        );

        let state_configs = node_configs
            .into_iter()
            .zip(std::iter::repeat(pubkeys.clone()))
            .map(|((key, exec), pubkeys)| {
                (
                    exec,
                    MonadConfig {
                        key,
                        validators: pubkeys,

                        delta: Duration::from_millis(2),
                        genesis_block: genesis_block.clone(),
                        genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
                        genesis_signatures: genesis_sigs.clone(),
                    },
                )
            })
            .collect::<Vec<_>>();

        let mut states = state_configs
            .into_iter()
            .map(|(mut executor, config)| {
                let (state, init_commands) = S::init(config);
                executor.exec(init_commands);
                (executor, state)
            })
            .collect::<Vec<_>>();

        while states.iter().any(|(_, state)| state.ledger().len() < 10) {
            let ((executor, state, event), _, _) =
                futures::future::select_all(states.iter_mut().map(|(executor, state)| {
                    let fut = async {
                        let event = executor.next().await.unwrap();
                        (executor, state, event)
                    };
                    Box::pin(fut)
                }))
                .await;
            let commands = state.update(event);
            executor.exec(commands);
        }
    }
}
