// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::BTreeMap,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures::{task::noop_waker_ref, SinkExt, StreamExt};
use monad_chain_config::{revision::MockChainRevision, ChainConfig, MockChainConfig};
use monad_consensus_types::block::GENESIS_TIMESTAMP;
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx, secret_to_eth_address, S1};
use monad_eth_txpool_executor::{
    forward::egress_max_size_bytes, EthTxPoolExecutor, EthTxPoolExecutorClient, EthTxPoolIpcConfig,
};
use monad_eth_txpool_ipc::EthTxPoolIpcClient;
use monad_eth_txpool_types::{EthTxPoolIpcTx, EthTxPoolSnapshot};
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::{AccountState, InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{SeqNum, GENESIS_ROUND, GENESIS_SEQ_NUM};

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;

async fn setup_txpool_executor_with_client() -> (
    EthTxPoolExecutorClient<
        SignatureType,
        SignatureCollectionType,
        StateBackendType,
        MockChainConfig,
        MockChainRevision,
    >,
    EthTxPoolIpcClient,
) {
    let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, u64::MAX);

    let state_backend: StateBackendType = InMemoryStateInner::new(
        SeqNum::MAX,
        InMemoryBlockState::genesis(BTreeMap::from_iter([(
            secret_to_eth_address(S1),
            AccountState::max_balance(),
        )])),
    );

    let ipc_tempdir = tempfile::tempdir().unwrap();
    let bind_path = ipc_tempdir.path().join("txpool_executor_test.socket");

    let mut txpool_executor = EthTxPoolExecutor::start(
        eth_block_policy,
        state_backend,
        EthTxPoolIpcConfig {
            bind_path: bind_path.clone(),
            tx_batch_size: 128,
            max_queued_batches: 1024,
            queued_batches_watermark: 512,
        },
        Duration::MAX,
        Duration::MAX,
        MockChainConfig::DEFAULT,
        GENESIS_ROUND,
        GENESIS_TIMESTAMP as u64,
        true,
    )
    .unwrap();

    txpool_executor.exec(vec![TxPoolCommand::Reset {
        last_delay_committed_blocks: vec![generate_block_with_txs(
            GENESIS_ROUND,
            GENESIS_SEQ_NUM,
            MIN_BASE_FEE,
            &MockChainConfig::DEFAULT,
            vec![],
        )],
    }]);

    let (ipc_client, EthTxPoolSnapshot { txs }) = EthTxPoolIpcClient::new(bind_path).await.unwrap();

    assert!(txs.is_empty());

    (txpool_executor, ipc_client)
}

#[tokio::test]
async fn test_ipc_tx_forwarding_pacing() {
    let (mut txpool_executor, mut ipc_client) = setup_txpool_executor_with_client().await;

    assert!(
        tokio::time::timeout(Duration::from_secs(1), txpool_executor.next())
            .await
            .is_err()
    );

    const NUM_TXS: usize = 1024;

    let handle = tokio::task::spawn(async move {
        for nonce in 0..NUM_TXS {
            ipc_client
                .feed(EthTxPoolIpcTx::new_with_default_priority(
                    make_legacy_tx(
                        S1,
                        MIN_BASE_FEE.into(),
                        30_000_000,
                        nonce as u64,
                        egress_max_size_bytes(
                            MockChainConfig::DEFAULT
                                .get_execution_chain_revision(0)
                                .execution_chain_params(),
                        ) / 2
                            - 256,
                    ),
                    Vec::default(),
                ))
                .await
                .unwrap();
        }

        ipc_client.flush().await.unwrap();

        ipc_client
    });

    let mut forwarded_txs = 0;

    while forwarded_txs < NUM_TXS {
        let event = tokio::time::timeout(Duration::from_secs(1), txpool_executor.next())
            .await
            .expect("TxpoolExecutor does not timeout")
            .unwrap();

        match event {
            MonadEvent::MempoolEvent(mempool_event) => match mempool_event {
                MempoolEvent::ForwardTxs(vec) => {
                    assert!(!vec.is_empty());
                    assert!(vec.len() <= 2, "vec len was {}", vec.len());
                    assert!(
                        vec.iter().map(Bytes::len).sum::<usize>()
                            <= egress_max_size_bytes(
                                MockChainConfig::DEFAULT
                                    .get_execution_chain_revision(0)
                                    .execution_chain_params(),
                            )
                    );

                    forwarded_txs += vec.len();
                }
                _ => panic!("txpool executor emitted non-forwward event"),
            },
            _ => panic!("txpool executor emitted non-mempool event"),
        }
    }

    assert_eq!(forwarded_txs, NUM_TXS);

    assert!(
        tokio::time::timeout(Duration::from_secs(1), txpool_executor.next())
            .await
            .is_err()
    );

    let ipc_client = handle.await.unwrap();
    drop(ipc_client);
}

#[tokio::test]
async fn test_ipc_full() {
    let (mut txpool_executor, mut ipc_client) = setup_txpool_executor_with_client().await;

    let mut cx = Context::from_waker(noop_waker_ref());

    assert!(txpool_executor.poll_next_unpin(&mut cx).is_pending());

    const TX_BYTES: usize = 256 * 1024;

    let handle = tokio::task::spawn(async move {
        for nonce in 0.. {
            let tx = make_legacy_tx(S1, MIN_BASE_FEE.into(), 30_000_000, nonce as u64, TX_BYTES);

            match tokio::time::timeout(
                std::time::Duration::from_millis(10),
                ipc_client.send(EthTxPoolIpcTx::new_with_default_priority(tx, vec![])),
            )
            .await
            {
                Ok(Ok(())) => continue,
                Err(_) => break,
                Ok(Err(err)) => panic!("send failed: {err:#?}"),
            }
        }

        ipc_client
    });

    // Wait for executor to process some events
    tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;

    // IPC socket is full on send side
    assert!(txpool_executor
        .poll_next_unpin(&mut cx)
        .map(|result| result.unwrap())
        .is_ready());

    while let Poll::Ready(result) = txpool_executor.poll_next_unpin(&mut cx) {
        assert!(result.is_some());
        tokio::task::yield_now().await;
    }

    let tx = make_legacy_tx(S1, MIN_BASE_FEE.into(), 30_000_000, 0, 0);

    let mut ipc_client = handle.await.unwrap();

    // IPC socket send succeeds

    let () = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        ipc_client.send(EthTxPoolIpcTx::new_with_default_priority(tx, vec![])),
    )
    .await
    .unwrap()
    .unwrap();
}
