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

use alloy_primitives::B256;
use bytes::Bytes;
use futures::{task::noop_waker_ref, SinkExt, StreamExt};
use monad_chain_config::{
    revision::{ChainRevision, MockChainRevision},
    ChainConfig, MockChainConfig,
};
use monad_consensus_types::block::GENESIS_TIMESTAMP;
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx, secret_to_eth_address, S1, S2};
use monad_eth_txpool_executor::{
    forward::{egress_max_size_bytes, EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF},
    EthTxPoolExecutor, EthTxPoolIpcConfig,
};
use monad_eth_txpool_ipc::EthTxPoolIpcClient;
use monad_eth_txpool_types::EthTxPoolSnapshot;
use monad_eth_types::EthExecutionProtocol;
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Balance, Round, SeqNum, GENESIS_ROUND, GENESIS_SEQ_NUM};
use monad_updaters::TokioTaskUpdater;

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;
type BlockPolicyType =
    EthBlockPolicy<SignatureType, SignatureCollectionType, MockChainConfig, MockChainRevision>;

async fn send_txs(
    ipc_client: &mut EthTxPoolIpcClient,
    secret: B256,
    start_nonce: usize,
    count: usize,
    tx_size: usize,
) {
    for nonce in start_nonce..(start_nonce + count) {
        ipc_client
            .feed(&make_legacy_tx(
                secret,
                MIN_BASE_FEE.into(),
                30_000_000,
                nonce as u64,
                tx_size,
            ))
            .await
            .unwrap();
    }
    ipc_client.flush().await.unwrap();
}

async fn collect_forwarded_txs(
    txpool_executor: &mut TokioTaskUpdater<
        TxPoolCommand<
            SignatureType,
            SignatureCollectionType,
            EthExecutionProtocol,
            BlockPolicyType,
            StateBackendType,
            MockChainConfig,
            MockChainRevision,
        >,
        MonadEvent<SignatureType, SignatureCollectionType, EthExecutionProtocol>,
    >,
    timeout_ms: u64,
) -> (usize, usize) {
    let mut total_bytes = 0;
    let mut total_txs = 0;

    let timer = tokio::time::sleep(Duration::from_millis(timeout_ms));
    tokio::pin!(timer);

    loop {
        tokio::select! {
            _ = &mut timer => {
                break;
            }
            result = txpool_executor.next() => {
                if let Some(MonadEvent::MempoolEvent(MempoolEvent::ForwardTxs(vec))) = result {
                    let batch_size: usize = vec.iter().map(Bytes::len).sum();
                    total_bytes += batch_size;
                    total_txs += vec.len();
                }
            }
        }
    }

    (total_bytes, total_txs)
}

async fn setup_txpool_executor_with_client() -> (
    TokioTaskUpdater<
        TxPoolCommand<
            SignatureType,
            SignatureCollectionType,
            EthExecutionProtocol,
            BlockPolicyType,
            StateBackendType,
            MockChainConfig,
            MockChainRevision,
        >,
        MonadEvent<SignatureType, SignatureCollectionType, EthExecutionProtocol>,
    >,
    EthTxPoolIpcClient,
) {
    let eth_block_policy = EthBlockPolicy::new(GENESIS_SEQ_NUM, u64::MAX);

    let state_backend: StateBackendType = InMemoryStateInner::new(
        Balance::MAX,
        SeqNum::MAX,
        InMemoryBlockState::genesis(BTreeMap::from_iter([
            (secret_to_eth_address(S1), 0),
            (secret_to_eth_address(S2), 0),
        ])),
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
        Duration::from_secs(3600),
        Duration::from_secs(3600),
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

    let (ipc_client, EthTxPoolSnapshot { pending, tracked }) =
        EthTxPoolIpcClient::new(bind_path).await.unwrap();

    assert!(pending.is_empty());
    assert!(tracked.is_empty());

    (txpool_executor, ipc_client)
}

#[tokio::test]
async fn test_ipc_tx_forwarding_pacing() {
    let (mut txpool_executor, mut ipc_client) = setup_txpool_executor_with_client().await;

    let mut cx = Context::from_waker(noop_waker_ref());

    assert!(txpool_executor.poll_next_unpin(&mut cx).is_pending());

    const NUM_TXS: usize = 32;
    const MAX_TXS_PER_BLOCK: usize = 16;
    let tx_size = egress_max_size_bytes(
        MockChainConfig::DEFAULT
            .get_execution_chain_revision(0)
            .execution_chain_params(),
    ) / MAX_TXS_PER_BLOCK
        - 256;

    send_txs(&mut ipc_client, S1, 0, NUM_TXS, tx_size).await;

    let mut forwarded_txs = 0;

    while forwarded_txs < NUM_TXS {
        let event;
        let mut retries = 0;

        loop {
            if let Poll::Ready(result) = txpool_executor.poll_next_unpin(&mut cx) {
                event = result.unwrap();
                break;
            };
            if retries > 10 {
                panic!("max retries hit");
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
            retries += 1;
        }

        match event {
            MonadEvent::MempoolEvent(mempool_event) => match mempool_event {
                MempoolEvent::ForwardTxs(vec) => {
                    assert!(!vec.is_empty());
                    assert!(vec.len() <= MAX_TXS_PER_BLOCK, "vec len was {}", vec.len());
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

    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(txpool_executor.poll_next_unpin(&mut cx).is_pending());
}

#[tokio::test]
async fn test_forwarding_limit() {
    let (mut txpool_executor, mut ipc_client) = setup_txpool_executor_with_client().await;

    let proposal_byte_limit = MockChainConfig::DEFAULT
        .get_chain_revision(GENESIS_ROUND)
        .chain_params()
        .proposal_byte_limit as usize;

    let tx_data_size = proposal_byte_limit / 10;
    let num_txs = 100;

    send_txs(&mut ipc_client, S1, 0, num_txs, tx_data_size).await;

    let (total_forwarded_bytes, total_forwarded_txs) =
        collect_forwarded_txs(&mut txpool_executor, 100).await;

    assert!(
        total_forwarded_bytes <= proposal_byte_limit,
        "should respect proposal byte limit"
    );
    assert!(
        total_forwarded_txs < num_txs,
        "should be limited by proposal byte limit"
    );
}

#[tokio::test]
async fn test_forwarding_with_block_commit() {
    let (mut txpool_executor, mut ipc_client) = setup_txpool_executor_with_client().await;

    let proposal_byte_limit = MockChainConfig::DEFAULT
        .get_chain_revision(GENESIS_ROUND)
        .chain_params()
        .proposal_byte_limit as usize;

    let first_batch_total = (proposal_byte_limit as f64 * 1.25) as usize;
    let tx_size = 50 * 1024;
    let first_batch_count = first_batch_total / tx_size;

    send_txs(&mut ipc_client, S1, 0, first_batch_count, tx_size).await;

    let (first_forwarded_bytes, first_forwarded_txs) =
        collect_forwarded_txs(&mut txpool_executor, 100).await;

    assert!(
        first_forwarded_bytes <= proposal_byte_limit,
        "first batch should respect proposal byte limit"
    );
    assert!(
        first_forwarded_txs < first_batch_count,
        "first batch should be limited by proposal byte limit"
    );
    assert!(
        first_forwarded_txs > 0,
        "first batch should forward some txs"
    );

    // txs are forward only at the last commit
    for i in 1..=EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF {
        txpool_executor.exec(vec![TxPoolCommand::BlockCommit(vec![
            generate_block_with_txs(
                GENESIS_ROUND + Round(i),
                GENESIS_SEQ_NUM + SeqNum(i),
                MIN_BASE_FEE,
                &MockChainConfig::DEFAULT,
                vec![],
            ),
        ])]);
    }

    let (after_commit_forwarded_bytes, _) = collect_forwarded_txs(&mut txpool_executor, 100).await;

    assert_ne!(
        after_commit_forwarded_bytes, 0,
        "after commit should forward remaining txs"
    );
    assert!(
        after_commit_forwarded_bytes <= proposal_byte_limit,
        "after commit should respect proposal byte limit"
    );

    let second_batch_total = (proposal_byte_limit as f64 * 0.5) as usize;
    let second_batch_count = second_batch_total / tx_size;

    send_txs(&mut ipc_client, S2, 0, second_batch_count, tx_size).await;

    let (_, second_forwarded_txs) = collect_forwarded_txs(&mut txpool_executor, 100).await;

    assert_eq!(
        second_forwarded_txs, 0,
        "second batch should not forward as limit exhausted"
    );
}
