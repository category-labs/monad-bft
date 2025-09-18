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

use alloy_primitives::{hex, B256};
use bytes::Bytes;
use futures::{task::noop_waker_ref, SinkExt, StreamExt};
use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_consensus_types::block::GENESIS_TIMESTAMP;
use monad_crypto::NopSignature;
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_testutil::{generate_block_with_txs, make_legacy_tx, secret_to_eth_address};
use monad_eth_txpool_executor::{EthTxPoolExecutor, EthTxPoolIpcConfig};
use monad_eth_txpool_ipc::EthTxPoolIpcClient;
use monad_eth_txpool_types::EthTxPoolSnapshot;
use monad_eth_types::EthExecutionProtocol;
use monad_executor::Executor;
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::{InMemoryBlockState, InMemoryState, InMemoryStateInner};
use monad_testutil::signing::MockSignatures;
use monad_tfm::base_fee::MIN_BASE_FEE;
use monad_types::{Balance, SeqNum, GENESIS_ROUND, GENESIS_SEQ_NUM};
use monad_updaters::TokioTaskUpdater;

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
type StateBackendType = InMemoryState<SignatureType, SignatureCollectionType>;
type BlockPolicyType =
    EthBlockPolicy<SignatureType, SignatureCollectionType, MockChainConfig, MockChainRevision>;

// pubkey starts with AAA
const S1: B256 = B256::new(hex!(
    "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
));

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
        InMemoryBlockState::genesis(BTreeMap::from_iter([(secret_to_eth_address(S1), 0)])),
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

    const NUM_TXS: usize = 1024;

    for nonce in 0..NUM_TXS {
        ipc_client
            .feed(&make_legacy_tx(
                S1,
                MIN_BASE_FEE.into(),
                30_000_000,
                nonce as u64,
                100_000,
            ))
            .await
            .unwrap();
    }

    ipc_client.flush().await.unwrap();

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
                    assert!(vec.len() <= 2, "vec len was {}", vec.len());
                    assert!(vec.iter().map(Bytes::len).sum::<usize>() < 256 * 1024);

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
