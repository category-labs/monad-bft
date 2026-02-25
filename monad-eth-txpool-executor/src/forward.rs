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
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloy_consensus::TxEnvelope;
use bytes::Bytes;
use monad_chain_config::{
    execution_revision::ExecutionChainParams, revision::ChainRevision, ChainConfig,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_txpool::{max_eip2718_encoded_length, EthTxPool};
use monad_eth_types::ExtractEthAddress;
use monad_executor_glue::TX_FORWARD_RAW_MAX_SIZE_BYTES;
use monad_state_backend::StateBackend;
use monad_validator::signature_collection::SignatureCollection;
use pin_project::pin_project;
use tracing::{error, warn};

const EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF: u64 = 5;
const EGRESS_MAX_RETRIES: usize = 3;

const INGRESS_CHUNK_MAX_SIZE: usize = 128;
const INGRESS_MAX_SIZE: usize = INGRESS_CHUNK_MAX_SIZE;

pub fn egress_max_size_bytes(execution_params: &ExecutionChainParams) -> usize {
    max_eip2718_encoded_length(execution_params).min(TX_FORWARD_RAW_MAX_SIZE_BYTES)
}

#[pin_project(project = EthTxPoolForwardingManagerProjected)]
pub struct EthTxPoolForwardingManager<N> {
    ingress: VecDeque<(TxEnvelope, N)>,
    ingress_waker: Option<Waker>,

    egress: VecDeque<Bytes>,
    egress_waker: Option<Waker>,
}

impl<N> Default for EthTxPoolForwardingManager<N> {
    fn default() -> Self {
        Self {
            ingress: VecDeque::default(),
            ingress_waker: None,

            egress: VecDeque::default(),
            egress_waker: None,
        }
    }
}

impl<N> EthTxPoolForwardingManager<N> {
    pub fn ingress_is_empty(&self) -> bool {
        self.ingress.is_empty()
    }

    pub fn poll_ingress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<(TxEnvelope, N)>> {
        let EthTxPoolForwardingManagerProjected {
            ingress,
            ingress_waker,
            ..
        } = self.project();

        if ingress.is_empty() {
            match ingress_waker.as_mut() {
                Some(waker) => waker.clone_from(cx.waker()),
                None => *ingress_waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }

        Poll::Ready(
            ingress
                .drain(..INGRESS_CHUNK_MAX_SIZE.min(ingress.len()))
                .collect(),
        )
    }

    pub fn poll_egress(
        self: Pin<&mut Self>,
        execution_params: &ExecutionChainParams,
        cx: &mut Context<'_>,
    ) -> Poll<Vec<Bytes>> {
        let EthTxPoolForwardingManagerProjected {
            egress,
            egress_waker,
            ..
        } = self.project();

        loop {
            if egress.is_empty() {
                match egress_waker.as_mut() {
                    Some(waker) => waker.clone_from(cx.waker()),
                    None => *egress_waker = Some(cx.waker().clone()),
                }

                return Poll::Pending;
            }

            let egress_max_size_bytes = egress_max_size_bytes(execution_params);

            let mut txs = Vec::default();
            let mut total_bytes = 0;

            while let Some(tx) = egress.front() {
                let new_total_bytes = total_bytes + tx.len();

                if new_total_bytes <= egress_max_size_bytes {
                    txs.push(egress.pop_front().unwrap());
                    total_bytes = new_total_bytes;
                    continue;
                }

                if tx.len() > egress_max_size_bytes {
                    error!("txpool forwarding manager detected tx larger than max tx byte size, skipping forwarding");
                    egress.pop_front();
                    continue;
                }

                break;
            }

            if txs.is_empty() {
                let tx = egress.pop_front();
                error!(
                    ?tx,
                    "txpool forwarding manager detected empty forward, dropping next tx"
                );
                continue;
            }

            return Poll::Ready(txs);
        }
    }

    pub fn complete_ingress(self: Pin<&mut Self>) {
        let _ = self;
    }
}

impl<'a, N: Clone> EthTxPoolForwardingManagerProjected<'a, N> {
    pub fn add_ingress_txs(&mut self, sender: N, txs: Vec<TxEnvelope>) {
        let Self {
            ingress,
            ingress_waker,
            ..
        } = self;

        let capacity_remaining = INGRESS_MAX_SIZE.saturating_sub(ingress.len());
        let dropped = txs.len().saturating_sub(capacity_remaining);

        if dropped > 0 {
            warn!(
                ingress_len = ingress.len(),
                capacity =? INGRESS_MAX_SIZE,
                ?capacity_remaining,
                ?dropped,
                "ingress queue full, discarding forwarded txs"
            )
        }

        ingress.extend(
            txs.into_iter()
                .take(capacity_remaining)
                .map(|tx| (tx, sender.clone())),
        );

        if ingress.is_empty() {
            return;
        }

        if let Some(waker) = ingress_waker.take() {
            waker.wake();
        }
    }

    pub fn add_egress_txs<'b>(&mut self, txs: impl Iterator<Item = &'b TxEnvelope>) {
        let Self {
            egress,
            egress_waker,
            ..
        } = self;

        egress.extend(txs.map(alloy_rlp::encode).map(Into::into));

        if egress.is_empty() {
            return;
        }

        if let Some(waker) = egress_waker.take() {
            waker.wake();
        }
    }

    pub fn schedule_egress_txs<ST, SCT, SBT, CCT, CRT>(
        &mut self,
        pool: &mut EthTxPool<ST, SCT, SBT, CCT, CRT>,
    ) where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        SBT: StateBackend<ST, SCT>,
        CertificateSignaturePubKey<ST>: ExtractEthAddress,
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        let Some(forwardable_txs) =
            pool.get_forwardable_txs::<EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF, EGRESS_MAX_RETRIES>()
        else {
            return;
        };

        self.add_egress_txs(forwardable_txs);
    }
}

#[cfg(test)]
mod test {
    use std::{
        pin::{pin, Pin},
        task::{Context, Poll},
        time::Duration,
    };

    use alloy_consensus::{Transaction, TxEnvelope};
    use bytes::Bytes;
    use futures::task::noop_waker_ref;
    use itertools::Itertools;
    use monad_chain_config::execution_revision::MonadExecutionRevision;
    use monad_eth_testutil::{make_eip1559_tx, make_eip7702_tx, make_legacy_tx, S1};

    use crate::forward::{
        egress_max_size_bytes, EthTxPoolForwardingManager, INGRESS_CHUNK_MAX_SIZE,
    };

    const EXECUTION_REVISION: MonadExecutionRevision = MonadExecutionRevision::LATEST;

    const BASE_FEE_PER_GAS: u128 = 100_000_000_000; // 100 Gwei

    type TestNodeId = u64;

    fn setup<'a>() -> (EthTxPoolForwardingManager<TestNodeId>, Context<'a>) {
        (
            EthTxPoolForwardingManager::default(),
            Context::from_waker(noop_waker_ref()),
        )
    }

    fn generate_tx(nonce: u64) -> TxEnvelope {
        make_legacy_tx(S1, BASE_FEE_PER_GAS, 100_000, nonce, 0)
    }

    async fn assert_pending_now_and_forever(
        mut forwarding_manager: Pin<&mut EthTxPoolForwardingManager<TestNodeId>>,
        mut cx: Context<'_>,
    ) {
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );
        assert_eq!(
            forwarding_manager
                .as_mut()
                .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx),
            Poll::Pending
        );

        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );
        assert_eq!(
            forwarding_manager
                .as_mut()
                .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx),
            Poll::Pending
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_poll_none() {
        let (forwarding_manager, cx) = setup();
        let forwarding_manager = pin!(forwarding_manager);

        assert_pending_now_and_forever(forwarding_manager, cx).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_ingress_simple() {
        for poll_ingress_before_insert in [false, true] {
            let (forwarding_manager, mut cx) = setup();
            let mut forwarding_manager = pin!(forwarding_manager);

            if poll_ingress_before_insert {
                assert_eq!(
                    forwarding_manager.as_mut().poll_ingress(&mut cx),
                    Poll::Pending
                );
            }

            let txs = vec![generate_tx(0)];

            forwarding_manager
                .as_mut()
                .project()
                .add_ingress_txs(0, txs.clone());

            let expected: Vec<_> = txs.into_iter().map(|tx| (tx, 0)).collect();
            assert_eq!(
                forwarding_manager.as_mut().poll_ingress(&mut cx),
                Poll::Ready(expected)
            );

            assert_pending_now_and_forever(forwarding_manager, cx).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_ingress_subsequent() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        let txs = vec![generate_tx(0)];
        let expected: Vec<_> = txs.iter().cloned().map(|tx| (tx, 0)).collect();

        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs(0, txs.clone());

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Ready(expected.clone())
        );
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs(0, txs.clone());

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Ready(expected)
        );

        assert_pending_now_and_forever(forwarding_manager, cx).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_ingress_chunks() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        forwarding_manager.as_mut().project().add_ingress_txs(
            0,
            (0..2 * INGRESS_CHUNK_MAX_SIZE as u64)
                .map(generate_tx)
                .collect_vec(),
        );

        let Poll::Ready(txs_with_senders) = forwarding_manager.as_mut().poll_ingress(&mut cx)
        else {
            panic!("forwarding manager should be ready");
        };

        assert_eq!(txs_with_senders.len(), INGRESS_CHUNK_MAX_SIZE);

        // Only the first chunk is kept when overfilled.
        txs_with_senders
            .into_iter()
            .enumerate()
            .for_each(|(idx, (tx, _sender))| {
                assert_eq!(tx.nonce(), idx as u64);
            });

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        assert_pending_now_and_forever(forwarding_manager, cx).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_ingress_complete() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        forwarding_manager.as_mut().project().add_ingress_txs(
            0,
            (0..2 * INGRESS_CHUNK_MAX_SIZE as u64)
                .map(generate_tx)
                .collect_vec(),
        );

        let Poll::Ready(txs_with_senders) = forwarding_manager.as_mut().poll_ingress(&mut cx)
        else {
            panic!("forwarding manager should be ready");
        };
        assert_eq!(txs_with_senders.len(), INGRESS_CHUNK_MAX_SIZE);
        forwarding_manager.as_mut().complete_ingress();

        // Ingress completion is a no-op with single-chunk no-buffer ingress.
        forwarding_manager.as_mut().project().add_ingress_txs(
            0,
            (0..INGRESS_CHUNK_MAX_SIZE as u64)
                .map(generate_tx)
                .collect_vec(),
        );
        assert_eq!(
            forwarding_manager
                .as_mut()
                .poll_ingress(&mut cx)
                .map(|txs| txs.len()),
            Poll::Ready(INGRESS_CHUNK_MAX_SIZE)
        );

        assert_pending_now_and_forever(forwarding_manager, cx).await;
    }

    #[tokio::test]
    async fn test_egress_limit() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        let mut egress_txs = Vec::new();
        let mut total_size = 0;
        let target_size = 448 * 1024;

        let mut nonce = 0u64;
        while total_size < target_size {
            let tx = generate_tx(nonce);
            total_size += tx.eip2718_encoded_length();
            egress_txs.push(tx);
            nonce += 1;
        }

        let actual_total_size = egress_txs
            .iter()
            .map(|b| b.eip2718_encoded_length())
            .sum::<usize>();
        assert!(actual_total_size >= target_size);

        forwarding_manager
            .as_mut()
            .project()
            .add_egress_txs(egress_txs.iter());

        let mut total_batches = 0usize;
        let mut total_txs = 0usize;
        let mut total_size_forwarded = 0usize;
        let egress_cap = egress_max_size_bytes(EXECUTION_REVISION.execution_chain_params());

        loop {
            match forwarding_manager
                .as_mut()
                .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx)
            {
                Poll::Ready(batch) => {
                    let batch_size: usize = batch.iter().map(|b| b.len()).sum();
                    assert!(batch_size <= egress_cap);
                    assert!(!batch.is_empty());

                    total_batches += 1;
                    total_txs += batch.len();
                    total_size_forwarded += batch_size;
                }
                Poll::Pending => break,
            }
        }

        assert!(total_batches >= 1);
        assert_eq!(total_txs, egress_txs.len());
        assert_eq!(total_size_forwarded, actual_total_size);

        assert_eq!(
            forwarding_manager
                .as_mut()
                .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx),
            Poll::Pending
        )
    }

    #[tokio::test]
    async fn test_egress_limit_exceeded() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        let legacy_tx_generator =
            |nonce, input_len| make_legacy_tx(S1, BASE_FEE_PER_GAS, 30_000_000, nonce, input_len);
        let eip1559_tx_generator = |nonce, input_len| {
            make_eip1559_tx(S1, BASE_FEE_PER_GAS, 0, 30_000_000, nonce, input_len)
        };
        let eip7702_tx_generator = |nonce, input_len| {
            make_eip7702_tx(
                S1,
                BASE_FEE_PER_GAS,
                0,
                30_000_000,
                nonce,
                vec![],
                input_len,
            )
        };

        for tx_generator in [
            legacy_tx_generator,
            eip1559_tx_generator,
            eip7702_tx_generator,
        ] {
            let tx1 = tx_generator(0, 0);
            assert!(
                tx1.eip2718_encoded_length()
                    <= egress_max_size_bytes(EXECUTION_REVISION.execution_chain_params())
            );

            let tx2 = tx_generator(
                1,
                egress_max_size_bytes(EXECUTION_REVISION.execution_chain_params()),
            );
            assert!(
                tx2.eip2718_encoded_length()
                    > egress_max_size_bytes(EXECUTION_REVISION.execution_chain_params())
            );

            let tx3 = tx_generator(2, 0);
            assert!(
                tx3.eip2718_encoded_length()
                    <= egress_max_size_bytes(EXECUTION_REVISION.execution_chain_params())
            );

            forwarding_manager
                .as_mut()
                .project()
                .add_egress_txs([&tx1, &tx2, &tx3].into_iter());

            let Poll::Ready(first_batch) = forwarding_manager
                .as_mut()
                .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx)
            else {
                panic!("first poll should be ready");
            };

            eprintln!("{first_batch:#?}\n{tx1:#?}\n{tx3:#?}");

            assert_eq!(first_batch.len(), 2);
            assert_eq!(
                first_batch.iter().map(Bytes::len).sum::<usize>(),
                alloy_rlp::encode(tx1).len() + alloy_rlp::encode(tx3).len(),
            );

            assert_eq!(
                forwarding_manager
                    .as_mut()
                    .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx),
                Poll::Pending
            )
        }
    }
}
