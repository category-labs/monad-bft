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
    time::Duration,
};

use alloy_consensus::TxEnvelope;
use bytes::Bytes;
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_txpool::EthTxPool;
use monad_eth_types::ExtractEthAddress;
use monad_state_backend::StateBackend;
use monad_validator::signature_collection::SignatureCollection;
use pin_project::pin_project;
use tracing::debug;

const EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF: u64 = 5;
const EGRESS_MAX_RETRIES: usize = 3;

const INGRESS_CHUNK_MAX_SIZE: usize = 128;
const INGRESS_CHUNK_INTERVAL_MS: u64 = 8;
const INGRESS_MAX_SIZE: usize = 8 * 1024;
const EGRESS_MAX_SIZE_BYTES: usize = 256 * 1024;

#[pin_project(project = EthTxPoolForwardingManagerProjected)]
pub struct EthTxPoolForwardingManager {
    ingress: VecDeque<TxEnvelope>,
    #[pin]
    ingress_timer: tokio::time::Interval,
    ingress_waker: Option<Waker>,

    egress: VecDeque<Bytes>,
    egress_waker: Option<Waker>,
}

impl EthTxPoolForwardingManager {
    pub fn new() -> Self {
        let mut ingress_timer =
            tokio::time::interval(Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS));

        ingress_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Self {
            ingress: VecDeque::default(),
            ingress_timer,
            ingress_waker: None,

            egress: VecDeque::default(),
            egress_waker: None,
        }
    }

    pub fn poll_ingress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<TxEnvelope>> {
        let EthTxPoolForwardingManagerProjected {
            ingress,
            mut ingress_timer,
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

        let Poll::Ready(_) = ingress_timer.poll_tick(cx) else {
            return Poll::Pending;
        };

        Poll::Ready(
            ingress
                .drain(..INGRESS_CHUNK_MAX_SIZE.min(ingress.len()))
                .collect(),
        )
    }

    pub fn poll_egress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<Bytes>> {
        let EthTxPoolForwardingManagerProjected {
            egress,
            egress_waker,
            ..
        } = self.project();

        if egress.is_empty() {
            match egress_waker.as_mut() {
                Some(waker) => waker.clone_from(cx.waker()),
                None => *egress_waker = Some(cx.waker().clone()),
            }
            return Poll::Pending;
        }

        let mut total_size = 0;
        let mut drain_count = 0;

        for (i, tx) in egress.iter().enumerate() {
            let tx_size = tx.len();
            if total_size + tx_size > EGRESS_MAX_SIZE_BYTES && i > 0 {
                break;
            }
            total_size += tx_size;
            drain_count = i + 1;
        }
        Poll::Ready(egress.drain(..drain_count).collect())
    }

    pub fn complete_ingress(self: Pin<&mut Self>) {
        self.get_mut().ingress_timer.reset();
    }
}

impl EthTxPoolForwardingManagerProjected<'_> {
    pub fn add_ingress_txs(&mut self, txs: Vec<TxEnvelope>) {
        let Self {
            ingress,
            ingress_waker,
            ..
        } = self;

        let capacity_remaining = INGRESS_MAX_SIZE.saturating_sub(ingress.len());

        let dropped = txs.len().saturating_sub(capacity_remaining);

        if dropped > 0 {
            debug!(
                capacity =? INGRESS_MAX_SIZE,
                ?capacity_remaining,
                ?dropped,
                "ingress queue full, discarding forwarded txs"
            )
        }

        ingress.extend(txs.into_iter().take(capacity_remaining));

        if ingress.is_empty() {
            return;
        }

        if let Some(waker) = ingress_waker.take() {
            waker.wake();
        }
    }

    pub fn add_egress_txs<ST, SCT, SBT, CCT, CRT>(
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

        let Self {
            egress,
            egress_waker,
            ..
        } = self;

        egress.extend(
            forwardable_txs
                .cloned()
                .map(alloy_rlp::encode)
                .map(Into::into),
        );

        if egress.is_empty() {
            return;
        }

        if let Some(waker) = egress_waker.take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        pin::{pin, Pin},
        task::{Context, Poll},
        time::Duration,
    };

    use alloy_consensus::{transaction::Recovered, Transaction, TxEnvelope};
    use alloy_primitives::{hex, B256};
    use bytes::Bytes;
    use futures::task::noop_waker_ref;
    use itertools::Itertools;
    use monad_eth_testutil::{make_legacy_tx, recover_tx};

    use crate::forward::{
        EthTxPoolForwardingManager, EGRESS_MAX_SIZE_BYTES, INGRESS_CHUNK_INTERVAL_MS,
        INGRESS_CHUNK_MAX_SIZE,
    };

    // pubkey starts with AAA
    const S1: B256 = B256::new(hex!(
        "0ed2e19e3aca1a321349f295837988e9c6f95d4a6fc54cfab6befd5ee82662ad"
    ));
    const BASE_FEE_PER_GAS: u128 = 100_000_000_000; // 100 Gwei

    fn setup<'a>() -> (EthTxPoolForwardingManager, Context<'a>) {
        (
            EthTxPoolForwardingManager::new(),
            Context::from_waker(noop_waker_ref()),
        )
    }

    fn generate_tx(nonce: u64) -> TxEnvelope {
        make_legacy_tx(S1, BASE_FEE_PER_GAS, 100_000, nonce, 0)
    }

    fn generate_recovered_tx(nonce: u64) -> Recovered<TxEnvelope> {
        recover_tx(generate_tx(nonce))
    }

    async fn assert_pending_now_and_forever(
        mut forwarding_manager: Pin<&mut EthTxPoolForwardingManager>,
        mut cx: Context<'_>,
    ) {
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );
        assert_eq!(
            forwarding_manager.as_mut().poll_egress(&mut cx),
            Poll::Pending
        );

        tokio::time::advance(Duration::from_secs(24 * 60 * 60)).await;

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );
        assert_eq!(
            forwarding_manager.as_mut().poll_egress(&mut cx),
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
                .add_ingress_txs(txs.clone());

            assert_eq!(
                forwarding_manager.as_mut().poll_ingress(&mut cx),
                Poll::Ready(txs.clone())
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

        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs(txs.clone());

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Ready(txs.clone())
        );
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs(txs.clone());

        // Since time is frozen and we just polled, the forwarding manager should wait its interval
        // even though it should be "empty"
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        tokio::time::advance(
            Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS)
                .checked_sub(Duration::from_nanos(1))
                .unwrap(),
        )
        .await;
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Ready(txs.clone())
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

        const NUM_CHUNKS: usize = 16;

        // We insert the last tx below to test adding to an existing chunk
        const NUM_TXS: usize = INGRESS_CHUNK_MAX_SIZE * NUM_CHUNKS - 1;

        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs((0..NUM_TXS as u64).map(generate_tx).collect_vec());

        for chunk_num in 0..NUM_CHUNKS {
            tokio::time::advance(Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS)).await;

            if chunk_num + 1 == NUM_CHUNKS {
                forwarding_manager
                    .as_mut()
                    .project()
                    .add_ingress_txs(vec![generate_tx(0)]);
            }

            let Poll::Ready(txs) = forwarding_manager.as_mut().poll_ingress(&mut cx) else {
                panic!("forwarding manager should be ready after each iteration");
            };

            assert_eq!(txs.len(), INGRESS_CHUNK_MAX_SIZE);

            // Check that txs are produced in the same order they are inserted
            txs.into_iter().enumerate().for_each(|(idx, tx)| {
                // By using % NUM_TXS, we can check that the last tx is the 0 nonce added above when
                // we're at the last chunk
                assert_eq!(
                    tx.nonce(),
                    ((idx + chunk_num * INGRESS_CHUNK_MAX_SIZE) as u64) % (NUM_TXS as u64)
                );
            });

            assert_eq!(
                forwarding_manager.as_mut().poll_ingress(&mut cx),
                Poll::Pending
            );
        }

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
            (0..2 * INGRESS_CHUNK_MAX_SIZE as u64)
                .map(generate_tx)
                .collect_vec(),
        );

        let Poll::Ready(txs) = forwarding_manager.as_mut().poll_ingress(&mut cx) else {
            panic!("forwarding manager should be ready");
        };
        assert_eq!(txs.len(), INGRESS_CHUNK_MAX_SIZE);

        tokio::time::advance(Duration::from_millis(1)).await;

        forwarding_manager.as_mut().complete_ingress();

        tokio::time::advance(
            Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS)
                .checked_sub(Duration::from_millis(1))
                .unwrap(),
        )
        .await;

        // Even though we have advanced INGRESS_CHUNK_INTERVAL_MS, the forwarding manager should
        // wait an additional 1ms since complete_ingress was called 1ms after the poll.
        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        tokio::time::advance(Duration::from_millis(1)).await;

        let Poll::Ready(txs) = forwarding_manager.as_mut().poll_ingress(&mut cx) else {
            panic!("forwarding manager should be ready");
        };
        assert_eq!(txs.len(), INGRESS_CHUNK_MAX_SIZE);

        assert_pending_now_and_forever(forwarding_manager, cx).await;
    }

    #[tokio::test]
    async fn test_egress_256kb_limit() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        let mut egress_txs = Vec::new();
        let mut total_size = 0;
        let target_size = 448 * 1024;

        let mut nonce = 0u64;
        while total_size < target_size {
            let tx = generate_recovered_tx(nonce);
            let encoded = alloy_rlp::encode(&tx);
            let tx_bytes: Bytes = encoded.into();
            total_size += tx_bytes.len();
            egress_txs.push(tx_bytes);
            nonce += 1;
        }

        let actual_total_size = egress_txs.iter().map(|b| b.len()).sum::<usize>();
        assert!(actual_total_size >= target_size);

        forwarding_manager
            .as_mut()
            .project()
            .egress
            .extend(egress_txs.clone());

        let Poll::Ready(first_batch) = forwarding_manager.as_mut().poll_egress(&mut cx) else {
            panic!("first poll should be ready");
        };

        let first_batch_size: usize = first_batch.iter().map(|b| b.len()).sum();
        assert!(first_batch_size <= EGRESS_MAX_SIZE_BYTES,);
        assert!(!first_batch.is_empty());

        let Poll::Ready(second_batch) = forwarding_manager.as_mut().poll_egress(&mut cx) else {
            panic!("second poll should be ready");
        };

        let second_batch_size: usize = second_batch.iter().map(|b| b.len()).sum();
        assert!(!second_batch.is_empty());

        assert_eq!(first_batch_size + second_batch_size, actual_total_size,);
        assert_eq!(first_batch.len() + second_batch.len(), egress_txs.len(),);

        assert_eq!(
            forwarding_manager.as_mut().poll_egress(&mut cx),
            Poll::Pending,
        );
    }
}
