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
    pin::Pin,
    task::{Context, Poll, Waker},
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use bytes::Bytes;
use monad_chain_config::{
    execution_revision::ExecutionChainParams, revision::ChainRevision, ChainConfig,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_txpool::{max_eip2718_encoded_length, EthTxPool};
use monad_eth_types::ExtractEthAddress;
use monad_execution_state_read::ExecutionStateRead;
use monad_types::{LimitedVec, MAX_FORWARDED_TXS_PER_MESSAGE};
use monad_validator::signature_collection::SignatureCollection;
use pin_project::pin_project;
use tracing::error;

use crate::batcher::Batcher;

// smaller messages let leaders process transactions before the full input
// list arrives. a lost datagram then blocks fewer transactions, reducing
// the amount of transaction data that needs to be retried.
const EGRESS_BATCH_TARGET_BYTES: usize = 16 * 1024;
const EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF: u64 = 5;
const EGRESS_MAX_RETRIES: usize = 3;

pub(crate) const INGRESS_CHUNK_MAX_SIZE: usize = 128;
pub(crate) const INGRESS_CHUNK_INTERVAL_MS: u64 = 8;

pub fn egress_max_size_bytes(execution_params: &ExecutionChainParams) -> usize {
    max_eip2718_encoded_length(execution_params)
}

#[pin_project(project = EthTxPoolForwardingManagerProjected)]
pub struct EthTxPoolForwardingManager<S> {
    ingress_batch: Option<Vec<(S, TxEnvelope)>>,
    ingress_waker: Option<Waker>,

    egress: Batcher,
    egress_waker: Option<Waker>,
}

impl<S> Default for EthTxPoolForwardingManager<S> {
    fn default() -> Self {
        Self {
            ingress_batch: None,
            ingress_waker: None,

            egress: Batcher::new(EGRESS_BATCH_TARGET_BYTES),
            egress_waker: None,
        }
    }
}

impl<S> EthTxPoolForwardingManager<S> {
    pub fn ingress_is_empty(&self) -> bool {
        self.ingress_batch.is_none()
    }

    pub fn poll_ingress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<(S, TxEnvelope)>> {
        let EthTxPoolForwardingManagerProjected {
            ingress_batch,
            ingress_waker,
            ..
        } = self.project();

        if let Some(txs) = ingress_batch.take() {
            return Poll::Ready(txs);
        }

        match ingress_waker.as_mut() {
            Some(waker) => waker.clone_from(cx.waker()),
            None => *ingress_waker = Some(cx.waker().clone()),
        }

        Poll::Pending
    }

    pub fn poll_egress(
        self: Pin<&mut Self>,
        execution_params: &ExecutionChainParams,
        cx: &mut Context<'_>,
    ) -> Poll<LimitedVec<Bytes, MAX_FORWARDED_TXS_PER_MESSAGE>> {
        let EthTxPoolForwardingManagerProjected {
            egress,
            egress_waker,
            ..
        } = self.project();

        if let Some(txs) = egress
            .batches(egress_max_size_bytes(execution_params))
            .next()
        {
            return Poll::Ready(txs);
        }

        match egress_waker.as_mut() {
            Some(waker) => waker.clone_from(cx.waker()),
            None => *egress_waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }
}

impl<S> EthTxPoolForwardingManagerProjected<'_, S> {
    pub fn add_ingress_txs(&mut self, txs: Vec<(S, TxEnvelope)>) {
        let Self {
            ingress_batch,
            ingress_waker,
            ..
        } = self;

        if txs.is_empty() {
            return;
        }

        if let Some(batch) = ingress_batch.as_mut() {
            error!(
                current_batch_len = batch.len(),
                dropped_batch_len = txs.len(),
                "txpool forwarding manager ingress batch already pending, dropping new batch"
            );
        } else {
            **ingress_batch = Some(txs);
        }

        if let Some(waker) = ingress_waker.take() {
            waker.wake();
        }
    }

    pub fn add_egress_txs<'a>(&mut self, txs: impl Iterator<Item = &'a Recovered<TxEnvelope>>) {
        let Self {
            egress,
            egress_waker,
            ..
        } = self;

        egress.extend(txs);

        if egress.is_empty() {
            return;
        }

        if let Some(waker) = egress_waker.take() {
            waker.wake();
        }
    }

    pub fn schedule_egress_txs<ST, SCT, ESRT, CCT, CRT>(
        &mut self,
        pool: &mut EthTxPool<ST, SCT, ESRT, CCT, CRT>,
    ) where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        ESRT: ExecutionStateRead<ST, SCT>,
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
    use alloy_eips::Encodable2718;
    use alloy_rlp::Encodable;
    use bytes::Bytes;
    use futures::task::noop_waker_ref;
    use monad_chain_config::execution_revision::MonadExecutionRevision;
    use monad_eth_testutil::{make_eip1559_tx, make_eip7702_tx, make_legacy_tx, recover_tx, S1};

    use crate::forward::{
        egress_max_size_bytes, EthTxPoolForwardingManager, EGRESS_BATCH_TARGET_BYTES,
    };

    const EXECUTION_REVISION: MonadExecutionRevision = MonadExecutionRevision::LATEST;

    const BASE_FEE_PER_GAS: u128 = 100_000_000_000; // 100 Gwei

    fn setup<'a>() -> (EthTxPoolForwardingManager<()>, Context<'a>) {
        (
            EthTxPoolForwardingManager::default(),
            Context::from_waker(noop_waker_ref()),
        )
    }

    fn generate_tx(nonce: u64) -> TxEnvelope {
        make_legacy_tx(S1, BASE_FEE_PER_GAS, 100_000, nonce, 0)
    }

    fn generate_ingress_tx(nonce: u64) -> ((), TxEnvelope) {
        ((), generate_tx(nonce))
    }

    async fn assert_pending_now_and_forever(
        mut forwarding_manager: Pin<&mut EthTxPoolForwardingManager<()>>,
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

            let txs = vec![generate_ingress_tx(0)];

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

        let txs = vec![generate_ingress_tx(0)];

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

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Ready(txs.clone())
        );

        assert_pending_now_and_forever(forwarding_manager, cx).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_ingress_drops_new_pending_batch() {
        let (forwarding_manager, mut cx) = setup();
        let mut forwarding_manager = pin!(forwarding_manager);

        assert_eq!(
            forwarding_manager.as_mut().poll_ingress(&mut cx),
            Poll::Pending
        );

        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs(vec![generate_ingress_tx(0), generate_ingress_tx(1)]);
        forwarding_manager
            .as_mut()
            .project()
            .add_ingress_txs(vec![generate_ingress_tx(2)]);

        let Poll::Ready(txs) = forwarding_manager.as_mut().poll_ingress(&mut cx) else {
            panic!("forwarding manager should be ready");
        };
        assert_eq!(txs.len(), 2);
        assert_eq!(
            txs.into_iter()
                .map(|(_, tx)| tx.nonce())
                .collect::<Vec<_>>(),
            vec![0, 1]
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
            egress_txs.push(recover_tx(tx));
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

        let mut forwarded = Vec::new();
        while let Poll::Ready(batch) = forwarding_manager
            .as_mut()
            .poll_egress(EXECUTION_REVISION.execution_chain_params(), &mut cx)
        {
            assert!(!batch.is_empty());
            assert!(batch.length() <= EGRESS_BATCH_TARGET_BYTES);
            forwarded.extend(batch);
        }
        assert_eq!(forwarded.len(), egress_txs.len());
        assert_eq!(
            forwarded.iter().map(Bytes::len).sum::<usize>(),
            actual_total_size
        );
        assert_eq!(
            forwarded,
            egress_txs
                .iter()
                .map(|tx| Bytes::from(tx.encoded_2718()))
                .collect::<Vec<_>>()
        );
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
                .add_egress_txs([tx1.clone(), tx2, tx3.clone()].map(recover_tx).iter());

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
                tx1.eip2718_encoded_length() + tx3.eip2718_encoded_length(),
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
