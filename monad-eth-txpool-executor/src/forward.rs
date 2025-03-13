use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use bytes::Bytes;
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_txpool::EthTxPool;
use monad_state_backend::StateBackend;
use pin_project::pin_project;

const EGRESS_MIN_COMMITTED_SEQ_NUM_DIFF: u64 = 5;
const EGRESS_MAX_RETRIES: usize = 2;

const INGRESS_CHUNK_MAX_SIZE: usize = 128;
const INGRESS_CHUNK_INTERVAL_MS: u64 = 8;

#[pin_project(project = EthTxPoolForwardingManagerProjected)]
pub struct EthTxPoolForwardingManager {
    ingress: Vec<Recovered<TxEnvelope>>,
    #[pin]
    ingress_timer: tokio::time::Interval,
    ingress_waker: Option<Waker>,

    egress: Vec<Bytes>,
    egress_waker: Option<Waker>,
}

impl EthTxPoolForwardingManager {
    pub fn new() -> Self {
        let mut ingress_timer =
            tokio::time::interval(Duration::from_millis(INGRESS_CHUNK_INTERVAL_MS));

        ingress_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        Self {
            ingress: Vec::default(),
            ingress_timer,
            ingress_waker: None,

            egress: Vec::default(),
            egress_waker: None,
        }
    }

    pub fn poll_ingress(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Vec<Recovered<TxEnvelope>>> {
        let EthTxPoolForwardingManagerProjected {
            ingress,
            mut ingress_timer,
            ingress_waker,
            ..
        } = self.project();

        if ingress.is_empty() {
            ingress_waker.get_or_insert_with(|| cx.waker().to_owned());
            return Poll::Pending;
        }

        let Poll::Ready(_) = ingress_timer.poll_tick(cx) else {
            return Poll::Pending;
        };

        if INGRESS_CHUNK_MAX_SIZE > ingress.len() {
            Poll::Ready(std::mem::take(ingress))
        } else {
            let mut chunk = ingress.split_off(INGRESS_CHUNK_MAX_SIZE);
            std::mem::swap(ingress, &mut chunk);
            Poll::Ready(chunk)
        }
    }

    pub fn poll_egress(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<Bytes>> {
        let EthTxPoolForwardingManagerProjected {
            egress,
            egress_waker,
            ..
        } = self.project();

        if egress.is_empty() {
            egress_waker.get_or_insert_with(|| cx.waker().to_owned());
            return Poll::Pending;
        }

        Poll::Ready(std::mem::take(egress))
    }

    pub fn complete_ingress(self: Pin<&mut Self>) {
        self.get_mut().ingress_timer.reset();
    }
}

impl EthTxPoolForwardingManagerProjected<'_> {
    pub fn add_ingress_txs(&mut self, txs: Vec<Recovered<TxEnvelope>>) {
        let Self {
            ingress,
            ingress_waker,
            ..
        } = self;

        ingress.extend(txs);

        if ingress.is_empty() {
            return;
        }

        if let Some(waker) = ingress_waker.take() {
            waker.wake();
        }
    }

    pub fn add_egress_txs<ST, SCT, SBT>(&mut self, pool: &mut EthTxPool<ST, SCT, SBT>)
    where
        ST: CertificateSignatureRecoverable,
        SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
        SBT: StateBackend,
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
