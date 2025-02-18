use std::{future::Future, io, pin::Pin, task::Poll, time::Duration};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use futures::{Stream, StreamExt};
use monad_consensus_types::signature_collection::SignatureCollection;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_txpool::EthTxPool;
use monad_eth_txpool_ipc::EthTxPoolIpcStream;
use monad_eth_txpool_types::{EthTxPoolEvent, EthTxPoolSnapshot};
use monad_state_backend::StateBackend;
use pin_project::pin_project;
use tokio::{
    net::UnixListener,
    time::{self, Sleep},
};
use tracing::{info, warn};

pub use self::config::EthTxPoolIpcConfig;

mod config;

const MAX_BATCH_LEN: usize = 128;
const BATCH_TIMER_INTERVAL_MS: u64 = 10;

#[pin_project(project = EthTxPoolIpcServerProjected)]
pub struct EthTxPoolIpcServer<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    #[pin]
    listener: UnixListener,

    connections: Vec<EthTxPoolIpcStream>,

    pub pool: EthTxPool<ST, SCT, SBT>,

    batch: Vec<Recovered<TxEnvelope>>,
    #[pin]
    batch_timer: Sleep,
}

impl<ST, SCT, SBT> EthTxPoolIpcServer<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn new(
        EthTxPoolIpcConfig {
            bind_path,
            tx_batch_size,
            max_queued_batches,
            queued_batches_watermark,
        }: EthTxPoolIpcConfig,
        pool: EthTxPool<ST, SCT, SBT>,
    ) -> Result<Self, io::Error> {
        assert!(queued_batches_watermark <= max_queued_batches);

        let listener = UnixListener::bind(bind_path)?;

        Ok(Self {
            listener,

            connections: Vec::default(),

            pool,

            batch: Vec::default(),
            batch_timer: time::sleep(Duration::ZERO),
        })
    }

    pub fn broadcast_tx_events(self: Pin<&mut Self>, events: &Vec<EthTxPoolEvent>) {
        self.project().connections.retain(|stream| {
            match stream.send_tx_events(events.to_owned()) {
                Ok(()) => true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    warn!("dropping ipc stream, reason: channel full!");
                    false
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    info!("dropping ipc stream, reason: channel closed!");
                    false
                }
            }
        });
    }
}

impl<ST, SCT, SBT> EthTxPoolIpcServerProjected<'_, ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    pub fn generate_snapshot(&mut self) -> EthTxPoolSnapshot {
        self.pool.generate_snapshot()
    }
}

impl<ST, SCT, SBT> Stream for EthTxPoolIpcServer<ST, SCT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend,
{
    type Item = Vec<Recovered<TxEnvelope>>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let EthTxPoolIpcServerProjected {
            listener,

            connections,

            pool,

            batch,
            mut batch_timer,
        } = self.project();

        while let Poll::Ready(result) = listener.poll_accept(cx) {
            match result {
                Err(error) => {
                    warn!("listener poll accept error={error:?}");
                    continue;
                }
                Ok((stream, _)) => {
                    connections.push(EthTxPoolIpcStream::new(stream, pool.generate_snapshot()));
                }
            }
        }

        if batch.is_empty() {
            batch_timer.set(time::sleep(Duration::from_millis(BATCH_TIMER_INTERVAL_MS)));
        }

        connections.retain_mut(|stream| {
            loop {
                if batch.len() >= MAX_BATCH_LEN {
                    break;
                }

                let Poll::Ready(result) = stream.poll_next_unpin(cx) else {
                    break;
                };

                let Some(tx) = result else {
                    return false;
                };

                batch.push(tx);
            }

            true
        });

        if batch.len() >= MAX_BATCH_LEN || batch_timer.as_mut().poll(cx).is_ready() {
            batch_timer.set(time::sleep(Duration::from_millis(BATCH_TIMER_INTERVAL_MS)));
            return Poll::Ready(Some(std::mem::take(batch)));
        }

        Poll::Pending
    }
}
