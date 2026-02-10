use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

use alloy_primitives::B256;
use monad_eth_txpool_types::{
    EthTxPoolEventType, EthTxPoolIpcTx, EthTxPoolSnapshot, EthTxPoolTxInputStream,
};
use pin_project::pin_project;
use tokio::sync::mpsc;

#[pin_project]
pub struct ChannelInputStream {
    rx: mpsc::UnboundedReceiver<EthTxPoolIpcTx>,
}

impl ChannelInputStream {
    pub fn new() -> (mpsc::UnboundedSender<EthTxPoolIpcTx>, Self) {
        let (tx, rx) = mpsc::unbounded_channel();
        (tx, Self { rx })
    }
}

impl EthTxPoolTxInputStream for ChannelInputStream {
    fn poll_txs(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _generate_snapshot: impl Fn() -> EthTxPoolSnapshot,
    ) -> Poll<Vec<EthTxPoolIpcTx>> {
        let this = self.project();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(tx)) => {
                let mut batch = vec![tx];
                while let Ok(tx) = this.rx.try_recv() {
                    batch.push(tx);
                }
                Poll::Ready(batch)
            }
            Poll::Ready(None) => Poll::Pending,
            Poll::Pending => Poll::Pending,
        }
    }

    fn broadcast_tx_events(self: Pin<&mut Self>, _events: BTreeMap<B256, EthTxPoolEventType>) {}
}
