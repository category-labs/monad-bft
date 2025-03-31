use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;
use tokio::task::JoinHandle;

pub struct EthTxPoolBridgeHandle {
    handle: JoinHandle<()>,
}

impl EthTxPoolBridgeHandle {
    pub fn new(handle: JoinHandle<()>) -> Self {
        Self { handle }
    }
}

impl Future for EthTxPoolBridgeHandle {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Poll::Ready(result) = self.handle.poll_unpin(cx) else {
            return Poll::Pending;
        };

        Poll::Ready(result.unwrap())
    }
}
