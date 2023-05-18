use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{Executor, MempoolCommand};

use futures::Stream;

pub struct MockMempool<E> {
    fetch_txs_state: Option<Box<dyn FnOnce(Vec<u8>) -> E>>,
}
impl<E> Default for MockMempool<E> {
    fn default() -> Self {
        Self {
            fetch_txs_state: None,
        }
    }
}
impl<E> Executor for MockMempool<E> {
    type Command = MempoolCommand<E>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                MempoolCommand::FetchTxs(cb) => self.fetch_txs_state = Some(cb),
                MempoolCommand::FetchReset => self.fetch_txs_state = None,
            }
        }
    }
}
impl<E> Stream for MockMempool<E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        Poll::Ready(this.fetch_txs_state.take().map(
            // run cb with empty tx list
            |cb| cb(Vec::new()),
        ))
    }
}
