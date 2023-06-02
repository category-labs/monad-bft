use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::{Executor, MempoolCommand};

use futures::Stream;

pub struct MockMempool<E> {
    fetch_txs_state: Option<Box<dyn (FnOnce(Vec<u8>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<E> MockMempool<E> {
    pub fn ready(&self) -> bool {
        self.fetch_txs_state.is_some()
    }
}

impl<E> Default for MockMempool<E> {
    fn default() -> Self {
        Self {
            fetch_txs_state: None,
            waker: None,
        }
    }
}
impl<E> Executor for MockMempool<E> {
    type Command = MempoolCommand<E>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;
        for command in commands {
            self.fetch_txs_state = match command {
                MempoolCommand::FetchTxs(cb) => {
                    wake = true;
                    Some(cb)
                }
                MempoolCommand::FetchReset => {
                    wake = false;
                    None
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}
impl<E> Stream for MockMempool<E>
where
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(cb) = this.fetch_txs_state.take() {
            return Poll::Ready(Some(cb(Vec::new())));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[test]
    fn test_fetch() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_double_fetch() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_reset() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchReset]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_double_fetch() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(Box::new(|_| {})),
            MempoolCommand::FetchTxs(Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(Box::new(|_| {})),
            MempoolCommand::FetchReset,
        ]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset_fetch() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![
            MempoolCommand::FetchReset,
            MempoolCommand::FetchTxs(Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_noop_exec() {
        let mut mempool = MockMempool::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        mempool.exec(Vec::new());
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }
}
