use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use crate::{Executor, MempoolCommand};

use futures::Stream;
use monad_consensus_types::transaction::TransactionCollection;

pub struct MockMempool<E, TC>
where
    TC: TransactionCollection,
{
    fetch_txs_state: Option<Box<dyn (FnOnce(Vec<u8>) -> E) + Send + Sync>>,
    fetch_full_txs_state: Option<Box<dyn (FnOnce(TC) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<E, TC> MockMempool<E, TC>
where
    TC: TransactionCollection,
{
    pub fn ready(&self) -> bool {
        self.fetch_txs_state.is_some() || self.fetch_full_txs_state.is_some()
    }
}

impl<E, TC> Default for MockMempool<E, TC>
where
    TC: TransactionCollection,
{
    fn default() -> Self {
        Self {
            fetch_txs_state: None,
            fetch_full_txs_state: None,
            waker: None,
        }
    }
}
impl<E, TC> Executor for MockMempool<E, TC>
where
    TC: TransactionCollection,
{
    type Command = MempoolCommand<E, TC>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                MempoolCommand::FetchTxs(cb) => {
                    self.fetch_txs_state = Some(cb);
                    wake = true;
                }
                MempoolCommand::FetchReset => {
                    self.fetch_txs_state = None;
                    wake = self.fetch_full_txs_state.is_some();
                }
                MempoolCommand::FetchFullTxs(cb) => {
                    self.fetch_full_txs_state = Some(cb);
                    wake = true;
                }
                MempoolCommand::FetchFullReset => {
                    self.fetch_full_txs_state = None;
                    wake = self.fetch_txs_state.is_some();
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
impl<E, TC> Stream for MockMempool<E, TC>
where
    Self: Unpin,
    TC: TransactionCollection,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(cb) = this.fetch_txs_state.take() {
            return Poll::Ready(Some(cb(Vec::new())));
        }

        if let Some(cb) = this.fetch_full_txs_state.take() {
            return Poll::Ready(Some(cb(TC::default())));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use monad_consensus_types::transaction::MockTransactions;

    use super::*;

    #[test]
    fn test_fetch() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_double_fetch() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_reset() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        mempool.exec(vec![MempoolCommand::FetchReset]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_double_fetch() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(Box::new(|_| {})),
            MempoolCommand::FetchTxs(Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![
            MempoolCommand::FetchTxs(Box::new(|_| {})),
            MempoolCommand::FetchReset,
        ]);
        assert!(!mempool.ready());
    }

    #[test]
    fn test_inline_reset_fetch() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![
            MempoolCommand::FetchReset,
            MempoolCommand::FetchTxs(Box::new(|_| {})),
        ]);
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }

    #[test]
    fn test_noop_exec() {
        let mut mempool = MockMempool::<(), MockTransactions>::default();
        mempool.exec(vec![MempoolCommand::FetchTxs(Box::new(|_| {}))]);
        mempool.exec(Vec::new());
        assert!(futures::executor::block_on(mempool.next()).is_some());
        assert!(!mempool.ready());
    }
}
