use std::{
    marker::Unpin,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;

use crate::{Executor, LedgerCommand};

pub struct MockLedger<O, E> {
    blockchain: Vec<O>,
    ledger_fetch_cb: Option<Box<dyn (FnOnce(Option<O>) -> E) + Send + Sync>>,
    waker: Option<Waker>,
}

impl<O, E> MockLedger<O, E>
where
    O: Clone,
{
    pub fn ready(&self) -> bool {
        self.ledger_fetch_cb.is_some()
    }
}

impl<O, E> Default for MockLedger<O, E> {
    fn default() -> Self {
        Self {
            blockchain: Vec::new(),
            ledger_fetch_cb: None,
            waker: None,
        }
    }
}

impl<O, E> MockLedger<O, E> {
    pub fn get_blocks(&self) -> &Vec<O> {
        &self.blockchain
    }
}

impl<O, E> Executor for MockLedger<O, E> {
    type Command = LedgerCommand<O, E>;
    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut wake = false;

        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(block) => {
                    self.blockchain.push(block);
                }
                LedgerCommand::LedgerFetch(block_id, cb) => {
                    self.ledger_fetch_cb = Some(cb);
                    wake = true;
                }
            }
        }
        if wake {
            if let Some(waker) = self.waker.take() {
                waker.wake()
            };
        }
    }
}

impl<O, E> Stream for MockLedger<O, E>
where
    Self: Unpin,
    O: Clone,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Some(cb) = this.ledger_fetch_cb.take() {
            return Poll::Ready(match self.blockchain.len() {
                0 => Some(cb(None)),
                // TODO, introduce better data structure for retrieving based on BlockId
                // right now just want to present the rough flow.
                n => Some(cb(Some(self.blockchain[n - 1].clone()))),
            });
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
