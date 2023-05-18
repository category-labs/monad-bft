use std::{
    collections::BTreeSet,
    ops::DerefMut,
    pin::Pin,
    sync::{Arc, RwLock},
    task::{Context, Poll},
};

use crate::{Executor, MempoolCommand};

use futures::Stream;

type Tx = Vec<u8>;

pub trait Mempool: Send + Sync + 'static {
    fn add_tx(&mut self, tx: Tx) -> bool;
    fn fetch_txs(&self, max: usize) -> Vec<Tx>;

    fn remove_tx(&mut self, tx: &Tx);
}

// Default SimpleMempool is a noop mempool
#[derive(Default)]
pub struct SimpleMempool {
    max_size: usize,

    txs: BTreeSet<Tx>,
}

impl SimpleMempool {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            txs: Default::default(),
        }
    }
}

impl Mempool for SimpleMempool {
    fn add_tx(&mut self, tx: Tx) -> bool {
        if self.txs.len() >= self.max_size {
            return false;
        }
        self.txs.insert(tx);
        true
    }

    fn fetch_txs(&self, max: usize) -> Vec<Tx> {
        self.txs.iter().take(max).cloned().collect()
    }

    fn remove_tx(&mut self, tx: &Tx) {
        self.txs.remove(tx);
    }
}

impl<M: Mempool> Mempool for Arc<RwLock<M>> {
    fn add_tx(&mut self, tx: Tx) -> bool {
        self.write().unwrap().add_tx(tx)
    }

    fn fetch_txs(&self, max: usize) -> Vec<Tx> {
        self.write().unwrap().fetch_txs(max)
    }

    fn remove_tx(&mut self, tx: &Tx) {
        self.write().unwrap().remove_tx(tx)
    }
}

pub struct MempoolExecutor<MP, E> {
    pub mempool: MP,

    pub fetch_txs_state: Option<Box<dyn FnOnce(Vec<u8>) -> E>>,

    pub fetch_num_tx: usize,
}

impl<MP, E> MempoolExecutor<MP, E> {
    pub fn ready(&self) -> bool {
        self.fetch_txs_state.is_some()
    }
}

impl<MP, E> Default for MempoolExecutor<MP, E>
where
    MP: Default,
{
    fn default() -> Self {
        Self {
            mempool: Default::default(),
            fetch_txs_state: Default::default(),

            // FIXME should this passed as a param to FetchTxs instead?
            fetch_num_tx: 5_000,
        }
    }
}
impl<MP, E> Executor for MempoolExecutor<MP, E> {
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

impl<MP, E> Stream for MempoolExecutor<MP, E>
where
    MP: Mempool,
    Self: Unpin,
{
    type Item = E;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        Poll::Ready(this.fetch_txs_state.take().map(|cb| {
            let txs = self.mempool.fetch_txs(self.fetch_num_tx);

            // FIXME encode this properly
            let txs = txs.into_iter().flatten().collect();

            cb(txs)
        }))
    }
}
