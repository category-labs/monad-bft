use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use monad_consensus_types::block::{FullTransactionList, TransactionList};
use monad_mempool_controller::{Controller, ControllerConfig};
use monad_mempool_messenger::MessengerError;
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{Executor, MempoolCommand};

#[derive(Error, Debug)]
enum ControllerTaskError {
    #[error("ChannelSendError({0})")]
    ChannelSendError(#[from] mpsc::error::SendError<ControllerTaskResult>),

    #[error(transparent)]
    MessengerError(#[from] MessengerError),
}

enum ControllerTaskCommand {
    FetchTxs,
    FetchFullTxs(TransactionList),
}

enum ControllerTaskResult {
    FetchTxs(TransactionList),
    FetchFullTxs(Option<FullTransactionList>),
}

pub struct MonadMempool<E> {
    controller_task: JoinHandle<Result<(), ControllerTaskError>>,
    controller_task_tx: mpsc::Sender<ControllerTaskCommand>,
    controller_task_rx: mpsc::Receiver<ControllerTaskResult>,

    fetch_txs_state: Option<Box<dyn (FnOnce(TransactionList) -> E) + Send + Sync>>,
    fetch_full_txs_state: Option<Box<dyn (FnOnce(Option<FullTransactionList>) -> E) + Send + Sync>>,
}

impl<E> Default for MonadMempool<E> {
    fn default() -> Self {
        let (controller_task_tx, rx) = mpsc::channel(1);
        let (tx, controller_task_rx) = mpsc::channel(1);

        let controller_task = tokio::spawn(async move {
            let mut controller = Controller::new(&ControllerConfig::default());

            controller.start().await?;

            let mut rx = rx;
            let tx = tx;

            while let Some(task) = rx.recv().await {
                match task {
                    ControllerTaskCommand::FetchTxs => {
                        let proposal = controller.create_proposal().await;

                        tx.send(ControllerTaskResult::FetchTxs(TransactionList(proposal)))
                            .await?;
                    }
                    ControllerTaskCommand::FetchFullTxs(txs) => {
                        let full_txs = controller.fetch_full_txs(txs.0).await;

                        tx.send(ControllerTaskResult::FetchFullTxs(
                            full_txs.map(FullTransactionList),
                        ))
                        .await?;
                    }
                }
            }

            Ok(())
        });

        Self {
            controller_task,
            controller_task_tx,
            controller_task_rx,

            fetch_txs_state: None,
            fetch_full_txs_state: None,
        }
    }
}

impl<E> Executor for MonadMempool<E> {
    type Command = MempoolCommand<E>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut task_command = None;

        for command in commands {
            match command {
                MempoolCommand::FetchTxs(cb) => {
                    self.fetch_txs_state = Some(cb);
                    task_command = Some(ControllerTaskCommand::FetchTxs);
                }
                MempoolCommand::FetchReset => {
                    self.fetch_txs_state = None;
                    task_command = None;
                }
                MempoolCommand::FetchFullTxs(txs, cb) => {
                    self.fetch_full_txs_state = Some(cb);
                    task_command = Some(ControllerTaskCommand::FetchFullTxs(txs));
                }
                MempoolCommand::FetchFullReset => {
                    self.fetch_full_txs_state = None;
                    task_command = None;
                }
            }
        }

        if let Some(command) = task_command {
            self.controller_task_tx.try_send(command).unwrap();
        }
    }
}

impl<E> Stream for MonadMempool<E>
where
    Self: Unpin,
{
    type Item = E;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        if let Poll::Ready(item) = this.controller_task_rx.poll_recv(cx) {
            let result = if let Some(result) = item {
                result
            } else {
                return Poll::Ready(None);
            };

            match result {
                ControllerTaskResult::FetchTxs(txs) => {
                    if let Some(cb) = this.fetch_txs_state.take() {
                        return Poll::Ready(Some(cb(txs)));
                    }
                }
                ControllerTaskResult::FetchFullTxs(full_txs) => {
                    if let Some(cb) = this.fetch_full_txs_state.take() {
                        return Poll::Ready(Some(cb(full_txs)));
                    }
                }
            }
        }

        Poll::Pending
    }
}

impl<E> Drop for MonadMempool<E> {
    fn drop(&mut self) {
        self.controller_task.abort()
    }
}
