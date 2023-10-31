use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use monad_consensus_types::{
    command::{FetchFullTxParams, FetchTxParams},
    message_signature::MessageSignature,
    payload::{FullTransactionList, TransactionHashList},
    signature_collection::SignatureCollection,
};
use monad_eth_types::EthTransactionList;
use monad_executor::Executor;
use monad_executor_glue::{MempoolCommand, MonadEvent};
use monad_mempool_controller::{Controller, ControllerConfig};
use monad_mempool_messenger::MessengerError;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinHandle},
};
use tracing::*;

const DEFAULT_MEMPOOL_CONTROLLER_CHANNEL_SIZE: usize = 64;

#[derive(Error, Debug)]
enum ControllerTaskError {
    #[error(transparent)]
    ChannelSend(#[from] mpsc::error::SendError<ControllerTaskResult>),

    #[error(transparent)]
    Messenger(#[from] MessengerError),

    #[error(transparent)]
    Join(#[from] JoinError),
}

#[derive(Debug)]
enum ControllerTaskCommand {
    FetchTxs(usize, Vec<TransactionHashList>),
    FetchFullTxs(TransactionHashList),
    DrainTxs(Vec<TransactionHashList>),
}

enum ControllerTaskResult {
    FetchTxs(TransactionHashList),
    FetchFullTxs(Option<FullTransactionList>),
}

pub struct MonadMempool<ST, SCT> {
    controller_task: JoinHandle<Result<(), ControllerTaskError>>,
    controller_task_tx: mpsc::Sender<ControllerTaskCommand>,
    controller_task_rx: mpsc::Receiver<ControllerTaskResult>,

    fetch_txs_state: Option<FetchTxParams<SCT>>,
    fetch_full_txs_state: Option<FetchFullTxParams<SCT>>,

    phantom: PhantomData<ST>,
}

impl<ST, SCT> Default for MonadMempool<ST, SCT>
where
    ST: 'static,
    SCT: 'static,
{
    fn default() -> Self {
        Self::new(ControllerConfig::default())
    }
}

impl<ST, SCT> MonadMempool<ST, SCT>
where
    ST: 'static,
    SCT: 'static,
{
    pub fn new(controller_config: ControllerConfig) -> Self {
        let (controller_task_tx, rx) = mpsc::channel(DEFAULT_MEMPOOL_CONTROLLER_CHANNEL_SIZE);
        let (tx, controller_task_rx) = mpsc::channel(DEFAULT_MEMPOOL_CONTROLLER_CHANNEL_SIZE);

        let controller_task = tokio::spawn(Self::controller_task(tx, rx, controller_config));

        Self {
            controller_task,
            controller_task_tx,
            controller_task_rx,

            fetch_txs_state: None,
            fetch_full_txs_state: None,
            phantom: PhantomData,
        }
    }

    async fn controller_task(
        tx: mpsc::Sender<ControllerTaskResult>,
        mut rx: mpsc::Receiver<ControllerTaskCommand>,
        config: ControllerConfig,
    ) -> Result<(), ControllerTaskError> {
        let mut controller = Controller::new(config).await?;

        loop {
            tokio::select! {
                task = rx.recv() => {
                    let Some(task) = task else {
                        return Ok(())
                    };

                    match task {
                        ControllerTaskCommand::FetchTxs(num_max_txs, pending_txs) => {
                            let Ok(pending_txs) = pending_txs.into_iter().map(|txs| EthTransactionList::rlp_decode(txs.as_bytes().to_vec())).collect::<Result<Vec<_>, _>>() else {
                                error!("Invalid pending_txs!");
                                continue;
                            };

                            let proposal = controller.create_proposal(num_max_txs, pending_txs).await;

                            tx.send(ControllerTaskResult::FetchTxs(TransactionHashList::new(proposal.rlp_encode())))
                                .await?;
                        }
                        ControllerTaskCommand::FetchFullTxs(txs) => {
                            let txs = match EthTransactionList::rlp_decode(txs.as_bytes().to_vec()) {
                                Ok(txs) => txs,
                                Err(_) => {
                                    // TODO: warn
                                    continue;
                                }
                            };

                            let full_txs = controller.fetch_full_txs(txs).await;

                            tx.send(ControllerTaskResult::FetchFullTxs(
                                full_txs.map(|full_txs| FullTransactionList::new(full_txs.rlp_encode())),
                            ))
                            .await?;
                        }
                        ControllerTaskCommand::DrainTxs(drain_txs) => {
                            let Ok(drain_txs) = drain_txs.into_iter().map(|drain_txs| EthTransactionList::rlp_decode(drain_txs.as_bytes().to_vec())).collect::<Result<Vec<EthTransactionList>, _>>() else {
                                error!("Invalid drain_txs!");
                                continue;
                            };

                            for txs in drain_txs {
                                controller.drain_txs(txs).await;
                            }
                        }
                    }
                }

                result = controller.next() => {
                    return if let Some(e) = result {
                        Err(e.into())
                    } else {
                        Ok(())
                    };
                }
            }
        }
    }
}

impl<ST, SCT> Executor for MonadMempool<ST, SCT> {
    type Command = MempoolCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let mut fetch_txs_command = None;
        let mut fetch_full_txs_command = None;
        let mut drain_txs_command = None;

        for command in commands {
            match command {
                MempoolCommand::FetchTxs(num_max_txs, pending_txs, s) => {
                    self.fetch_txs_state = Some(s);
                    fetch_txs_command =
                        Some(ControllerTaskCommand::FetchTxs(num_max_txs, pending_txs));
                }
                MempoolCommand::FetchReset => {
                    self.fetch_txs_state = None;
                    fetch_txs_command = None;
                }
                MempoolCommand::FetchFullTxs(txs, s) => {
                    self.fetch_full_txs_state = Some(s);
                    fetch_full_txs_command = Some(ControllerTaskCommand::FetchFullTxs(txs));
                }
                MempoolCommand::FetchFullReset => {
                    self.fetch_full_txs_state = None;
                    fetch_full_txs_command = None;
                }
                MempoolCommand::DrainTxs(drain_txs) => {
                    drain_txs_command = Some(ControllerTaskCommand::DrainTxs(drain_txs));
                }
            }
        }

        for cmd in fetch_txs_command
            .into_iter()
            .chain(fetch_full_txs_command)
            .chain(drain_txs_command)
        {
            self.controller_task_tx.try_send(cmd).unwrap();
        }
    }
}

impl<ST, SCT> Stream for MonadMempool<ST, SCT>
where
    Self: Unpin,
    ST: MessageSignature,
    SCT: SignatureCollection,
{
    type Item = MonadEvent<ST, SCT>;
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
                    if let Some(s) = this.fetch_txs_state.take() {
                        return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                            monad_executor_glue::ConsensusEvent::FetchedTxs(s, txs),
                        )));
                    }
                }
                ControllerTaskResult::FetchFullTxs(full_txs) => {
                    if let Some(s) = this.fetch_full_txs_state.take() {
                        return Poll::Ready(Some(MonadEvent::ConsensusEvent(
                            monad_executor_glue::ConsensusEvent::FetchedFullTxs(s, full_txs),
                        )));
                    }
                }
            }
        }

        Poll::Pending
    }
}

impl<ST, SCT> Drop for MonadMempool<ST, SCT> {
    fn drop(&mut self) {
        self.controller_task.abort()
    }
}
