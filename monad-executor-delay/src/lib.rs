use std::{ops::DerefMut, time::Duration};

use driver::{DelayDriver, DelayDriverItem};
use futures::{Stream, StreamExt};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use tokio::sync::mpsc::{error::TrySendError, UnboundedSender};

mod driver;

/// An executor wrapper class that adds a fixed delay to all stream events.
/// Commands are executed immediately
pub struct ExecutorDelay<E>
where
    E: Executor + Stream,
{
    command_tx: UnboundedSender<E::Command>,
    rx: tokio::sync::mpsc::Receiver<DelayDriverItem<E::Item>>,
    metrics: ExecutorMetrics,
}

impl<E> ExecutorDelay<E>
where
    E: Executor + Stream + Unpin + Send + 'static,
    E::Command: Send + 'static,
    E::Item: Send + 'static,
    DelayDriver<E>: Unpin,
{
    pub fn new(executor: E, delay: Duration) -> Self {
        const EVENT_BUFFER_SIZE: usize = 1000;
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(EVENT_BUFFER_SIZE);
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::task::spawn({
            async move {
                let mut delay_driver = DelayDriver::new(executor, delay, command_rx);
                while let Some(event) = delay_driver.next().await {
                    let result = event_tx.try_send(event);
                    match result {
                        Ok(()) => {}
                        Err(TrySendError::Full(_)) => todo!(
                            "consensus event loop not consuming fast enough, buf_size={}",
                            EVENT_BUFFER_SIZE
                        ),
                        Err(TrySendError::Closed(_)) => break,
                    }
                }
            }
        });
        Self {
            command_tx,
            rx: event_rx,
            metrics: Default::default(),
        }
    }
}

impl<E> Executor for ExecutorDelay<E>
where
    E: Executor + Stream,
{
    type Command = E::Command;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            self.command_tx.send(command).unwrap();
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<E> Stream for ExecutorDelay<E>
where
    E: Executor + Stream,

    Self: Unpin,
{
    type Item = E::Item;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            let poll_result = self.deref_mut().rx.poll_recv(cx);

            match poll_result {
                std::task::Poll::Ready(Some(item)) => match item {
                    DelayDriverItem::MetricsUpdate(metrics) => {
                        self.metrics = metrics.into_iter().into();
                        continue;
                    }
                    DelayDriverItem::Item(event) => return std::task::Poll::Ready(Some(event)),
                },
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}
