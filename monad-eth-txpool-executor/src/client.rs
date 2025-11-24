// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{future::Future, pin::Pin};

use futures::Stream;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;

pub struct EthTxPoolExecutorClient<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    handle: tokio::task::JoinHandle<()>,
    metrics: ExecutorMetrics,
    update_metrics: Box<dyn Fn(&mut ExecutorMetrics)>,

    command_tx: tokio::sync::mpsc::Sender<Vec<C>>,
    event_rx: tokio::sync::mpsc::Receiver<E>,
}

impl<C, E> EthTxPoolExecutorClient<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    pub fn new<F>(
        updater: impl FnOnce(tokio::sync::mpsc::Receiver<Vec<C>>, tokio::sync::mpsc::Sender<E>) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::new_with_buffer_sizes(
            updater,
            update_metrics,
            DEFAULT_COMMAND_BUFFER_SIZE,
            DEFAULT_EVENT_BUFFER_SIZE,
        )
    }

    pub fn new_with_buffer_sizes<F>(
        updater: impl FnOnce(tokio::sync::mpsc::Receiver<Vec<C>>, tokio::sync::mpsc::Sender<E>) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        command_buffer_size: usize,
        event_buffer_size: usize,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(command_buffer_size);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(event_buffer_size);

        let handle = tokio::spawn(updater(command_rx, event_tx));

        Self {
            handle,
            metrics: ExecutorMetrics::default(),
            update_metrics,

            command_tx,
            event_rx,
        }
    }

    fn verify_handle_liveness(&self) {
        if self.handle.is_finished() {
            panic!("EthTxPoolExecutorClient handle terminated!");
        }

        if self.command_tx.is_closed() {
            panic!("EthTxPoolExecutorClient command_rx dropped!");
        }

        if self.event_rx.is_closed() {
            panic!("EthTxPoolExecutorClient event_tx dropped!");
        }
    }
}

impl<C, E> Executor for EthTxPoolExecutorClient<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    type Command = C;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.verify_handle_liveness();

        self.command_tx
            .try_send(commands)
            .expect("EthTxPoolExecutorClient executor is lagging")
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::from(&self.metrics)
    }
}

impl<C, E> Stream for EthTxPoolExecutorClient<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    type Item = E;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.verify_handle_liveness();

        (this.update_metrics)(&mut this.metrics);

        this.event_rx.poll_recv(cx)
    }
}
