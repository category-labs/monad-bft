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

use bytes::Bytes;
use futures::Stream;
use itertools::{Either, Itertools};
use monad_chain_config::{revision::ChainRevision, ChainConfig};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, TxPoolCommand};
use monad_secp::ExtractEthAddress;
use monad_state_backend::StateBackend;
use monad_types::NodeId;
use monad_validator::signature_collection::SignatureCollection;
use tracing::warn;

pub struct ForwardedTxs<SCT>
where
    SCT: SignatureCollection,
{
    pub sender: NodeId<SCT::NodeIdPubKey>,
    pub txs: Vec<Bytes>,
}

const DEFAULT_COMMAND_BUFFER_SIZE: usize = 1024;
const DEFAULT_FORWARDED_BUFFER_SIZE: usize = 1024;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1024;

pub struct EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    handle: tokio::task::JoinHandle<()>,
    metrics: ExecutorMetrics,
    update_metrics: Box<dyn Fn(&mut ExecutorMetrics)>,

    command_tx: tokio::sync::mpsc::Sender<
        Vec<
            TxPoolCommand<
                ST,
                SCT,
                EthExecutionProtocol,
                EthBlockPolicy<ST, SCT, CCT, CRT>,
                SBT,
                CCT,
                CRT,
            >,
        >,
    >,
    forwarded_tx: tokio::sync::mpsc::Sender<Vec<ForwardedTxs<SCT>>>,
    event_rx: tokio::sync::mpsc::Receiver<MonadEvent<ST, SCT, EthExecutionProtocol>>,
}

impl<ST, SCT, SBT, CCT, CRT> EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    pub fn new<F>(
        updater: impl FnOnce(
                tokio::sync::mpsc::Receiver<
                    Vec<
                        TxPoolCommand<
                            ST,
                            SCT,
                            EthExecutionProtocol,
                            EthBlockPolicy<ST, SCT, CCT, CRT>,
                            SBT,
                            CCT,
                            CRT,
                        >,
                    >,
                >,
                tokio::sync::mpsc::Receiver<Vec<ForwardedTxs<SCT>>>,
                tokio::sync::mpsc::Sender<MonadEvent<ST, SCT, EthExecutionProtocol>>,
            ) -> F
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
            DEFAULT_FORWARDED_BUFFER_SIZE,
            DEFAULT_EVENT_BUFFER_SIZE,
        )
    }

    pub fn new_with_buffer_sizes<F>(
        updater: impl FnOnce(
                tokio::sync::mpsc::Receiver<
                    Vec<
                        TxPoolCommand<
                            ST,
                            SCT,
                            EthExecutionProtocol,
                            EthBlockPolicy<ST, SCT, CCT, CRT>,
                            SBT,
                            CCT,
                            CRT,
                        >,
                    >,
                >,
                tokio::sync::mpsc::Receiver<Vec<ForwardedTxs<SCT>>>,
                tokio::sync::mpsc::Sender<MonadEvent<ST, SCT, EthExecutionProtocol>>,
            ) -> F
            + Send
            + 'static,
        update_metrics: Box<dyn Fn(&mut ExecutorMetrics) + Send + 'static>,
        command_buffer_size: usize,
        forwarded_buffer_size: usize,
        event_buffer_size: usize,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (command_tx, command_rx) = tokio::sync::mpsc::channel(command_buffer_size);
        let (forwarded_tx, forwarded_rx) = tokio::sync::mpsc::channel(forwarded_buffer_size);
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(event_buffer_size);

        let handle = tokio::spawn(updater(command_rx, forwarded_rx, event_tx));

        Self {
            handle,
            metrics: ExecutorMetrics::default(),
            update_metrics,

            command_tx,
            forwarded_tx,
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

        if self.forwarded_tx.is_closed() {
            panic!("EthTxPoolExecutorClient forwarded_rx dropped!");
        }

        if self.event_rx.is_closed() {
            panic!("EthTxPoolExecutorClient event_tx dropped!");
        }
    }
}

impl<ST, SCT, SBT, CCT, CRT> Executor for EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Command = TxPoolCommand<
        ST,
        SCT,
        EthExecutionProtocol,
        EthBlockPolicy<ST, SCT, CCT, CRT>,
        SBT,
        CCT,
        CRT,
    >;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        self.verify_handle_liveness();

        let (commands, forwarded): (Vec<Self::Command>, Vec<ForwardedTxs<SCT>>) =
            commands.into_iter().partition_map(|command| match command {
                TxPoolCommand::InsertForwardedTxs { sender, txs } => {
                    Either::Right(ForwardedTxs { sender, txs })
                }
                command => Either::Left(command),
            });

        if !commands.is_empty() {
            self.command_tx
                .try_send(commands)
                .expect("EthTxPoolExecutorClient executor is lagging")
        }

        if !forwarded.is_empty() {
            if let Err(err) = self.forwarded_tx.try_send(forwarded) {
                warn!(
                    ?err,
                    "txpool executor client forwarded channel full, dropping forwarded txs"
                );
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::from(&self.metrics)
    }
}

impl<ST, SCT, SBT, CCT, CRT> Stream for EthTxPoolExecutorClient<ST, SCT, SBT, CCT, CRT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    SBT: StateBackend<ST, SCT>,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

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
