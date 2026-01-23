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

use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{
    MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage, StateSyncRequest,
    StateSyncResponse, StateSyncUpsertType, StateSyncUpsertV1, SELF_STATESYNC_VERSION,
};
use monad_state_backend::{InMemoryState, StateBackend};
use monad_types::{ExecutionProtocol, FinalizedHeader, NodeId, SeqNum, GENESIS_SEQ_NUM};
use monad_validator::signature_collection::SignatureCollection;

pub trait MockableStateSync:
    Executor<Command = StateSyncCommand<Self::Signature, Self::ExecutionProtocol>> + Unpin
{
    type Signature: CertificateSignatureRecoverable;
    type SignatureCollection: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::Signature>,
    >;
    type ExecutionProtocol: ExecutionProtocol;

    fn ready(&self) -> bool;
    fn pop(
        &mut self,
    ) -> Option<MonadEvent<Self::Signature, Self::SignatureCollection, Self::ExecutionProtocol>>;
}

impl<T: MockableStateSync + ?Sized> MockableStateSync for Box<T> {
    type Signature = T::Signature;
    type SignatureCollection = T::SignatureCollection;
    type ExecutionProtocol = T::ExecutionProtocol;

    fn ready(&self) -> bool {
        (**self).ready()
    }
    fn pop(
        &mut self,
    ) -> Option<MonadEvent<Self::Signature, Self::SignatureCollection, Self::ExecutionProtocol>>
    {
        (**self).pop()
    }
}

pub struct MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    events: VecDeque<MonadEvent<ST, SCT, EPT>>,

    state_backend: InMemoryState<ST, SCT>,
    peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    max_service_window: SeqNum,

    started_execution: bool,
    request: Option<StateSyncRequest>,

    waker: Option<Waker>,
}

impl<ST, SCT, EPT> Executor for MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = StateSyncCommand<ST, EPT>;

    fn exec(&mut self, cmds: Vec<Self::Command>) {
        for cmd in cmds {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
            match cmd {
                StateSyncCommand::StartExecution => {
                    assert!(!self.started_execution);
                    self.started_execution = true;
                }
                StateSyncCommand::ExpandUpstreamPeers(new_peers) => {
                    let new_peers: Vec<_> = new_peers
                        .into_iter()
                        .filter(|peer| !self.peers.contains(peer))
                        .collect();
                    self.peers.extend(new_peers);
                }
                StateSyncCommand::RequestSync(eth_header)
                    if eth_header.seq_num() == GENESIS_SEQ_NUM =>
                {
                    self.events
                        .push_back(MonadEvent::StateSyncEvent(StateSyncEvent::DoneSync(
                            GENESIS_SEQ_NUM,
                        )));
                }
                StateSyncCommand::RequestSync(eth_header) => {
                    assert!(!self.started_execution);
                    assert!(!self.peers.is_empty());
                    let request = StateSyncRequest {
                        version: SELF_STATESYNC_VERSION,
                        target: eth_header.seq_num().0,
                        from: 0,
                        prefix: 0,
                        prefix_bytes: 1,
                        until: 0,
                        old_target: 0,
                    };
                    self.request = Some(request);
                    self.events.extend(self.peers.iter().map(|peer| {
                        MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                            *peer,
                            StateSyncNetworkMessage::Request(request),
                            None, // don't care about completion for mock
                        ))
                    }))
                }
                StateSyncCommand::Message((from, message)) => match message {
                    StateSyncNetworkMessage::Request(request) => {
                        if !self.started_execution {
                            return;
                        }
                        let state = self.state_backend.lock().unwrap();
                        let latest_finalized = state.raw_read_latest_finalized_block();
                        if latest_finalized.is_some_and(|latest_finalized| {
                            request.target.saturating_add(self.max_service_window.0)
                                < latest_finalized.0
                        }) {
                            return;
                        }

                        let Some(state) = state.committed_state(&SeqNum(request.target)) else {
                            return;
                        };

                        let serialized = serde_json::to_vec(state).unwrap();
                        let response = StateSyncResponse {
                            version: SELF_STATESYNC_VERSION,
                            nonce: 0,
                            response_index: 0,

                            request,
                            response: vec![StateSyncUpsertV1::new(
                                StateSyncUpsertType::Code,
                                serialized.into(),
                            )],
                            response_n: 1,
                        };
                        self.events
                            .push_back(MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                                from,
                                StateSyncNetworkMessage::Response(response),
                                None, // don't care about completion for mock
                            )))
                    }
                    StateSyncNetworkMessage::Response(response) => {
                        if !self.started_execution
                            && self
                                .request
                                .is_some_and(|request| request == response.request)
                        {
                            self.request = None;
                            let deserialized =
                                serde_json::from_slice(&response.response[0].data).unwrap();
                            let mut old_state = self.state_backend.lock().unwrap();
                            old_state.reset_state(deserialized);
                            self.events.push_back(MonadEvent::StateSyncEvent(
                                StateSyncEvent::DoneSync(SeqNum(response.request.target)),
                            ));
                        }
                    }
                    StateSyncNetworkMessage::BadVersion(_) => {}
                    StateSyncNetworkMessage::Completion(_) => {}
                    StateSyncNetworkMessage::NotWhitelisted => {}
                },
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain<'_> {
        Default::default()
    }
}

impl<ST, SCT, EPT> MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(state_backend: InMemoryState<ST, SCT>) -> Self {
        Self {
            events: Default::default(),
            state_backend,
            // no peers on initialization
            // these get populated via StateSyncCommand::ExpandUpstreamPeers
            peers: Default::default(),
            max_service_window: SeqNum::MAX,
            started_execution: false,
            request: None,
            waker: None,
        }
    }
    pub fn with_max_service_window(mut self, max_service_window: SeqNum) -> Self {
        self.max_service_window = max_service_window;
        self
    }
}

impl<ST, SCT, EPT> MockableStateSync for MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EPT;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn pop(&mut self) -> Option<MonadEvent<ST, SCT, EPT>> {
        self.events.pop_front()
    }
}

impl<ST, SCT, EPT> Stream for MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
