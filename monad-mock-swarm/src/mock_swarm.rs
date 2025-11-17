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

use std::{collections::BTreeMap, time::Duration};

use bytes::Bytes;
use monad_crypto::certificate_signature::CertificateSignaturePubKey;
use monad_executor_glue::MonadEvent;
use monad_transformer::{LinkMessage, Pipeline, ID};
use rayon::prelude::*;

use crate::{
    mock::MockExecutor,
    node::{Node, NodeBuilder},
    swarm_relation::{DebugSwarmRelation, SwarmRelation},
    terminator::NodesTerminator,
};

pub struct Nodes<S>
where
    S: SwarmRelation,
{
    pub(crate) states: BTreeMap<ID<CertificateSignaturePubKey<S::SignatureType>>, Node<S>>,
    pub(crate) tick: Duration,
    must_deliver: bool,
    no_duplicate_peers: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SwarmEventType {
    ExecutorEvent,
    ScheduledMessage,
}

impl<S> Nodes<S>
where
    S: SwarmRelation,
    MockExecutor<S>: Unpin,
{
    pub fn can_fail_deliver(mut self) -> Self {
        self.must_deliver = false;
        self
    }

    pub fn can_have_duplicate_peer(mut self) -> Self {
        self.no_duplicate_peers = false;
        self
    }

    fn peek_event(
        &self,
    ) -> Option<(
        Duration,
        SwarmEventType,
        ID<CertificateSignaturePubKey<S::SignatureType>>,
    )> {
        self.states
            .iter()
            .filter_map(|(id, node)| {
                node.peek_event()
                    .map(|(tick, event_type)| (tick, event_type, *id))
            })
            .min()
    }

    pub fn peek_tick(&self) -> Option<Duration> {
        self.peek_event().map(|(tick, _, _)| tick)
    }

    // step until exactly the next event, either internal or external event out of all the nodes.
    pub fn step_until(
        &mut self,
        terminator: &mut impl NodesTerminator<S>,
    ) -> Option<(
        Duration,
        ID<CertificateSignaturePubKey<S::SignatureType>>,
        MonadEvent<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
    )> {
        while let Some((tick, _event_type, id)) = self.peek_event() {
            if terminator.should_terminate(self, tick) {
                break;
            }

            let node = self
                .states
                .get_mut(&id)
                .expect("logic error, should be nonempty");

            let mut emitted_messages = Vec::new();
            let emitted_event = node.step_until(tick, &mut emitted_messages);
            self.tick = tick;

            for (sched_tick, message) in emitted_messages {
                assert_ne!(message.from, message.to);
                let node = self.states.get_mut(&message.to);
                // if message must be delivered, then node must exists
                assert!(!self.must_deliver || node.is_some());
                if let Some(node) = node {
                    node.push_inbound_message(sched_tick, message);
                };
            }
            if let Some((tick, event)) = emitted_event {
                return Some((tick, id, event));
            }
        }
        None
    }

    pub fn batch_step_until(
        &mut self,
        terminator: &mut impl NodesTerminator<S>,
    ) -> Option<Duration> {
        while let Some(tick) = {
            self.peek_event().map(|(min_tick, _, id)| {
                let min_unsafe_tick = min_tick
                    + self
                        .states
                        .get(&id)
                        .expect("must exist")
                        .outbound_pipeline
                        .min_external_delay();
                // max safe tick is (min_unsafe_tick - EPSILON)
                min_unsafe_tick - Duration::from_nanos(1)
            })
        } {
            if terminator.should_terminate(self, tick) {
                return Some(self.tick);
            }

            let mut emitted_messages: Vec<(
                Duration,
                LinkMessage<CertificateSignaturePubKey<S::SignatureType>, S::TransportMessage>,
            )> = Vec::new();

            emitted_messages.par_extend(self.states.par_iter_mut().flat_map_iter(|(_id, node)| {
                let mut emitted = Vec::new();
                while let Some((_tick, _event)) = node.step_until(tick, &mut emitted) {}
                emitted.into_iter()
            }));
            self.tick = tick;

            for (sched_tick, message) in emitted_messages {
                let node = self.states.get_mut(&message.to);
                // if message must be delivered, then node must exists
                assert!(!self.must_deliver || node.is_some());

                if let Some(node) = node {
                    node.push_inbound_message(sched_tick, message);
                };
            }
        }
        None
    }

    pub fn states(&self) -> &BTreeMap<ID<CertificateSignaturePubKey<S::SignatureType>>, Node<S>> {
        &self.states
    }

    pub fn mut_states(
        &mut self,
    ) -> &mut BTreeMap<ID<CertificateSignaturePubKey<S::SignatureType>>, Node<S>> {
        &mut self.states
    }

    pub fn remove_state(
        &mut self,
        peer_id: &ID<CertificateSignaturePubKey<S::SignatureType>>,
    ) -> Option<Node<S>> {
        self.states.remove(peer_id)
    }

    pub fn add_state(&mut self, peer: NodeBuilder<S>) {
        let node = peer.build(self.tick);

        // No duplicate ID insertion should be allowed
        assert!(!self.states.contains_key(&node.id));
        // if nodes only want to run with unique ids
        assert!(!self.no_duplicate_peers || node.id.is_unique());

        self.states.insert(node.id, node);
    }

    pub fn send_transaction(
        &mut self,
        node_id: ID<CertificateSignaturePubKey<S::SignatureType>>,
        tx: Bytes,
    ) {
        self.states
            .get_mut(&node_id)
            .expect("node should exist")
            .executor
            .send_transaction(tx);
    }

    pub fn update_outbound_pipeline_for_all(&mut self, new_pipeline: S::Pipeline)
    where
        S::Pipeline: Clone,
    {
        for node in &mut self.states.values_mut() {
            node.outbound_pipeline = new_pipeline.clone();
        }
    }
    pub fn update_outbound_pipeline(
        &mut self,
        id: &ID<CertificateSignaturePubKey<S::SignatureType>>,
        new_pipeline: S::Pipeline,
    ) {
        self.states
            .get_mut(id)
            .map(|node| node.outbound_pipeline = new_pipeline);
    }
}

pub struct SwarmBuilder<S: SwarmRelation>(pub Vec<NodeBuilder<S>>);
impl<S> SwarmBuilder<S>
where
    S: SwarmRelation,
    MockExecutor<S>: Unpin,
    Node<S>: Send,
{
    pub fn debug(self) -> SwarmBuilder<DebugSwarmRelation>
    where
        S: SwarmRelation<
            SignatureType = <DebugSwarmRelation as SwarmRelation>::SignatureType,
            SignatureCollectionType = <DebugSwarmRelation as SwarmRelation>::SignatureCollectionType,
            ExecutionProtocolType = <DebugSwarmRelation as SwarmRelation>::ExecutionProtocolType,
            TransportMessage = <DebugSwarmRelation as SwarmRelation>::TransportMessage,
            BlockPolicyType = <DebugSwarmRelation as SwarmRelation>::BlockPolicyType,
            BlockValidator = <DebugSwarmRelation as SwarmRelation>::BlockValidator,
            StateBackendType = <DebugSwarmRelation as SwarmRelation>::StateBackendType,
            ChainConfigType = <DebugSwarmRelation as SwarmRelation>::ChainConfigType,
            ChainRevisionType = <DebugSwarmRelation as SwarmRelation>::ChainRevisionType,
        > + 'static,
    // FIXME can this be deleted?
        S::RouterScheduler: Sync,
        S::Ledger: Sync,
    {
        SwarmBuilder(self.0.into_iter().map(NodeBuilder::debug).collect())
    }
    pub fn build(self) -> Nodes<S> {
        let mut nodes = Nodes {
            states: BTreeMap::new(),
            tick: Duration::ZERO,
            must_deliver: true,
            no_duplicate_peers: true,
        };

        for peer in self.0 {
            nodes.add_state(peer);
        }

        nodes
    }
}
