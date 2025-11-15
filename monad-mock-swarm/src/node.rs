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
    collections::{BTreeMap, VecDeque},
    marker::PhantomData,
    time::Duration,
};

use itertools::Itertools;
use monad_consensus_types::validator_data::{ValidatorData, ValidatorSetData};
use monad_crypto::certificate_signature::{
    CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
};
use monad_executor::Executor;
use monad_executor_glue::MonadEvent;
use monad_state::{Forkpoint, MonadStateBuilder};
use monad_transformer::{LinkMessage, Pipeline, ID};
use monad_validator::{
    signature_collection::SignatureCollection,
    validator_mapping::ValidatorMapping,
    validator_set::{BoxedValidatorSetTypeFactory, ValidatorSetType, ValidatorSetTypeFactory},
};
use rand::{Rng, SeedableRng};
use rand_chacha::{ChaCha20Rng, ChaChaRng};

use crate::{
    mock::{MockExecutor, MockExecutorEvent, TimestamperConfig},
    mock_swarm::SwarmEventType,
    swarm_relation::{DebugSwarmRelation, SwarmRelation, SwarmRelationStateType},
};

pub struct NodeBuilder<S: SwarmRelation> {
    pub id: ID<CertificateSignaturePubKey<S::SignatureType>>,
    pub state_builder: MonadStateBuilder<
        S::SignatureType,
        S::SignatureCollectionType,
        S::ExecutionProtocolType,
        S::BlockPolicyType,
        S::StateBackendType,
        S::ValidatorSetTypeFactory,
        S::LeaderElection,
        S::BlockValidator,
        S::ChainConfigType,
        S::ChainRevisionType,
    >,
    pub router_scheduler: S::RouterScheduler,
    pub val_set_updater: S::ValSetUpdater,
    pub txpool_executor: S::TxPoolExecutor,
    pub ledger: S::Ledger,
    pub statesync_executor: S::StateSyncExecutor,
    pub outbound_pipeline: S::Pipeline,
    pub inbound_pipeline: S::Pipeline,
    pub timestamper_config: TimestamperConfig,
    pub seed: u64,
}
impl<S: SwarmRelation> NodeBuilder<S> {
    pub fn new(
        id: ID<CertificateSignaturePubKey<S::SignatureType>>,
        state_builder: MonadStateBuilder<
            S::SignatureType,
            S::SignatureCollectionType,
            S::ExecutionProtocolType,
            S::BlockPolicyType,
            S::StateBackendType,
            S::ValidatorSetTypeFactory,
            S::LeaderElection,
            S::BlockValidator,
            S::ChainConfigType,
            S::ChainRevisionType,
        >,
        router_scheduler: S::RouterScheduler,
        val_set_updater: S::ValSetUpdater,
        txpool_executor: S::TxPoolExecutor,
        ledger: S::Ledger,
        statesync_executor: S::StateSyncExecutor,
        outbound_pipeline: S::Pipeline,
        inbound_pipeline: S::Pipeline,
        timestamper_config: TimestamperConfig,
        seed: u64,
    ) -> Self {
        Self {
            id,
            state_builder,
            router_scheduler,
            val_set_updater,
            txpool_executor,
            ledger,
            statesync_executor,
            outbound_pipeline,
            inbound_pipeline,
            timestamper_config,
            seed,
        }
    }

    pub fn debug(self) -> NodeBuilder<DebugSwarmRelation>
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
        >,
    // FIXME can this be deleted?
        S::RouterScheduler: Sync,
        S::Ledger: Sync,
    {
        NodeBuilder {
            id: self.id,
            state_builder: MonadStateBuilder {
                validator_set_factory: BoxedValidatorSetTypeFactory::new(
                    self.state_builder.validator_set_factory,
                ),
                leader_election: Box::new(self.state_builder.leader_election),
                block_validator: self.state_builder.block_validator,
                block_policy: self.state_builder.block_policy,
                state_backend: self.state_builder.state_backend,
                key: self.state_builder.key,
                certkey: self.state_builder.certkey,
                beneficiary: self.state_builder.beneficiary,
                forkpoint: self.state_builder.forkpoint,
                locked_epoch_validators: self.state_builder.locked_epoch_validators,
                block_sync_override_peers: self.state_builder.block_sync_override_peers,
                consensus_config: self.state_builder.consensus_config,
                whitelisted_statesync_nodes: self.state_builder.whitelisted_statesync_nodes,
                statesync_expand_to_group: self.state_builder.statesync_expand_to_group,

                _phantom: PhantomData,
            },
            router_scheduler: Box::new(self.router_scheduler),
            val_set_updater: Box::new(self.val_set_updater),
            txpool_executor: Box::new(self.txpool_executor),
            ledger: Box::new(self.ledger),
            statesync_executor: Box::new(self.statesync_executor),
            outbound_pipeline: Box::new(self.outbound_pipeline),
            inbound_pipeline: Box::new(self.inbound_pipeline),
            timestamper_config: self.timestamper_config,
            seed: self.seed,
        }
    }
    pub fn build(self, tick: Duration) -> Node<S> {
        let mut executor: MockExecutor<S> = MockExecutor::new(
            self.router_scheduler,
            self.val_set_updater,
            self.txpool_executor,
            self.ledger,
            self.statesync_executor,
            self.timestamper_config,
            tick,
        );
        let (state, init_commands) = self.state_builder.build();

        executor.exec(init_commands);

        let mut rng = ChaChaRng::seed_from_u64(self.seed);

        Node {
            id: self.id,
            executor,
            state,
            outbound_pipeline: self.outbound_pipeline,
            inbound_pipeline: self.inbound_pipeline,
            pending_inbound_messages: Default::default(),
            rng: ChaCha20Rng::seed_from_u64(rng.gen()),
            current_seed: rng.gen(),
            message_nonce: 0,
        }
    }
}

pub struct Node<S>
where
    S: SwarmRelation,
{
    pub id: ID<CertificateSignaturePubKey<S::SignatureType>>,
    pub executor: MockExecutor<S>,
    pub state: SwarmRelationStateType<S>,
    pub outbound_pipeline: S::Pipeline,
    pub inbound_pipeline: S::Pipeline,
    pub pending_inbound_messages: BTreeMap<
        Duration,
        VecDeque<LinkMessage<CertificateSignaturePubKey<S::SignatureType>, S::TransportMessage>>,
    >,
    rng: ChaCha20Rng,
    current_seed: usize,

    message_nonce: usize,
}

impl<S: SwarmRelation> Node<S> {
    fn update_rng(&mut self) {
        self.current_seed = self.rng.gen();
    }

    pub fn peek_event(&self) -> Option<(Duration, SwarmEventType)> {
        // avoid modification of the original rng
        let events = std::iter::empty()
            .chain(
                self.executor
                    .peek_tick()
                    .iter()
                    .map(|tick| (*tick, SwarmEventType::ExecutorEvent)),
            )
            .chain(self.pending_inbound_messages.first_key_value().map(
                |(min_scheduled_tick, _)| (*min_scheduled_tick, SwarmEventType::ScheduledMessage),
            ))
            .min_set();
        if !events.is_empty() {
            Some(events[self.current_seed % events.len()])
        } else {
            None
        }
    }

    pub fn push_inbound_message(
        &mut self,
        sched_tick: Duration,
        message: LinkMessage<CertificateSignaturePubKey<S::SignatureType>, S::TransportMessage>,
    ) {
        let inbound_transformed = self.inbound_pipeline.process(message);
        for (inbound_delay, msg) in inbound_transformed {
            // final tick = from_tick + outbound pipeline delay + inbound pipeline delay
            let inbound_tick = sched_tick + inbound_delay;

            self.pending_inbound_messages
                .entry(inbound_tick)
                .or_default()
                .push_back(msg);
        }
    }

    pub fn step_until(
        &mut self,
        until: Duration,
        emitted_messages: &mut Vec<(
            Duration,
            LinkMessage<CertificateSignaturePubKey<S::SignatureType>, S::TransportMessage>,
        )>,
    ) -> Option<(
        Duration,
        MonadEvent<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType>,
    )> {
        while let Some((tick, event_type)) = self.peek_event() {
            let _mock_swarm_span = tracing::trace_span!("mock_swarm_span", ?tick).entered();
            if tick > until {
                break;
            }
            // polling event, thus update the rng
            self.update_rng();
            let event = match event_type {
                SwarmEventType::ExecutorEvent => {
                    let executor_event = self.executor.step_until(tick);
                    match executor_event {
                        None => continue,
                        Some(MockExecutorEvent::Event(event)) => {
                            let node_span =
                                tracing::trace_span!("node", id = format!("{}", self.id));
                            let _guard = node_span.enter();
                            let event_clone = event.lossy_clone();
                            let commands = self.state.update(event);

                            self.executor.exec(commands);

                            (tick, event_clone)
                        }
                        Some(MockExecutorEvent::Send(to, serialized)) => {
                            let lm = LinkMessage {
                                from: self.id,
                                to: ID::new(to),
                                message: serialized,

                                from_tick: tick,
                                nonce: self.message_nonce,
                            };
                            self.message_nonce += 1;
                            let outbound_transformed = self.outbound_pipeline.process(lm);
                            for (delay, msg) in outbound_transformed {
                                let sched_tick = tick + delay;

                                // FIXME-3: do we need to transform msg to self?
                                if msg.to == self.id {
                                    self.push_inbound_message(sched_tick, msg);
                                } else {
                                    emitted_messages.push((sched_tick, msg))
                                }
                            }
                            continue;
                        }
                    }
                }
                SwarmEventType::ScheduledMessage => {
                    let mut entry = self
                        .pending_inbound_messages
                        .first_entry()
                        .expect("logic error, should be nonempty");

                    let scheduled_tick = *entry.key();
                    let msgs = entry.get_mut();

                    assert_eq!(tick, scheduled_tick);

                    let message = msgs.pop_front().expect("logic error, should be nonempty");

                    if msgs.is_empty() {
                        entry.remove_entry();
                    }

                    self.executor.send_message(
                        scheduled_tick,
                        *message.from.get_peer_id(),
                        message.message,
                    );

                    continue;
                }
            };
            return Some(event);
        }
        None
    }

    fn build_validator_set_data(
        validator_set: &<S::ValidatorSetTypeFactory as ValidatorSetTypeFactory>::ValidatorSetType,
        validator_mapping: &ValidatorMapping<<<S::SignatureType as CertificateSignature>::KeyPairType as CertificateKeyPair>::PubKeyType, <<S::SignatureCollectionType as SignatureCollection>::SignatureType as CertificateSignature>::KeyPairType>,
    ) -> ValidatorSetData<S::SignatureCollectionType> {
        let mut validator_set_data = Vec::new();
        for (node_id, stake) in validator_set.get_members() {
            let cert_pubkey = validator_mapping
                .map
                .get(node_id)
                .expect("validator set and mapping are paired");
            validator_set_data.push(ValidatorData {
                node_id: *node_id,
                stake: *stake,
                cert_pubkey: *cert_pubkey,
            });
        }
        ValidatorSetData(validator_set_data)
    }

    pub fn get_forkpoint(
        &self,
    ) -> Forkpoint<S::SignatureType, S::SignatureCollectionType, S::ExecutionProtocolType> {
        self.executor
            .checkpoint()
            .expect("no forkpoint generated")
            .into()
    }
}
